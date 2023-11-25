from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from io import UnsupportedOperation
from typing import Dict, List, Callable, Any, Awaitable

from mysql_mimic import Session as _Session, AllowedResult, ResultSet
from mysql_mimic.errors import MysqlError, ErrorCode
from mysql_mimic.schema import BaseInfoSchema, Column
from mysql_mimic.session import Query
from sqlglot import expressions as exp
from sqlglot.executor import execute

from .util import reloading


def is_coroutine_function(func: Any):
    return asyncio.iscoroutinefunction(func) or (hasattr(func, "func") and asyncio.iscoroutinefunction(func.func))


class Session(_Session):
    SCHEMA_PROVIDERS: List[Callable[[], Dict[str, Dict[str, List[Column]]]]] = []
    DATA_PROVIDERS: Dict[str, Callable[[str], Awaitable[List[Dict[str, Any]]] | List[Dict[str, Any]]]] = {}
    DATA_CREATORS: Dict[str, Callable[[Session, str, list, list], Awaitable | None]] = {}
    DATA_MODIFIERS: Dict[str, Callable[[Session, str, list, dict], Awaitable[int] | int]] = {}
    DATA_REMOVERS: Dict[str, Callable[[Session, str, list], Awaitable | None]] = {}

    TABLES: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(defaultdict)
    SCHEMA = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_trx = False
        self.trx_commits = []
        self.trx_rollbacks = []

    @reloading
    def extract_tables(self, tables, expression):
        if 'from' in expression.args:
            if expression.args['from'].this.key == 'subquery':
                self.extract_tables(tables, expression.args['from'].this.this)
            else:
                tables.append(expression.args['from'].this)
        for join in expression.args.get('joins', []):
            tables.append(join.this)

    @reloading
    async def query(self, expression, sql: str, attrs) -> AllowedResult:
        if expression.key == 'select':
            tables = []
            self.extract_tables(tables, expression)
            for table in tables:
                db = self.database if table.db == '' and self.database is not None else table.db
                supplier = self.DATA_PROVIDERS.get(db)
                if supplier is None:
                    raise MysqlError(f"Unknown database '{db}'", code=ErrorCode.NO_DB_ERROR)
                rows = supplier(table.name)
                if inspect.isawaitable(rows):
                    rows = await rows
                if rows is None:
                    raise MysqlError(f"Table '{db}.{table.name}' doesn't exist", code=ErrorCode.NO_DB_ERROR)
                self.TABLES[db][table.name] = rows
            result = execute(expression, schema=self.SCHEMA, tables=self.TABLES)
            return result.rows, result.columns
        elif expression.key == 'insert':
            if expression.this.key == 'table':
                table = expression.this
            else:
                schema = expression.this
                table = schema.this
                fields = [i.name for i in schema.expressions]
            values = []
            if expression.expression.key == 'values':
                for row in expression.expression.expressions:
                    values.append([v.this if v.is_string else int(v.this) if v.is_int else float(v.this) for v in row])
            db = self.database if table.db == '' and self.database is not None else table.db
            creator = self.DATA_CREATORS.get(db)
            try:
                if creator is None:
                    raise UnsupportedOperation()
                result = creator(self, table.name, fields, values)
                if inspect.isawaitable(result):
                    result = await result
            except UnsupportedOperation:
                raise MysqlError(f"Unsupported {expression.key} on {db}.{table.name}", code=ErrorCode.NOT_SUPPORTED_YET)

            rs = ResultSet(rows=[], columns=[])
            setattr(rs, 'affected_rows', result)
            return rs
        elif expression.key == 'update':
            table = expression.this
            db = self.database if table.db == '' and self.database is not None else table.db
            alias = {col: col for col in self.SCHEMA[db][table.name]}
            fields = {expr.left.sql(): expr.right.sql() for expr in expression.expressions}
            alias.update(fields)
            projects = [f"{v} as {k}" for k, v in alias.items()]
            query = f"select {', '.join(projects)} from {table.name} {expression.args['where']}"
            query_expression = self._parse(query)[0]
            rows, columns = await self.query(query_expression, query, attrs)
            if not rows:
                rs = ResultSet(rows=[], columns=[])
                setattr(rs, 'affected_rows', 0)
                return rs
            modifier = self.DATA_MODIFIERS.get(db)
            try:
                if modifier is None:
                    raise UnsupportedOperation()
                rows_to_modify = [{k: v for k, v in zip(columns, row)} for row in rows]
                result = modifier(self, table.name, rows_to_modify, fields)
                if inspect.isawaitable(result):
                    await result
            except UnsupportedOperation:
                raise MysqlError(f"Unsupported {expression.key} on {db}.{table.name}", code=ErrorCode.NOT_SUPPORTED_YET)
            rs = ResultSet(rows=[], columns=[])
            setattr(rs, 'affected_rows', len(rows))
            return rs
        elif expression.key == 'delete':
            table = expression.this
            query = f"select * from {table.name} {expression.args['where']}"
            query_expression = self._parse(query)[0]
            rows, columns = await self.query(query_expression, query, attrs)
            if not rows:
                rs = ResultSet(rows=[], columns=[])
                setattr(rs, 'affected_rows', 0)
                return rs
            db = self.database if table.db == '' and self.database is not None else table.db
            remover = self.DATA_REMOVERS.get(db)
            try:
                if remover is None:
                    raise UnsupportedOperation()
                rows_to_remove = [{k: v for k, v in zip(columns, row)} for row in rows]
                result = remover(self, table.name, rows_to_remove)
                if inspect.isawaitable(result):
                    await result
            except UnsupportedOperation:
                raise MysqlError(f"Unsupported {expression.key} on {db}.{table.name}", code=ErrorCode.NOT_SUPPORTED_YET)
            rs = ResultSet(rows=[], columns=[])
            setattr(rs, 'affected_rows', len(rows))
            return rs
        return [], []

    @reloading
    async def _rollback_middleware(self, q: Query) -> AllowedResult:
        if isinstance(q.expression, exp.Rollback):
            self.in_trx = False
            for rollback in self.trx_rollbacks[::-1]:
                rollback()
            self.trx_commits.clear()
            self.trx_rollbacks.clear()
            return [], []
        return await q.next()

    @reloading
    async def _commit_middleware(self, q: Query) -> AllowedResult:
        if isinstance(q.expression, exp.Commit):
            self.in_trx = False
            for commit in self.trx_commits:
                commit()
            self.trx_commits.clear()
            self.trx_rollbacks.clear()
            return [], []
        return await q.next()

    @reloading
    async def _begin_middleware(self, q: Query) -> AllowedResult:
        if isinstance(q.expression, exp.Transaction):
            self.in_trx = True
            self.trx_commits.clear()
            self.trx_rollbacks.clear()
            return [], []
        return await q.next()

    async def schema(self) -> dict | BaseInfoSchema:
        for provider in self.SCHEMA_PROVIDERS:
            self.SCHEMA.update(provider())
        return self.SCHEMA
