from __future__ import annotations

import inspect
from collections import defaultdict
from io import UnsupportedOperation
from typing import Dict, List, Callable, Any, Awaitable, Optional

from mysql_mimic import Session as _Session, AllowedResult, ResultSet
from mysql_mimic.errors import MysqlError, ErrorCode
from mysql_mimic.schema import BaseInfoSchema, Column
from mysql_mimic.session import Query, expression_to_value, value_to_expression, setitem_kind
from sqlglot import expressions as exp
from sqlglot.executor import execute

from .util import reloading


class Session(_Session):
    SCHEMA_PROVIDERS: List[Callable[[], Dict[str, Dict[str, List[Column]]]]] = []
    DATA_PROVIDERS: Dict[
        str, Callable[[str, Optional[List[Dict]]], Awaitable[List[Dict[str, Any]]] | List[Dict[str, Any]]]] = {}
    DATA_CREATORS: Dict[str, Callable[[Session, str, list, list], Awaitable | None]] = {}
    DATA_MODIFIERS: Dict[str, Callable[[Session, str, list, dict], Awaitable[int] | int]] = {}
    DATA_REMOVERS: Dict[str, Callable[[Session, str, list], Awaitable | None]] = {}

    TABLES: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(defaultdict)
    SCHEMA = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_variables = {}
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
        if not self.SCHEMA:
            await self.schema()
        if expression.key == 'select':
            tables = []
            self.extract_tables(tables, expression)
            for table in tables:
                db = self.database if table.db == '' and self.database is not None else table.db
                supplier = self.DATA_PROVIDERS.get(db)
                if supplier is None:
                    raise MysqlError(f"Unknown database '{db}'", code=ErrorCode.NO_DB_ERROR)
                rows, columns = [], []
                if table.name == 'ohlcv':
                    query = f"select symbol from subscriptions {expression.args.get('where', '')}"
                    query_expression = self._parse(query)[0]
                    rows, columns = await self.query(query_expression, query, attrs)

                rows = supplier(table.name, [{k: v for k, v in zip(columns, row)} for row in
                                             rows] if 'where' in expression.args else None)
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
                db = self.database if table.db == '' and self.database is not None else table.db
                fields = [col for col in self.SCHEMA[db][table.name]]
            else:
                schema = expression.this
                table = schema.this
                fields = [i.name for i in schema.expressions]
            values = []
            if expression.expression.key == 'values':
                for row in expression.expression.expressions:
                    value = []
                    for v in row:
                        if isinstance(v, exp.Literal):
                            value.append(v.this if v.is_string else int(v.this) if v.is_int else float(v.this))
                        else:
                            query = f"select {v} limit 1"
                            result = execute(query)
                            for rt_row in result.rows:
                                value.append(rt_row[0])
                    values.append(value)

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
            query = f"select {', '.join(projects)} from {table.name} {expression.args.get('where', '')}"
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

    async def _set_variable(self, setitem: exp.SetItem) -> None:
        try:
            super()._set_variable(setitem)
        except MysqlError as e:
            if e.code != ErrorCode.NOT_SUPPORTED_YET:
                raise e
            assignment = setitem.this
            left = assignment.left
            name = left.name
            right = assignment.right
            value = expression_to_value(right)
            if value == right.name and not isinstance(right, exp.Literal):
                if right.key in ['subquery']:
                    query = right.this.sql()
                elif right.key in ['select']:
                    query = right.sql()
                else:
                    query = f"select {right} as value"
                rows, col = await self.handle_query(query, {})
                for row in rows:
                    self.user_variables[name] = row[0]
                    break
            else:
                self.user_variables[name] = value

    async def _replace_variables_middleware(self, q: Query) -> AllowedResult:
        def _transform(node: exp.Expression) -> exp.Expression:
            new_node = None

            if isinstance(node, exp.Func):
                if isinstance(node, exp.Anonymous):
                    func_name = node.name.upper()
                else:
                    func_name = node.sql_name()
                func = self._functions.get(func_name)
                if func:
                    value = func()
                    new_node = value_to_expression(value)
            elif isinstance(node, exp.Column) and node.sql() in self._constants:
                value = self._functions[node.sql()]()
                new_node = value_to_expression(value)
            elif isinstance(node, exp.SessionParameter):
                value = self.variables.get(node.name)
                new_node = value_to_expression(value)
            elif isinstance(node, exp.Parameter):
                value = self.user_variables.get(node.name)
                new_node = value_to_expression(value)

            if (
                    new_node
                    and isinstance(node.parent, exp.Select)
                    and node.arg_key == "expressions"
            ):
                new_node = exp.alias_(new_node, exp.to_identifier(node.sql()))

            return new_node or node

        if isinstance(q.expression, exp.Set):
            for setitem in q.expression.expressions:
                if isinstance(setitem.this, exp.Binary):
                    # In the case of statements like: SET @@foo = @@bar
                    # We only want to replace variables on the right
                    setitem.this.set(
                        "expression",
                        setitem.this.expression.transform(_transform, copy=True),
                    )
        else:
            q.expression.transform(_transform, copy=False)

        return await q.next()

    async def _set_middleware(self, q: Query) -> AllowedResult:
        """Intercept SET statements"""
        if isinstance(q.expression, exp.Set):
            expressions = q.expression.expressions
            for item in expressions:
                assert isinstance(item, exp.SetItem)

                kind = setitem_kind(item)

                if kind == "VARIABLE":
                    await self._set_variable(item)
                elif kind == "CHARACTER SET":
                    self._set_charset(item)
                elif kind == "NAMES":
                    self._set_names(item)
                elif kind == "TRANSACTION":
                    self._set_transaction(item)
                else:
                    raise MysqlError(
                        f"Unsupported SET statement: {kind}",
                        code=ErrorCode.NOT_SUPPORTED_YET,
                    )

            return [], []
        return await q.next()
