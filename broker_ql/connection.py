import mysql_mimic.results as _results
from mysql_mimic import packets
from mysql_mimic.connection import Connection as _Connection
from sqlglot.executor.env import ENV as _ENV, null_if_any
from sqlglot.expressions import Func, AggFunc
from sqlglot.helper import subclasses
from sqlglot.dialects.mysql import MySQL
import numpy as np

from .results import _ensure_result_cols, _qualify_outputs

_results._ensure_result_cols = _ensure_result_cols

import sqlglot.optimizer.qualify_columns as qualify_columns

qualify_columns._qualify_outputs = _qualify_outputs

from sqlglot.executor import optimize as _optimize

import sqlglot.executor as _executor
import sqlglot.optimizer as _optimizer


def optimize(*args, **kwargs):
    if 'rules' not in kwargs:
        kwargs['rules'] = _optimizer.RULES[:-1]
    return _optimize(*args, **kwargs)


_executor.optimize = optimize


class Connection(_Connection):

    async def handle_query(self, data: bytes) -> None:
        com_query = packets.parse_com_query(
            capabilities=self.capabilities,
            client_charset=self.client_charset,
            data=data,
        )

        result_set = await self.query(com_query.sql, com_query.query_attrs)

        if not result_set:
            affected_rows = 0
            if hasattr(result_set, 'affected_rows'):
                affected_rows = result_set.affected_rows
            await self.stream.write(self.ok(affected_rows=affected_rows))
            return

        async for packet in self.text_resultset(result_set):
            await self.stream.write(packet)


class OffsetAggFunc(AggFunc):
    @classmethod
    def offset_value(cls, column, offset=None):
        values = [v for v in column]
        if offset is not None:
            offset = max(offset)
            if offset > 0:
                values = values[:-offset]
        return values


class TA_Lowest(OffsetAggFunc):
    _sql_names = ["TA_LOWEST"]
    arg_types = {"this": False, "window": False, "offset": False}

    @classmethod
    def apply(cls, column, window, offset=None):
        values = cls.offset_value(column, offset)
        window = max(window)
        if len(values) >= window:
            values = values[-window:]
        return min([v for v in values if v is not None and not np.isnan(v)])


_ENV["TA_LOWEST"] = TA_Lowest.apply


class TA_Highest(OffsetAggFunc):
    _sql_names = ["TA_HIGHEST"]
    arg_types = {"this": False, "window": False, "offset": False}

    @classmethod
    def apply(cls, column, window, offset=None):
        values = cls.offset_value(column, offset)
        window = max(window)
        if len(values) >= window:
            values = values[-window:]
        return max([v for v in values if v is not None and not np.isnan(v)])


_ENV["TA_HIGHEST"] = TA_Highest.apply

_ENV.update({
    "ROUND": null_if_any(lambda this, e: round(this, e)),
})

for f in subclasses(__name__, Func, (Func, AggFunc, OffsetAggFunc)):
    MySQL.Parser.FUNCTIONS.update({
        f.__name__.upper(): f.from_arg_list
    })
