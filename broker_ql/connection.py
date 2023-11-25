import mysql_mimic.results as _results
from mysql_mimic import packets
from mysql_mimic.connection import Connection as _Connection
from sqlglot.executor.env import ENV as _ENV, null_if_any

from .results import _ensure_result_cols

_results._ensure_result_cols = _ensure_result_cols


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


_ENV.update({
    "ROUND": null_if_any(lambda this, e: round(this, e)),
})
