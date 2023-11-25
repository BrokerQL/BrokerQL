from __future__ import annotations

from typing import Sequence, AsyncIterable, Any, cast

from mysql_mimic import ColumnType
from mysql_mimic.results import AllowedColumn, ResultSet, ResultColumn, infer_type
from mysql_mimic.utils import aiterate


async def _ensure_result_cols(
        rows: Sequence[Sequence[Any]] | AsyncIterable[Sequence[Any]],
        columns: Sequence[AllowedColumn],
) -> ResultSet:
    # Which columns need to be inferred?
    remaining = {
        i: col for i, col in enumerate(columns) if not isinstance(col, ResultColumn)
    }

    if not remaining:
        return ResultSet(
            rows=rows,
            columns=cast(Sequence[ResultColumn], columns),
        )

    # Copy the columns
    columns = list(columns)

    arows = aiterate(rows)

    # Keep track of rows we've consumed from the iterator so we can add them back
    peeks = []

    # Find the first non-null value for each column
    while remaining:
        try:
            peek = await arows.__anext__()
        except StopAsyncIteration:
            break

        peeks.append(peek)

        inferred = []
        for i, name in remaining.items():
            value = peek[i]
            if value is not None:
                type_ = infer_type(value)
                columns[i] = ResultColumn(
                    name=str(name),
                    type=type_,
                )
                inferred.append(i)

        for i in inferred:
            remaining.pop(i)

    # If we failed to find a non-null value, set the type to NULL
    for i, name in remaining.items():
        columns[i] = ResultColumn(
            name=str(name),
            type=ColumnType.NULL,
        )

    # Add the consumed rows back in to the iterator
    async def gen_rows() -> AsyncIterable[Sequence[Any]]:
        for row in peeks:
            yield row

        async for row in arows:
            yield row

    assert all(isinstance(col, ResultColumn) for col in columns)
    return ResultSet(rows=gen_rows(), columns=cast(Sequence[ResultColumn], columns))
