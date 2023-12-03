from __future__ import annotations

import itertools
from typing import Sequence, AsyncIterable, Any, cast

from mysql_mimic import ColumnType
from mysql_mimic.results import AllowedColumn, ResultSet, ResultColumn, infer_type
from mysql_mimic.utils import aiterate
from sqlglot.optimizer import Scope
from sqlglot import alias, exp


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


def _qualify_outputs(scope: Scope) -> None:
    """Ensure all output columns are aliased"""
    new_selections = []

    for i, (selection, aliased_column) in enumerate(
            itertools.zip_longest(scope.expression.selects, scope.outer_column_list)
    ):
        if isinstance(selection, exp.Subquery):
            if not selection.output_name:
                selection.set("alias", exp.TableAlias(this=exp.to_identifier(f"_col_{i}")))
        elif not isinstance(selection, exp.Alias) and not selection.is_star:
            selection = alias(
                selection,
                alias=selection.output_name or selection.sql(),
            )
        if aliased_column:
            selection.set("alias", exp.to_identifier(aliased_column))

        new_selections.append(selection)

    scope.expression.set("expressions", new_selections)
