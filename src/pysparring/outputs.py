"""Whether two DataFrames hold the same rows, and when they do not, which columns differ and how."""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Iterator
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

from rich.console import Console
from rich.console import ConsoleOptions
from rich.table import Table
from rich.text import Text

from pysparring.rendering import cell
from pysparring.rendering import count
from pysparring.rendering import counted
from pysparring.rendering import plain_text
from pysparring.rendering import show

if TYPE_CHECKING:
    from pyspark.sql import Column
    from pyspark.sql import DataFrame

UNKEYED_ROUND_DIGITS = 6


@dc.dataclass(frozen=True)
class ComparedColumn:
    """A column both sides hold. `cast` is the type both sides become when their types differ.

    `kind` is how values compare: `tolerance` for floats and doubles, `json` for structs, arrays and
    maps, which compare as their JSON text, and `exact` for everything else.
    """

    name: str
    type_a: str
    type_b: str
    cast: str | None
    kind: str
    numeric: bool


@dc.dataclass(frozen=True)
class ColumnDifference:
    """One column across the rows matched by key. A value inside the tolerance is never a mismatch."""

    column: str
    mismatches: int
    null_only_in_a: int
    null_only_in_b: int
    max_abs_diff: float | None
    within_tolerance: int
    max_abs_diff_within_tolerance: float | None


@dc.dataclass(frozen=True)
class StatisticDifference:
    column: str
    statistic: str
    a: Any
    b: Any


@dc.dataclass(frozen=True)
class DifferingValue:
    key: tuple[Any, ...]
    column: str
    a: Any
    b: Any


@dc.dataclass(frozen=True)
class OutputComparison:
    """Schema, rows and columns of two outputs. Row order, column order and nullability are ignored."""

    names: tuple[str, str]
    keys: tuple[str, ...]
    columns_only_in_a: tuple[tuple[str, str], ...]
    columns_only_in_b: tuple[tuple[str, str], ...]
    type_changes: tuple[ComparedColumn, ...]
    rows_a: int
    rows_b: int
    matched_rows: int | None
    only_in_a_rows: int
    only_in_b_rows: int
    duplicate_keys_a: int
    duplicate_keys_b: int
    column_differences: tuple[ColumnDifference, ...]
    statistic_differences: tuple[StatisticDifference, ...]
    differing_values: tuple[DifferingValue, ...]
    only_in_a_sample: tuple[tuple[Any, ...], ...]
    only_in_b_sample: tuple[tuple[Any, ...], ...]
    sample_columns: tuple[str, ...]

    @property
    def matches(self) -> bool:
        return (
            not (self.columns_only_in_a or self.columns_only_in_b or self.type_changes)
            and self.rows_a == self.rows_b
            and self.only_in_a_rows == 0
            and self.only_in_b_rows == 0
            and not any(difference.mismatches for difference in self.column_differences)
        )

    def assert_matches(self) -> None:
        if not self.matches:
            raise AssertionError('\n' + str(self))

    def __str__(self) -> str:
        return plain_text(self)

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> Iterator[Any]:
        a, b = self.names
        how = f'keyed on {", ".join(self.keys)}' if self.keys else 'compared without keys'
        verdict = Text(
            f'{a} vs {b}: outputs match' if self.matches else f'{a} vs {b}: outputs differ',
            style='bold green' if self.matches else 'bold red',
        )
        yield verdict.append(f', {how}', style='')
        yield from self.schema_section()
        yield from self.rows_section()
        yield from self.columns_section()
        yield from self.samples_section()
        footer = 'Row order, column order and nullability are ignored.'
        if not self.keys:
            footer += f' Floats and doubles are rounded to {UNKEYED_ROUND_DIGITS} places before rows are compared.'
            footer += ' Pass keys= to see which columns differ.'
        yield Text(footer, style='dim')

    def schema_section(self) -> Iterator[Any]:
        a, b = self.names
        yield Text('\nSchema', style='bold')
        if not (self.columns_only_in_a or self.columns_only_in_b or self.type_changes):
            yield Text('  same columns and types')
            return
        for side, columns in ((a, self.columns_only_in_a), (b, self.columns_only_in_b)):
            for name, type_name in columns:
                yield Text(f'  only in {side}    {name}   {type_name}')
        for change in self.type_changes:
            yield Text(f'  type changed   {change.name}   {change.type_a} → {change.type_b}, compared as {change.cast}')

    def rows_section(self) -> Iterator[Any]:
        a, b = self.names
        yield Text('\nRows', style='bold')
        yield Text(f'  {a} {count(self.rows_a)}   {b} {count(self.rows_b)}')
        matched = '' if self.matched_rows is None else f'matched {count(self.matched_rows)}   '
        yield Text(f'  {matched}only in {a} {count(self.only_in_a_rows)}   only in {b} {count(self.only_in_b_rows)}')
        for side, duplicates in ((a, self.duplicate_keys_a), (b, self.duplicate_keys_b)):
            if duplicates:
                yield Text(f'  {side} has {counted(duplicates, "key")} appearing more than once', style='yellow')

    def columns_section(self) -> Iterator[Any]:
        a, b = self.names
        mismatched = [d for d in self.column_differences if d.mismatches]
        if self.keys:
            if mismatched:
                yield Text('\nColumns on matched rows, most mismatches first', style='bold')
                table = Table()
                for heading in ('column', 'differ', f'null only in {a}', f'null only in {b}', 'max abs diff'):
                    table.add_column(heading, justify='left' if heading == 'column' else 'right')
                for d in sorted(mismatched, key=lambda d: -d.mismatches):
                    table.add_row(d.column, count(d.mismatches), count(d.null_only_in_a), count(d.null_only_in_b), number(d.max_abs_diff))
                yield table
            elif self.matched_rows:
                yield Text(f'\nEvery column matches on the {counted(self.matched_rows, "matched row")}.')
            for d in self.column_differences:
                if d.within_tolerance:
                    largest = number(d.max_abs_diff_within_tolerance)
                    rows = counted(d.within_tolerance, 'row')
                    yield Text(f'  {d.column}: {rows} differ by at most {largest}, within tolerance', style='dim')
        elif self.statistic_differences:
            yield Text('\nColumn statistics that differ', style='bold')
            table = Table()
            for heading in ('column', 'statistic', a, b):
                table.add_column(heading)
            for s in self.statistic_differences:
                table.add_row(s.column, s.statistic, cell(s.a), cell(s.b))
            yield table

    def samples_section(self) -> Iterator[Any]:
        a, b = self.names
        if self.differing_values:
            total = sum(d.mismatches for d in self.column_differences)
            yield Text(f'\nDiffering values, {len(self.differing_values)} of {count(total)}', style='bold')
            table = Table()
            for heading in (*self.keys, 'column', a, b):
                table.add_column(heading)
            for value in self.differing_values:
                table.add_row(
                    *[cell(k) for k in value.key], value.column, Text(cell(value.a), style='red'), Text(cell(value.b), style='green')
                )
            yield table
        for side, sample, total in ((a, self.only_in_a_sample, self.only_in_a_rows), (b, self.only_in_b_sample, self.only_in_b_rows)):
            if sample:
                yield Text(f'\nOnly in {side}, {len(sample)} of {count(total)}', style='bold')
                table = Table()
                for heading in self.sample_columns:
                    table.add_column(heading)
                for row in sample:
                    table.add_row(*[cell(v) for v in row])
                yield table


def number(value: float | None) -> str:
    return '' if value is None else f'{value:.3g}'


def quoted(name: str) -> str:
    return '`' + name.replace('`', '``') + '`'


def compared_column(name: str, type_a: Any, type_b: Any) -> ComparedColumn:
    import pyspark.sql.types as st

    a, b = type_a.simpleString(), type_b.simpleString()
    if a == b:
        if isinstance(type_a, (st.StructType, st.ArrayType, st.MapType)):
            return ComparedColumn(name, a, b, None, 'json', False)
        if isinstance(type_a, (st.FloatType, st.DoubleType)):
            return ComparedColumn(name, a, b, None, 'tolerance', True)
        return ComparedColumn(name, a, b, None, 'exact', isinstance(type_a, st.NumericType))
    if isinstance(type_a, st.IntegralType) and isinstance(type_b, st.IntegralType):
        return ComparedColumn(name, a, b, 'bigint', 'exact', True)
    if isinstance(type_a, st.NumericType) and isinstance(type_b, st.NumericType):
        return ComparedColumn(name, a, b, 'double', 'tolerance', True)
    return ComparedColumn(name, a, b, 'string', 'exact', False)


def prepared(column: ComparedColumn) -> Column:
    """The column as it is compared: cast to the shared type, or as JSON text."""
    import pyspark.sql.functions as sf

    value = sf.col(quoted(column.name))
    if column.kind == 'json':
        return sf.to_json(value)
    return value.cast(column.cast) if column.cast else value


def compare_outputs(
    a: DataFrame,
    b: DataFrame,
    keys: str | Sequence[str] | None = None,
    names: Sequence[str] = ('a', 'b'),
    rel_tol: float = 1e-9,
    abs_tol: float = 0.0,
    ignore_columns: Sequence[str] = (),
    sample_rows: int = 5,
) -> OutputComparison:
    """Print whether a and b hold the same rows, and when they do not, which columns differ and how.

    With `keys`, rows pair by key and every column reports its mismatches. A float or double matches
    when `|a - b| <= abs_tol + rel_tol * |b|`. Without keys, rows compare whole, after rounding floats
    and doubles to 6 places. Everything runs on Spark, and only the samples are collected.
    """
    key_names = (keys,) if isinstance(keys, str) else tuple(keys or ())
    labels = tuple(names)
    if len(labels) != 2 or labels[0] == labels[1]:
        raise ValueError(f'names must be two different names, and got {list(labels)}')
    ignored = set(ignore_columns)
    fields_a = {f.name: f.dataType for f in a.schema.fields if f.name not in ignored}
    fields_b = {f.name: f.dataType for f in b.schema.fields if f.name not in ignored}
    for key in key_names:
        missing = [label for label, fields in zip(labels, (fields_a, fields_b), strict=True) if key not in fields]
        if missing:
            raise ValueError(f'key {key!r} is not a column of {" or ".join(missing)}')
    shared = {name: compared_column(name, fields_a[name], fields_b[name]) for name in fields_a if name in fields_b}
    schema: dict[str, Any] = {
        'columns_only_in_a': tuple((n, t.simpleString()) for n, t in fields_a.items() if n not in fields_b),
        'columns_only_in_b': tuple((n, t.simpleString()) for n, t in fields_b.items() if n not in fields_a),
        'type_changes': tuple(c for c in shared.values() if c.cast),
    }
    key_columns = [shared[k] for k in key_names]
    value_columns = [c for c in shared.values() if c.name not in key_names]
    if key_names:
        found = compare_keyed(a, b, key_columns, value_columns, rel_tol, abs_tol, sample_rows)
    else:
        found = compare_unkeyed(a, b, value_columns, rel_tol, abs_tol, sample_rows)
    comparison = OutputComparison(names=(labels[0], labels[1]), keys=key_names, **schema, **found)
    show(comparison)
    return comparison


def compare_keyed(
    a: DataFrame,
    b: DataFrame,
    key_columns: list[ComparedColumn],
    value_columns: list[ComparedColumn],
    rel_tol: float,
    abs_tol: float,
    sample_rows: int,
) -> dict[str, Any]:
    import pyspark.sql.functions as sf
    from pyspark.sql import Window

    def side(df: DataFrame, prefix: str) -> DataFrame:
        return df.select(
            *[prepared(c).alias(f'{prefix}_k{j}') for j, c in enumerate(key_columns)],
            *[prepared(c).alias(f'{prefix}_v{i}') for i, c in enumerate(value_columns)],
            sf.lit(True).alias(f'{prefix}_present'),
        )

    def duplicated_keys(df: DataFrame, prefix: str) -> int:
        return df.groupBy(*[f'{prefix}_k{j}' for j in range(len(key_columns))]).count().where(sf.col('count') > 1).count()

    sides = {'a': side(a, 'a'), 'b': side(b, 'b')}
    duplicates = {prefix: duplicated_keys(df, prefix) for prefix, df in sides.items()}
    condition = [sf.col(f'a_k{j}').eqNullSafe(sf.col(f'b_k{j}')) for j in range(len(key_columns))]
    if any(duplicates.values()):
        # Rows sharing a key pair up in order of their values, which is datacompy's technique.
        for prefix, df in sides.items():
            keys = [f'{prefix}_k{j}' for j in range(len(key_columns))]
            ordering = [f'{prefix}_v{i}' for i in range(len(value_columns))] or keys
            sides[prefix] = df.withColumn(f'{prefix}_row', sf.row_number().over(Window.partitionBy(*keys).orderBy(*ordering)))
        condition.append(sf.col('a_row') == sf.col('b_row'))
    joined = sides['a'].join(sides['b'], on=condition, how='full_outer').cache()
    try:
        in_a, in_b = sf.col('a_present').isNotNull(), sf.col('b_present').isNotNull()
        both = in_a & in_b
        aggregates = [
            sf.count(sf.when(in_a, 1)).alias('rows_a'),
            sf.count(sf.when(in_b, 1)).alias('rows_b'),
            sf.count(sf.when(both, 1)).alias('matched'),
            sf.count(sf.when(in_a & ~in_b, 1)).alias('only_a'),
            sf.count(sf.when(in_b & ~in_a, 1)).alias('only_b'),
        ]
        mismatches = []
        for i, column in enumerate(value_columns):
            av, bv = sf.col(f'a_v{i}'), sf.col(f'b_v{i}')
            equal = av.eqNullSafe(bv)
            gap = sf.abs(av.cast('double') - bv.cast('double'))
            if column.kind == 'tolerance':
                close = av.isNotNull() & bv.isNotNull() & (sf.abs(av - bv) <= sf.lit(abs_tol) + sf.lit(rel_tol) * sf.abs(bv))
                mismatch = both & ~(equal | close)
                tolerated = both & ~equal & close
                aggregates += [
                    sf.count(sf.when(tolerated, 1)).alias(f'tolerated{i}'),
                    sf.max(sf.when(tolerated, gap)).alias(f'tolerated_gap{i}'),
                ]
            else:
                mismatch = both & ~equal
                aggregates += [sf.lit(0).alias(f'tolerated{i}'), sf.lit(None).cast('double').alias(f'tolerated_gap{i}')]
            mismatches.append(mismatch)
            aggregates += [
                sf.count(sf.when(mismatch, 1)).alias(f'mismatch{i}'),
                sf.count(sf.when(both & av.isNull() & bv.isNotNull(), 1)).alias(f'null_a{i}'),
                sf.count(sf.when(both & bv.isNull() & av.isNotNull(), 1)).alias(f'null_b{i}'),
                (sf.max(sf.when(mismatch, gap)) if column.numeric else sf.lit(None).cast('double')).alias(f'gap{i}'),
            ]
        totals = joined.agg(*aggregates).collect()[0]
        differences = tuple(
            ColumnDifference(
                column=column.name,
                mismatches=totals[f'mismatch{i}'],
                null_only_in_a=totals[f'null_a{i}'],
                null_only_in_b=totals[f'null_b{i}'],
                max_abs_diff=totals[f'gap{i}'],
                within_tolerance=totals[f'tolerated{i}'],
                max_abs_diff_within_tolerance=totals[f'tolerated_gap{i}'],
            )
            for i, column in enumerate(value_columns)
        )
        key_fields = [f'a_k{j}' for j in range(len(key_columns))]
        differing = []
        for i, (column, difference) in enumerate(zip(value_columns, differences, strict=True)):
            if difference.mismatches:
                # The table already counts nulls found on one side, so changed values sample first.
                null_on_one_side = sf.col(f'a_v{i}').isNull() | sf.col(f'b_v{i}').isNull()
                sample = joined.where(mismatches[i]).orderBy(null_on_one_side).select(*key_fields, f'a_v{i}', f'b_v{i}')
                for row in sample.limit(sample_rows).collect():
                    differing.append(DifferingValue(tuple(row[k] for k in key_fields), column.name, row[f'a_v{i}'], row[f'b_v{i}']))

        def side_columns(prefix: str) -> list[str]:
            return [f'{prefix}_k{j}' for j in range(len(key_columns))] + [f'{prefix}_v{i}' for i in range(len(value_columns))]

        only_a = joined.where(in_a & ~in_b).select(*side_columns('a')).limit(sample_rows).collect() if totals['only_a'] else []
        only_b = joined.where(in_b & ~in_a).select(*side_columns('b')).limit(sample_rows).collect() if totals['only_b'] else []
    finally:
        joined.unpersist()
    return {
        'rows_a': totals['rows_a'],
        'rows_b': totals['rows_b'],
        'matched_rows': totals['matched'],
        'only_in_a_rows': totals['only_a'],
        'only_in_b_rows': totals['only_b'],
        'duplicate_keys_a': duplicates['a'],
        'duplicate_keys_b': duplicates['b'],
        'column_differences': differences,
        'statistic_differences': (),
        'differing_values': tuple(differing),
        'only_in_a_sample': tuple(tuple(row) for row in only_a),
        'only_in_b_sample': tuple(tuple(row) for row in only_b),
        'sample_columns': tuple(c.name for c in (*key_columns, *value_columns)),
    }


def compare_unkeyed(
    a: DataFrame,
    b: DataFrame,
    value_columns: list[ComparedColumn],
    rel_tol: float,
    abs_tol: float,
    sample_rows: int,
) -> dict[str, Any]:
    import pyspark.sql.functions as sf

    def side(df: DataFrame) -> DataFrame:
        return df.select(*[prepared(c).alias(f'v{i}') for i, c in enumerate(value_columns)])

    def statistics(df: DataFrame) -> dict[str, Any]:
        aggregates = [sf.count(sf.lit(1)).alias('rows')]
        for i, column in enumerate(value_columns):
            value = sf.col(f'v{i}')
            aggregates += [sf.count(sf.when(value.isNull(), 1)).alias(f'nulls{i}'), sf.approx_count_distinct(value).alias(f'distinct{i}')]
            if column.kind != 'json':
                aggregates += [sf.min(value).alias(f'min{i}'), sf.max(value).alias(f'max{i}')]
            if column.numeric:
                summed = value.cast('decimal(38,0)') if column.kind == 'exact' and column.type_a.endswith('int') else value
                aggregates.append(sf.sum(summed).alias(f'sum{i}'))
        return df.agg(*aggregates).collect()[0].asDict()

    def close(x: Any, y: Any) -> bool:
        return x is not None and y is not None and abs(x - y) <= abs_tol + rel_tol * abs(y)

    sides = {'a': side(a), 'b': side(b)}
    stats = {prefix: statistics(df) for prefix, df in sides.items()}
    differences = []
    labels = {'nulls': 'nulls', 'distinct': 'approx distinct', 'min': 'min', 'max': 'max', 'sum': 'sum'}
    for i, column in enumerate(value_columns):
        for stat, label in labels.items():
            field = f'{stat}{i}'
            if field not in stats['a']:
                continue
            x, y = stats['a'][field], stats['b'][field]
            same = x == y or (stat in ('min', 'max', 'sum') and column.kind == 'tolerance' and close(x, y))
            if not same:
                differences.append(StatisticDifference(column.name, label, x, y))

    def rounded(df: DataFrame) -> DataFrame:
        return df.select(
            *[
                sf.round(sf.col(f'v{i}'), UNKEYED_ROUND_DIGITS).alias(f'v{i}') if c.kind == 'tolerance' else sf.col(f'v{i}')
                for i, c in enumerate(value_columns)
            ]
        )

    only: dict[str, tuple[int, list[Any]]] = {}
    if value_columns:
        for prefix, other in (('a', 'b'), ('b', 'a')):
            extra = rounded(sides[prefix]).exceptAll(rounded(sides[other])).cache()
            try:
                extra_rows = extra.count()
                only[prefix] = (extra_rows, extra.limit(sample_rows).collect() if extra_rows else [])
            finally:
                extra.unpersist()
    else:
        only = {'a': (stats['a']['rows'], []), 'b': (stats['b']['rows'], [])}
    return {
        'rows_a': stats['a']['rows'],
        'rows_b': stats['b']['rows'],
        'matched_rows': None,
        'only_in_a_rows': only['a'][0],
        'only_in_b_rows': only['b'][0],
        'duplicate_keys_a': 0,
        'duplicate_keys_b': 0,
        'column_differences': (),
        'statistic_differences': tuple(differences),
        'differing_values': (),
        'only_in_a_sample': tuple(tuple(row) for row in only['a'][1]),
        'only_in_b_sample': tuple(tuple(row) for row in only['b'][1]),
        'sample_columns': tuple(c.name for c in value_columns),
    }
