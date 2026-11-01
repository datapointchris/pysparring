"""Whether two DataFrames hold the same rows, and when they do not, which columns differ and how."""

from __future__ import annotations

import dataclasses as dc
import enum
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


class ColumnKind(enum.StrEnum):
    """How a column's values compare."""

    TOLERANCE = 'tolerance'  # floats and doubles, and numbers of two types compared as double
    JSON = 'json'  # structs, arrays and maps, compared as their JSON text
    EXACT = 'exact'


@dc.dataclass(frozen=True)
class ComparedColumn:
    """A column both sides hold. `cast` is the type both sides become when their types differ."""

    name: str
    type_a: str
    type_b: str
    cast: str | None
    kind: ColumnKind
    numeric: bool
    integral: bool


@dc.dataclass(frozen=True)
class SchemaDifference:
    columns_only_in_a: tuple[tuple[str, str], ...]
    columns_only_in_b: tuple[tuple[str, str], ...]
    type_changes: tuple[ComparedColumn, ...]

    @property
    def same(self) -> bool:
        return not (self.columns_only_in_a or self.columns_only_in_b or self.type_changes)


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


@dc.dataclass(frozen=True, repr=False)
class OutputComparison:
    """Schema, rows and columns of two outputs. Row order, column order and nullability are ignored.

    A field ending in `_a` or `_b` describes `names[0]` or `names[1]`. A field is None where the
    comparison did not measure it: a keyed comparison has no column statistics, and one without
    keys has no matched rows or per-column mismatches.
    """

    names: tuple[str, str]
    keys: tuple[str, ...]
    schema: SchemaDifference
    rows_a: int
    rows_b: int
    only_in_a_rows: int
    only_in_b_rows: int
    matched_rows: int | None
    duplicate_keys_a: int | None
    duplicate_keys_b: int | None
    column_differences: tuple[ColumnDifference, ...] | None
    differing_values: tuple[DifferingValue, ...] | None
    statistic_differences: tuple[StatisticDifference, ...] | None
    rows_within_tolerance: int | None
    only_in_a_sample: tuple[tuple[Any, ...], ...]
    only_in_b_sample: tuple[tuple[Any, ...], ...]
    sample_columns: tuple[str, ...]

    @property
    def matches(self) -> bool:
        return (
            self.schema.same
            and self.rows_a == self.rows_b
            and self.only_in_a_rows == 0
            and self.only_in_b_rows == 0
            and not any(difference.mismatches for difference in self.column_differences or ())
        )

    def assert_matches(self) -> None:
        if not self.matches:
            raise AssertionError('\n' + str(self))

    def __str__(self) -> str:
        return plain_text(self)

    def __repr__(self) -> str:
        a, b = self.names
        how = f'keyed on {", ".join(self.keys)}' if self.keys else 'without keys'
        return f'<OutputComparison {a} vs {b}, {how}: {"match" if self.matches else "differ"}>'

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
            footer += ' Rows pair by their other columns, and floats and doubles match within the tolerance.'
            footer += ' Pass keys= to see which columns differ.'
        yield Text(footer, style='dim')

    def schema_section(self) -> Iterator[Any]:
        a, b = self.names
        yield Text('\nSchema', style='bold')
        if self.schema.same:
            yield Text('  same columns and types')
            return
        for side, columns in ((a, self.schema.columns_only_in_a), (b, self.schema.columns_only_in_b)):
            for name, type_name in columns:
                yield Text(f'  only in {side}    {name}   {type_name}')
        for change in self.schema.type_changes:
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
        if self.rows_within_tolerance:
            yield Text(f'  {counted(self.rows_within_tolerance, "row")} matched only within the tolerance', style='dim')

    def columns_section(self) -> Iterator[Any]:
        a, b = self.names
        if self.column_differences is not None:
            mismatched = [d for d in self.column_differences if d.mismatches]
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
        if self.statistic_differences:
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
            total = sum(d.mismatches for d in self.column_differences or ())
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


def within_tolerance(a: Column, b: Column, rel_tol: float, abs_tol: float) -> Column:
    """Equal, or both set and `|a - b| <= abs_tol + rel_tol * |b|`, numpy's isclose convention."""
    import pyspark.sql.functions as sf

    close = a.isNotNull() & b.isNotNull() & (sf.abs(a - b) <= sf.lit(abs_tol) + sf.lit(rel_tol) * sf.abs(b))
    return a.eqNullSafe(b) | close


def values_within_tolerance(a: Any, b: Any, rel_tol: float, abs_tol: float) -> bool:
    """within_tolerance for two values already on the driver."""
    return a == b or (a is not None and b is not None and abs(a - b) <= abs_tol + rel_tol * abs(b))


def compared_column(name: str, type_a: Any, type_b: Any) -> ComparedColumn:
    import pyspark.sql.types as st

    a, b = type_a.simpleString(), type_b.simpleString()
    if a == b:
        numeric, integral = isinstance(type_a, st.NumericType), isinstance(type_a, st.IntegralType)
        if isinstance(type_a, (st.StructType, st.ArrayType, st.MapType)):
            return ComparedColumn(name, a, b, None, ColumnKind.JSON, False, False)
        if isinstance(type_a, (st.FloatType, st.DoubleType)):
            return ComparedColumn(name, a, b, None, ColumnKind.TOLERANCE, True, False)
        return ComparedColumn(name, a, b, None, ColumnKind.EXACT, numeric, integral)
    if isinstance(type_a, st.IntegralType) and isinstance(type_b, st.IntegralType):
        return ComparedColumn(name, a, b, 'bigint', ColumnKind.EXACT, True, True)
    if isinstance(type_a, st.NumericType) and isinstance(type_b, st.NumericType):
        return ComparedColumn(name, a, b, 'double', ColumnKind.TOLERANCE, True, False)
    return ComparedColumn(name, a, b, 'string', ColumnKind.EXACT, False, False)


def prepared(column: ComparedColumn) -> Column:
    """The column as it is compared: cast to the shared type, or as JSON text."""
    import pyspark.sql.functions as sf

    value = sf.col(quoted(column.name))
    match column.kind:
        case ColumnKind.JSON:
            return sf.to_json(value)
        case ColumnKind.TOLERANCE | ColumnKind.EXACT:
            return value.cast(column.cast) if column.cast else value


def compare_outputs(
    a: DataFrame,
    b: DataFrame,
    keys: str | Sequence[str] | None = None,
    names: Sequence[str] = ('df1', 'df2'),
    rel_tol: float = 1e-9,
    abs_tol: float = 0.0,
    ignore_columns: Sequence[str] = (),
    sample_rows: int = 5,
) -> OutputComparison:
    """Print whether a and b hold the same rows, and when they do not, which columns differ and how.

    A float or double matches when `|a - b| <= abs_tol + rel_tol * |b|`. With `keys`, rows pair by
    key and every column reports its mismatches. Without keys, rows pair by their other columns.
    Everything runs on Spark, and only counts and samples reach the driver.
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
    schema = SchemaDifference(
        columns_only_in_a=tuple((n, t.simpleString()) for n, t in fields_a.items() if n not in fields_b),
        columns_only_in_b=tuple((n, t.simpleString()) for n, t in fields_b.items() if n not in fields_a),
        type_changes=tuple(c for c in shared.values() if c.cast),
    )
    key_columns = [shared[k] for k in key_names]
    value_columns = [c for c in shared.values() if c.name not in key_names]
    names_pair = (labels[0], labels[1])
    if key_names:
        comparison = compare_keyed(a, b, names_pair, schema, key_columns, value_columns, rel_tol, abs_tol, sample_rows)
    elif value_columns:
        comparison = compare_unkeyed(a, b, names_pair, schema, value_columns, rel_tol, abs_tol, sample_rows)
    else:
        raise ValueError(f'{labels[0]} and {labels[1]} share no columns left to compare')
    show(comparison)
    return comparison


def compare_keyed(
    a: DataFrame,
    b: DataFrame,
    names: tuple[str, str],
    schema: SchemaDifference,
    key_columns: list[ComparedColumn],
    value_columns: list[ComparedColumn],
    rel_tol: float,
    abs_tol: float,
    sample_rows: int,
) -> OutputComparison:
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
            match column.kind:
                case ColumnKind.TOLERANCE:
                    close = within_tolerance(av, bv, rel_tol, abs_tol)
                    mismatch = both & ~close
                    tolerated = both & ~equal & close
                    aggregates += [
                        sf.count(sf.when(tolerated, 1)).alias(f'tolerated{i}'),
                        sf.max(sf.when(tolerated, gap)).alias(f'tolerated_gap{i}'),
                    ]
                case ColumnKind.EXACT | ColumnKind.JSON:
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
    return OutputComparison(
        names=names,
        keys=tuple(c.name for c in key_columns),
        schema=schema,
        rows_a=totals['rows_a'],
        rows_b=totals['rows_b'],
        only_in_a_rows=totals['only_a'],
        only_in_b_rows=totals['only_b'],
        matched_rows=totals['matched'],
        duplicate_keys_a=duplicates['a'],
        duplicate_keys_b=duplicates['b'],
        column_differences=differences,
        differing_values=tuple(differing),
        statistic_differences=None,
        rows_within_tolerance=None,
        only_in_a_sample=tuple(tuple(row) for row in only_a),
        only_in_b_sample=tuple(tuple(row) for row in only_b),
        sample_columns=tuple(c.name for c in (*key_columns, *value_columns)),
    )


def compare_unkeyed(
    a: DataFrame,
    b: DataFrame,
    names: tuple[str, str],
    schema: SchemaDifference,
    value_columns: list[ComparedColumn],
    rel_tol: float,
    abs_tol: float,
    sample_rows: int,
) -> OutputComparison:
    import pyspark.sql.functions as sf
    from pyspark.sql import Window

    def side(df: DataFrame) -> DataFrame:
        return df.select(*[prepared(c).alias(f'v{i}') for i, c in enumerate(value_columns)])

    def statistics(df: DataFrame) -> dict[str, Any]:
        aggregates = [sf.count(sf.lit(1)).alias('rows')]
        for i, column in enumerate(value_columns):
            value = sf.col(f'v{i}')
            aggregates += [sf.count(sf.when(value.isNull(), 1)).alias(f'nulls{i}'), sf.approx_count_distinct(value).alias(f'distinct{i}')]
            match column.kind:
                case ColumnKind.TOLERANCE | ColumnKind.EXACT:
                    aggregates += [sf.min(value).alias(f'min{i}'), sf.max(value).alias(f'max{i}')]
                case ColumnKind.JSON:
                    pass
            if column.numeric:
                aggregates.append(sf.sum(value.cast('decimal(38,0)') if column.integral else value).alias(f'sum{i}'))
        return df.agg(*aggregates).collect()[0].asDict()

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
            tolerant = column.kind is ColumnKind.TOLERANCE and stat in ('min', 'max', 'sum')
            same = values_within_tolerance(x, y, rel_tol, abs_tol) if tolerant else x == y
            if not same:
                differences.append(StatisticDifference(column.name, label, x, y))

    # Rows with an exact twin on the other side leave first. The rest pair by their exact columns,
    # in order of their float columns, and a pair whose floats are all within the tolerance matches.
    exact = [f'v{i}' for i, c in enumerate(value_columns) if c.kind is not ColumnKind.TOLERANCE]
    floats = [f'v{i}' for i, c in enumerate(value_columns) if c.kind is ColumnKind.TOLERANCE]

    def numbered(df: DataFrame, prefix: str) -> DataFrame:
        order = Window.partitionBy(*exact).orderBy(*(floats or exact))
        return df.select(
            *[sf.col(f'v{i}').alias(f'{prefix}_v{i}') for i in range(len(value_columns))],
            sf.row_number().over(order).alias(f'{prefix}_row'),
        )

    extra_a = numbered(sides['a'].exceptAll(sides['b']), 'a')
    extra_b = numbered(sides['b'].exceptAll(sides['a']), 'b')
    condition = [sf.col(f'a_{c}').eqNullSafe(sf.col(f'b_{c}')) for c in exact] + [sf.col('a_row') == sf.col('b_row')]
    close = sf.col('a_row').isNotNull() & sf.col('b_row').isNotNull()
    for c in floats:
        close = close & within_tolerance(sf.col(f'a_{c}'), sf.col(f'b_{c}'), rel_tol, abs_tol)
    paired = extra_a.join(extra_b, on=condition, how='full_outer').withColumn('close', close).cache()
    try:
        in_a, in_b = sf.col('a_row').isNotNull(), sf.col('b_row').isNotNull()
        found = paired.agg(
            sf.count(sf.when(in_a & ~sf.col('close'), 1)).alias('only_a'),
            sf.count(sf.when(in_b & ~sf.col('close'), 1)).alias('only_b'),
            sf.count(sf.when(sf.col('close'), 1)).alias('within_tolerance'),
        ).collect()[0]

        def sample(prefix: str, present: Column, rows: int) -> list[Any]:
            if not rows:
                return []
            columns = [f'{prefix}_v{i}' for i in range(len(value_columns))]
            return paired.where(present & ~sf.col('close')).select(*columns).limit(sample_rows).collect()

        only_a = sample('a', in_a, found['only_a'])
        only_b = sample('b', in_b, found['only_b'])
    finally:
        paired.unpersist()
    return OutputComparison(
        names=names,
        keys=(),
        schema=schema,
        rows_a=stats['a']['rows'],
        rows_b=stats['b']['rows'],
        only_in_a_rows=found['only_a'],
        only_in_b_rows=found['only_b'],
        matched_rows=None,
        duplicate_keys_a=None,
        duplicate_keys_b=None,
        column_differences=None,
        differing_values=None,
        statistic_differences=tuple(differences),
        rows_within_tolerance=found['within_tolerance'],
        only_in_a_sample=tuple(tuple(row) for row in only_a),
        only_in_b_sample=tuple(tuple(row) for row in only_b),
        sample_columns=tuple(c.name for c in value_columns),
    )
