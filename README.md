# pysparring

Put two PySpark DataFrames in the ring and see how they differ: their physical plans, how fast they
run, and whether their outputs match.

```python
from pysparring import compare_outputs
from pysparring import compare_plans

old = join_then_aggregate(orders, customers)
new = aggregate_then_join(orders, customers)

compare_plans(old, new, names=['old', 'new'])           # colored plan diff, then the operators that differ
compare_plans(old, new, names=['old', 'new'], runs=5)   # also times both and says which is faster
compare_outputs(old, new, keys='segment', names=('old', 'new'))
```

Both functions take DataFrames from any session: local pyspark, a Docker container, or a notebook
on a cluster, classic or Spark Connect. Each prints its report and returns it as an object.

## `compare_plans` diffs plans and, when asked, times them

`compare_plans(*dataframes, names=None, runs=0)` takes two or more DataFrames and compares each with
the first.

- It diffs each plan as `explain("formatted")` prints it, after stripping the ids Spark renumbers on
  every build. A plan built twice reads as identical.
- It lists every operator whose count differs, such as a `SortMergeJoin` that replaced a
  `BroadcastHashJoin`.
- It never names a winner from a plan. Spark's explain carries no total cost, and a plan with more
  operators can run faster, because it shuffles fewer bytes.
- `runs=5` or more times each DataFrame with a no-op write: three discarded warm-up rounds, then every
  round in a shuffled order. A DataFrame is called faster only when an exact Mann-Whitney U test gives
  p < 0.01 and its median is at least 1.1x better. Otherwise the report reads "no clear difference".
- The plan diff prints before the timing starts.
- The session's cache is left alone. A DataFrame built on a cached table is timed reading the cache,
  and the report says so.

The returned `PlanComparison` holds the plans, the diffs, the operator counts, the run times and the
verdicts, each keyed by name.

## `compare_outputs` says which columns differ and how

`compare_outputs(a, b, keys=None, names=('df1', 'df2'), rel_tol=1e-9, abs_tol=0.0, ignore_columns=(), sample_rows=5)`
reports, in order:

- **Schema**: columns found on one side only, and type changes. A changed column is still compared,
  after casting both sides to one type.
- **Rows**: the count on each side, and how many are on one side only. With `keys`, also how many
  rows matched and how many keys appear more than once. Without keys, a row pairs with one that has
  the same values in every other column, and floats and doubles within the tolerance.
- **Columns**: with `keys`, each column's mismatches on the matched rows, the nulls found on only one
  side, and its largest absolute difference. Without keys, each column's null count, approximate
  distinct count, min, max and sum, where they differ.
- **Samples**: differing values side by side, then rows found on only one side.

A float or double matches when `|a - b| <= abs_tol + rel_tol * |b|`, so two aggregations that sum in
a different order still match. Row order, column order and nullability are ignored. Everything runs
on Spark, and only counts and samples reach the driver.

The returned `OutputComparison` has `.matches`, and a field ending in `_a` or `_b` describes
`names[0]` or `names[1]`. `.assert_matches()` raises an `AssertionError` carrying the whole report,
so a test can gate a refactor:

```python
def test_refactor_keeps_the_output(spark):
    compare_outputs(old_pipeline(spark), new_pipeline(spark), keys=['customer_id']).assert_matches()
```

## Comparing on a sample of real tables

- **Sample related tables by the same key hash.** `.sample(0.01)` on two tables keeps about 0.01% of
  the rows that join. A filter on a hash of the join key keeps the same keys in every table:

  ```python
  import pyspark.sql.functions as sf

  def sample_by_key(df, key, percent):
      return df.where(sf.pmod(sf.xxhash64(key), sf.lit(100)) < percent)

  orders = sample_by_key(spark.read.parquet('s3://bucket/orders'), 'customer_id', 1)
  customers = sample_by_key(spark.read.parquet('s3://bucket/customers'), 'customer_id', 1)
  ```

  `pmod` matters. `xxhash64` returns negative numbers, and `%` keeps their sign, so `% 100 < 1`
  keeps about half the rows. Hash the key at one type in every table, because an `int` 42 and a
  `bigint` 42 hash differently.
- **A sample can broadcast a join that production sort-merges.** A sample written to files and read
  back falls under `spark.sql.autoBroadcastJoinThreshold`, 10 MB by default, so its plan
  broadcasts. A sample filtered in the same job plans a sort-merge join. Adaptive execution then
  switches it to a broadcast at runtime, which the plan `compare_plans` prints cannot show but its
  timings measure. A threshold of `-1` keeps both on sort-merge.
- **Timings on a sample rank the plans at that size.** A plan's fixed cost is a larger share of a
  small run.

## Install

pysparring installs into the environment whose Spark it compares on, and never installs pyspark.
It needs Python 3.11 or later, and rich. The AWS Glue 5 image runs Python 3.11. The official
`apache/spark` images run Python 3.10, so it does not install there.

```bash
uv add --dev --index https://pypi.ichrisbirch.com/simple/ pysparring
```

Every release is published to that package index.

## Develop

```bash
task setup      # install into .venv
task test
task lint
fixtures/regenerate.sh   # recapture the Spark output the tests read, in Docker
```

## License

MIT
