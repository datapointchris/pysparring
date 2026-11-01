# pysparring

Put two PySpark DataFrames in the ring and see how they differ: their physical plans, how fast they
run, and whether their outputs match.

```python
from pysparring import compare_outputs
from pysparring import compare_plans

old = join_then_aggregate(orders, customers)
new = aggregate_then_join(orders, customers)

compare_outputs(old, new, keys='segment', names=('old', 'new'))   # same rows? which columns differ?
compare_plans(old, new, names=['old', 'new'], runs=5)            # plan diff, operator counts, timing
```

Both functions take DataFrames from any session: local pyspark, a Docker container, or a notebook
on a cluster, classic or Spark Connect. Each prints its report and returns it as an object.

The [user guide](docs/user-guide.md) covers installing on each platform, reading every part of both
reports, choosing keys, tolerances and run counts, testing with `assert_matches`, and sampling real
tables so they still join.

## `compare_outputs` says which columns differ and how

```text
old vs new: outputs differ, keyed on customer_id

Schema
  only in new    discount_pct   double
  type changed   total   double → decimal(18,2), compared as double

Rows
  old 2,000   new 2,002
  matched 1,999   only in old 1   only in new 3
  new has 3 keys appearing more than once

Columns on matched rows, most mismatches first
┏━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ column  ┃ differ ┃ null only in old ┃ null only in new ┃ max abs diff ┃
┡━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ total   │  1,996 │                0 │                0 │        0.005 │
│ segment │    285 │                0 │              284 │              │
│ n       │      1 │                0 │                0 │            1 │
└─────────┴────────┴──────────────────┴──────────────────┴──────────────┘
```

The report goes on to show differing values side by side, then rows found on one side only.

- **With `keys`**, rows pair by a null-safe join on the keys, and each column reports its
  mismatches, its nulls found on one side only, and its largest difference.
- **Without keys**, rows pair by their other columns, and column statistics stand in for the
  per-column report.
- **A float or double matches** when `|a - b| <= abs_tol + rel_tol * |b|`, so two aggregations that
  sum in a different order still match.
- **Row order, column order and nullability are ignored.** Everything runs on Spark, and only counts
  and samples reach the driver.

`.assert_matches()` raises an `AssertionError` carrying the whole report, so a test can gate a
refactor:

```python
def test_refactor_keeps_the_output(spark):
    compare_outputs(old_pipeline(spark), new_pipeline(spark), keys=['customer_id']).assert_matches()
```

## `compare_plans` diffs plans and, when asked, times them

```text
Plan diff, join first → agg first
--- join first
+++ agg first
@@ -6,9 +6,11 @@
          +- Project
             +- SortMergeJoin Inner
                :- Sort
-               :  +- Exchange
-               :     +- Filter
-               :        +- Scan parquet
+               :  +- HashAggregate
+               :     +- Exchange
+               :        +- HashAggregate
+               :           +- Filter
+               :              +- Scan parquet
```

The node details follow in the same diff, then the operators whose counts differ and the timing:

```text
Operators whose counts differ
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ operator      ┃ join first ┃ agg first ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ HashAggregate │          2 │         4 │
└───────────────┴────────────┴───────────┘

Run times, ms
┏━━━━━━━━━━━━┳━━━━━━━━┳━━━━━┳━━━━━━━┳━━━━━━┓
┃            ┃ median ┃ min ┃   max ┃ runs ┃
┡━━━━━━━━━━━━╇━━━━━━━━╇━━━━━╇━━━━━━━╇━━━━━━┩
│ join first │    944 │ 707 │ 1,359 │    6 │
│ agg first  │    519 │ 422 │   584 │    6 │
└────────────┴────────┴─────┴───────┴──────┘
agg first ran 1.82x faster than join first (p=0.0022, 6 runs each)
```

- **The diff compares plans as `explain("formatted")` prints them**, with the ids Spark renumbers
  on every build stripped. A plan built twice reads as identical.
- **It never names a winner from a plan.** Spark's explain carries no total cost, and above, the
  plan with more operators ran faster, because it shuffles fewer rows.
- **`runs=5` or more times each DataFrame** with a no-op write: three discarded warm-up rounds, then
  every round in a shuffled order. A DataFrame is called faster only when an exact Mann-Whitney U
  test gives p < 0.01 and its median is at least 1.1x better. Otherwise the report reads "no clear
  difference".
- **The plans print before the timing starts**, and the session's cache is left alone.

## Install

pysparring installs into the environment whose Spark it compares on, and never installs pyspark.
It needs Python 3.11 or later, and rich. Every release is published to its own package index, and
rich comes from PyPI.

```bash
uv add --dev --index https://pypi.ichrisbirch.com/simple/ pysparring
```

The AWS Glue 5 image and Glue 5.0 and 5.1 jobs run Python 3.11. The official `apache/spark` images
run Python 3.10, so they need Python 3.11 installed first. The user guide gives the steps for pip,
Glue jobs and notebooks, Spark Connect and those images.

## Develop

```bash
task setup      # install into .venv
task test
task lint
fixtures/regenerate.sh   # recapture the Spark output the tests read, in Docker
```

[docs/](docs/README.md) holds the user guide, and why each comparison is built the way it is.

## License

MIT
