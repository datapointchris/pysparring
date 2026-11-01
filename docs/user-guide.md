# Using pysparring

pysparring is two functions for rewriting a PySpark transform with evidence.

- `compare_outputs(a, b, keys=...)` says whether two DataFrames hold the same rows. When they do
  not, it names the columns that differ, how many rows each, and shows the values side by side.
- `compare_plans(a, b, ..., runs=...)` diffs the physical plans Spark chose for two or more
  DataFrames, and counts the operators each added or dropped. With `runs`, it times them and says
  which is faster, only when the difference is significant.

Each prints its report and returns it as an object. They take DataFrames from any session, classic
or Spark Connect, local or on a cluster.

Contents:

1. [Install beside the pyspark your Spark already runs](#pysparring-installs-beside-the-pyspark-your-spark-already-runs)
2. [A first comparison](#a-first-comparison-is-two-calls-on-two-dataframes)
3. [`compare_plans`](#compare_plans-shows-how-the-plans-differ-and-times-them-when-asked)
4. [`compare_outputs`](#compare_outputs-says-which-columns-differ-and-keys-decide-how-much-it-can-say)
5. [In a test](#assert_matches-turns-a-comparison-into-a-test)
6. [A refactor, step by step](#a-refactor-goes-outputs-first-then-plans-then-time)
7. [Comparing on a sample of real tables](#a-sample-of-real-tables-keeps-the-same-keys-in-every-table)
8. [Color, notebooks and logs](#the-report-prints-in-color-where-it-can-and-as-plain-text-where-it-cannot)
9. [Troubleshooting](#troubleshooting-each-error-names-its-cause)
10. [What pysparring does not do](#what-pysparring-does-not-do)

## pysparring installs beside the pyspark your Spark already runs

pysparring needs Python 3.11 or later, and rich. It never installs pyspark. Your environment
already has the pyspark that matches its cluster: local `pyspark`, `pyspark[connect]`, or
`pyspark-client`. Declaring one would install a second over it.

Releases are published to `https://pypi.ichrisbirch.com/simple/`. That index holds pysparring
alone, and rich comes from PyPI. So each recipe below installs pysparring from the index and rich
from PyPI, and pip never looks for a `pysparring` on PyPI.

### Locally, uv takes pysparring from its index and the rest from PyPI

```bash
uv add --dev pyspark
uv add --dev --index https://pypi.ichrisbirch.com/simple/ pysparring
```

uv takes a package from the first index that has it, so pysparring comes from its index and rich
from PyPI. pyspark needs a Java runtime, and Spark 4 needs Java 17 or later. Pin pyspark to your
cluster's version, so local plans match the cluster's. The Glue 5 image runs Spark 3.5.4, so
`pyspark==3.5.4` matches it.

With pip, install the two separately:

```bash
pip install --no-deps --index-url https://pypi.ichrisbirch.com/simple/ pysparring
pip install rich
```

### The AWS Glue 5 image runs Python 3.11, so it installs directly

The Glue 5 image runs Python 3.11 and Spark 3.5.4-amzn-0. It is the closest local match to a Glue
5 job.

```bash
docker run --rm -it --user 0 -e HOME=/tmp -v "$PWD:/work" -w /work \
  --entrypoint bash public.ecr.aws/glue/aws-glue-libs:5

# inside the container
python3 -m pip install --no-deps --index-url https://pypi.ichrisbirch.com/simple/ pysparring
python3 -m pip install rich
spark-submit --master 'local[4]' compare.py
```

### A Glue job takes the wheel from S3

Glue 5.0 and 5.1 run Python 3.11, and Glue 6.0 runs 3.13. Glue 4.0 runs Python 3.10, which
pysparring does not support.

Download the wheel on any Python 3.11 or later, and upload it:

```bash
pip download --no-deps --index-url https://pypi.ichrisbirch.com/simple/ --dest dist pysparring
aws s3 cp dist/pysparring-0.3.0-py3-none-any.whl s3://my-bucket/libs/
```

Then give the job this parameter:

```text
--additional-python-modules   s3://my-bucket/libs/pysparring-0.3.0-py3-none-any.whl,rich
```

Glue installs rich from PyPI when the job starts. A job without internet access needs rich and its
dependencies as wheels as well. AWS's zip-of-wheels form carries them: a `.gluewheels.zip` passed
the same way, with `--python-modules-installer-option --no-index`.

### A Glue notebook installs it before the session starts

In an AWS Glue Studio notebook or an interactive session, put this in the first cell, before any
cell starts the session:

```text
%additional_python_modules rich, s3://my-bucket/libs/pysparring-0.3.0-py3-none-any.whl
```

A running session cannot take new modules. Stop it with `%stop_session`, set the magic, and run
again.

### Spark Connect needs only the client

Install `pyspark[connect]`, or from Spark 4.0 the smaller `pyspark-client`, at the server's
version. Then install pysparring as for local Spark.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote('sc://spark-connect.example.com:15002').getOrCreate()
```

Both functions behave the same over Connect. Every comparison runs on the server, and only counts,
samples and plan text come back.

### The apache/spark images need Python 3.11 installed first

The official `apache/spark` images run Python 3.10.12. Ubuntu's `python3.11` package installs in
them as 3.11.0rc1, and pysparring runs on it.

```bash
docker run --rm -it --user 0 -e HOME=/tmp -v "$PWD:/work" -w /work \
  --entrypoint bash apache/spark:4.2.0-python3

# inside the container
apt-get update && apt-get install -y python3.11
python3 -m pip install --target /opt/pysparring-deps --ignore-requires-python --no-deps \
  --index-url https://pypi.ichrisbirch.com/simple/ pysparring
python3 -m pip install --target /opt/pysparring-deps rich
PYSPARK_PYTHON=python3.11 PYSPARK_DRIVER_PYTHON=python3.11 PYTHONPATH=/opt/pysparring-deps \
  /opt/spark/bin/spark-submit compare.py
```

The image's pip belongs to Python 3.10. `--ignore-requires-python` lets it install a package that
needs 3.11. `--target` puts both packages where `PYTHONPATH` points Python 3.11. pysparring, rich
and rich's dependencies are pure Python, so a 3.10 pip installs them correctly for 3.11.

## A first comparison is two calls on two DataFrames

Two transforms compute total order amounts per customer segment. `join_first` joins every order to
its customer, then aggregates. `agg_first` aggregates per customer first, then joins the smaller
result.

```python
import pyspark.sql.functions as sf

from pysparring import compare_outputs
from pysparring import compare_plans

orders = spark.read.parquet('data/orders')  # order_id, customer_id, amount
customers = spark.read.parquet('data/customers')  # customer_id, segment

join_first = (
    orders.filter(sf.col('amount') > 10)
    .join(customers, 'customer_id')
    .groupBy('segment')
    .agg(sf.sum('amount').alias('total'), sf.count('*').alias('n'))
)
per_customer = (
    orders.filter(sf.col('amount') > 10)
    .groupBy('customer_id')
    .agg(sf.sum('amount').alias('total'), sf.count('*').alias('n'))
)
agg_first = per_customer.join(customers, 'customer_id').groupBy('segment').agg(sf.sum('total').alias('total'), sf.sum('n').alias('n'))

compare_outputs(join_first, agg_first, keys='segment', names=('join first', 'agg first'))
```

The outputs match. Their totals differ in the last digits, because the two transforms add the same
doubles in a different order:

```text
join first vs agg first: outputs match, keyed on segment

Schema
  same columns and types

Rows
  join first 5   agg first 5
  matched 5   only in join first 0   only in agg first 0

Every column matches on the 5 matched rows.
  total: 5 rows differ by at most 3.35e-08, within tolerance
Row order, column order and nullability are ignored.
```

Then compare the plans and time them. Broadcast joins are turned off here, as they would be for
tables too large to broadcast:

```python
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
compare_plans(join_first, agg_first, names=['join first', 'agg first'], runs=5)
```

The report opens with the plan tree's diff. Red lines are only in `join first`, and green lines
only in `agg first`. The node details that follow it are cut here:

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
                +- Sort
                   +- Exchange
                      +- Filter
```

It ends with the operator counts and the timing:

```text
Operators whose counts differ
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ operator      ┃ join first ┃ agg first ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ HashAggregate │          2 │         4 │
└───────────────┴────────────┴───────────┘
Plans as explained before running. Adaptive execution can still change joins and partitions.

Run times, ms
┏━━━━━━━━━━━━┳━━━━━━━━┳━━━━━┳━━━━━┳━━━━━━┓
┃            ┃ median ┃ min ┃ max ┃ runs ┃
┡━━━━━━━━━━━━╇━━━━━━━━╇━━━━━╇━━━━━╇━━━━━━┩
│ join first │    298 │ 272 │ 306 │    5 │
│ agg first  │    285 │ 225 │ 301 │    5 │
└────────────┴────────┴─────┴─────┴──────┘
no clear difference between join first and agg first (ratio 1.05x, p=0.310, 5 runs each)
```

`agg_first` plans two more `HashAggregate`s, and on 400,000 orders neither ran faster. On 2,000,000
orders, the same pair gives a verdict:

```text
Run times, ms
┏━━━━━━━━━━━━┳━━━━━━━━┳━━━━━┳━━━━━━━┳━━━━━━┓
┃            ┃ median ┃ min ┃   max ┃ runs ┃
┡━━━━━━━━━━━━╇━━━━━━━━╇━━━━━╇━━━━━━━╇━━━━━━┩
│ join first │    944 │ 707 │ 1,359 │    6 │
│ agg first  │    519 │ 422 │   584 │    6 │
└────────────┴────────┴─────┴───────┴──────┘
agg first ran 1.82x faster than join first (p=0.0022, 6 runs each)
```

More operators ran in about half the time. Its first shuffle carries one partial total per
customer instead of every order. That is why pysparring never names a winner from a plan, and only
measured time does.

## `compare_plans` shows how the plans differ, and times them when asked

```python
compare_plans(*dataframes, names=None, runs=0) -> PlanComparison
```

### The diff compares plans with Spark's renumbered ids stripped

Each plan is what `df.explain(mode="formatted")` prints: a tree of numbered nodes, then each node's
details, such as its input columns, keys and arguments. Spark numbers several things afresh every
time a DataFrame is built. A diff of the raw text would mark most lines even for one transform
built twice. So pysparring strips each of these before diffing:

- expression ids, as in `amount#10` and `count#103L`;
- exchange ids, as in `[plan_id=639]`, and `[id=#12]` before Spark 3.2;
- codegen stage ids, as in `[codegen id : 3]`;
- query stage numbers, as in `ShuffleQueryStage 2`;
- the node numbers, as in `(12)`, and every `Reuses operator id` or `Hosting operator id` pointing
  at them.

Each rewrite is anchored to the syntax around its number, so `count(1)` and `amount > 10.0` are left
alone. One DataFrame against itself reads `identical once ids are stripped`.

In the diff, a red line is only in the first plan, and a green line only in the plan it is compared
with. A cyan `@@` line gives the line numbers of the hunk that follows. The tree comes first, and
the node details follow it in the same diff, so a changed join shows in both.

### The operator table counts what each plan added or dropped

Under the diffs, one table lists every operator whose count is not the same in every plan. An
operator is the node's name without its trailing bracket, such as `Scan parquet`, `SortMergeJoin`
or `ReusedExchange`. A row of counts often says more than the diff. Here a broadcast hint on the
third DataFrame trades a sort-merge join, its two sorts and two shuffles for one broadcast:

```text
Operators whose counts differ
┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ operator          ┃ join first ┃ agg first ┃ broadcast hint ┃
┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ BroadcastExchange │          0 │         0 │              1 │
│ BroadcastHashJoin │          0 │         0 │              1 │
│ Exchange          │          3 │         3 │              1 │
│ HashAggregate     │          2 │         4 │              2 │
│ Sort              │          2 │         2 │              0 │
│ SortMergeJoin     │          1 │         1 │              0 │
└───────────────────┴────────────┴───────────┴────────────────┘
```

When every count matches, the report says `Every operator count matches.` The plans can still
differ, in a filter's condition or a projection's columns, and the diff shows where.

### The plan is Spark's plan before running

With adaptive query execution on, the default since Spark 3.2, the plan carries `isFinalPlan=false`
and the report adds a dim line:

```text
Plans as explained before running. Adaptive execution can still change joins and partitions.
```

At runtime, adaptive execution can switch a sort-merge join to a broadcast, coalesce shuffle
partitions, and split skewed ones. The plan pysparring prints cannot show those changes. The
timings measure the plan that ran, changes included. The SQL tab of the Spark UI shows the final
plan of each run.

### Each DataFrame after the first is compared with the first

Pass three or more DataFrames to compare several candidates at once. The first is the reference.
Each other one gets its own diff against it, its own column in the operator table, and its own
speed verdict.

```python
compare_plans(current, candidate_a, candidate_b, names=['current', 'a', 'b'], runs=5)
```

Names default to `df1`, `df2` and so on. They must be distinct, because they key every dict in the
result.

### `runs` times each DataFrame, and names a faster one only at p < 0.01 and 1.1x

`runs=5` or more times each DataFrame. Each run writes the DataFrame to Spark's `noop` sink, which
executes the whole plan and writes nothing. `count()` would let the optimizer drop columns, and
`collect()` would add the transfer to the driver.

The schedule:

1. Three warm-up rounds run every DataFrame and are discarded. The first runs pay for JIT
   compilation and file listing.
2. `runs` measured rounds follow. Each round runs every DataFrame once, in a shuffled order. A
   slow moment on the cluster then lands on every DataFrame, not on one.
3. Each DataFrame after the first is tested against the first with an exact two-sided Mann-Whitney
   U test.

A DataFrame is called faster only when p < 0.01 and the slower median is at least 1.1 times the
faster. The ratio floor keeps a real but trivial difference from reading as a win.

Timing costs `(runs + 3) × number of DataFrames` full executions. The plans print before timing
starts, so a run that fails or is interrupted leaves the diff on screen. While it runs, a status
line on stderr shows the round.

### More runs let a faster DataFrame lose a few pairings and still win

The test compares every run of one DataFrame with every run of the other. With 5 runs each, the
smallest p-value it can reach is 0.0079. So at 5 runs, every one of the faster DataFrame's runs
must beat every one of the slower one's. One overlap lifts p to 0.016, and the report reads "no
clear difference". Below 5 runs, p can never reach 0.01, so `runs` from 1 to 4 raises a
`ValueError`.

| runs | pairings | pairings the slower DataFrame may win, at p < 0.01 |
| --- | --- | --- |
| 5 | 25 | 0 |
| 6 | 36 | 2 |
| 8 | 64 | 7 |
| 10 | 100 | 16 |
| 12 | 144 | 27 |

Use 5 on a quiet machine with a large difference. Use 8 to 12 on a shared cluster, or when the
difference you are looking for is near 1.1x.

### "no clear difference" says the runs could not separate them

```text
no clear difference between join first and agg first (ratio 1.05x, p=0.310, 5 runs each)
```

It does not say they run equally fast. It says these runs could not tell them apart. Check, in
order:

- **The run time.** A run of a few hundred milliseconds is mostly fixed cost: scheduling, task
  launch, file listing. Time on data large enough that each run takes seconds.
- **The ratio.** Under 1.1x, no number of runs gives a verdict. The floor keeps a real but small
  difference from reading as a win.
- **The spread.** When one DataFrame's min and max straddle the other's median, more runs are the
  remedy.

### Timing reads cached data when the DataFrame was built on a cache

pysparring leaves the session's cache alone. A DataFrame built on a cached table is timed reading
the cache, and the report says so:

```text
df1, df2 read cached data, so those runs time the cache.
```

To time reading from storage, call `unpersist()` on the cached table before `compare_plans`.

### The result holds every plan, count, time and verdict by name

`compare_plans` returns a `PlanComparison`. Every dict in it is keyed by the names you gave.

| Attribute | Holds |
| --- | --- |
| `names` | the names in order. `reference` is the first |
| `plans` | each plan as `explain` printed it |
| `normalized_plans` | each plan with its ids stripped |
| `diffs` | each name after the first, mapped to its diff lines. An empty list means identical |
| `operator_counts` | each plan's operators, mapped to their counts |
| `operator_deltas()` | each operator whose count differs, mapped to its count in every plan |
| `timings_ms` | each DataFrame's measured run times. Empty when `runs` was 0 |
| `verdicts` | each name after the first, mapped to a `SpeedVerdict`. Empty when `runs` was 0 |

A `SpeedVerdict` holds `median_ms`, `reference_median_ms`, `ratio`, `p_value`, `runs`, and
`faster`: the faster name, or None. Its `sentence` is the line the report prints.

```python
result = compare_plans(old, new, names=['old', 'new'], runs=8)
verdict = result.verdicts['new']
if verdict.faster == 'new':
    print(f'new saves {verdict.reference_median_ms - verdict.median_ms:.0f} ms a run')
```

## `compare_outputs` says which columns differ, and keys decide how much it can say

```python
compare_outputs(a, b, keys=None, names=('df1', 'df2'), rel_tol=1e-9, abs_tol=0.0, ignore_columns=(), sample_rows=5)
-> OutputComparison
```

### Keys are the columns that identify one row of the output

Pass the columns that make a row unique, the output's grain: `keys='segment'` for totals per
segment, or `keys=['customer_id', 'order_date']` for a daily table per customer.

With keys, rows pair by a full outer join on the keys, and every column reports its mismatches over
the paired rows. The join is null-safe, so a null key pairs with a null key.

Without keys, pysparring says whether the outputs match, and how many rows have no partner. It
cannot name the column that changed, because it does not know which rows should pair. Use it when
the output has no key, and pass keys whenever it has one.

### A keyed report names each fault a refactor introduced

Here `old` is a per-customer table, and `new` is a rewrite of it with six faults:

- it casts `total` from `double` to `decimal(18,2)`;
- it adds a `discount_pct` column;
- it drops customer 42;
- it repeats the rows of customers 1 to 3;
- it left-joins a customers table missing every seventh customer, which nulls their `segment`;
- it miscounts `n` for customer 5, and appends a space to customer 10's `segment`.

```python
compare_outputs(old, new, keys='customer_id', names=('old', 'new'))
```

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
  total: 3 rows differ by at most 0.00351, within tolerance

Differing values, 11 of 2,282
┏━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ customer_id ┃ column  ┃ old                ┃ new       ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ 11          │ total   │ 7126.6300748965505 │ 7126.63   │
│ 15          │ total   │ 6558.033418986644  │ 6558.03   │
│ 30          │ total   │ 8567.083497832318  │ 8567.08   │
│ 50          │ total   │ 6883.914365415216  │ 6883.91   │
│ 55          │ total   │ 7250.647276370248  │ 7250.65   │
│ 5           │ n       │ 110                │ 111       │
│ 10          │ segment │ 'retail'           │ 'retail ' │
│ 245         │ segment │ 'retail'           │ null      │
│ 280         │ segment │ 'retail'           │ null      │
│ 301         │ segment │ 'smb'              │ null      │
│ 315         │ segment │ 'retail'           │ null      │
└─────────────┴─────────┴────────────────────┴───────────┘

Only in old, 1 of 1
┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━┳━━━━━━━━━━━━━━┓
┃ customer_id ┃ total              ┃ n   ┃ segment      ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━╇━━━━━━━━━━━━━━┩
│ 42          │ 6395.4533979025455 │ 122 │ 'enterprise' │
└─────────────┴────────────────────┴─────┴──────────────┘

Only in new, 3 of 3
┏━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ customer_id ┃ total      ┃ n      ┃ segment      ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━┩
│ 2           │ 7151.95    │ 140    │ 'enterprise' │
│ 3           │ 7277.79    │ 128    │ 'gov'        │
│ 1           │ 5934930.17 │ 107909 │ 'smb'        │
└─────────────┴────────────┴────────┴──────────────┘
Row order, column order and nullability are ignored.
```

Each section answers one question:

- **Schema** lists the columns on one side only, and each type change. A column whose type changed
  is still compared, after casting both sides to one type: `bigint` for two integer types,
  `double` for two other numeric types, and `string` otherwise.
- **Rows** gives each side's count, how many rows paired by key, and how many are on one side only.
  A key appearing more than once on either side is counted, because it usually means a join fanned
  out.
- **Columns on matched rows** gives each column's mismatches, most first. A null against a value is
  a mismatch, and its own columns count those. `max abs diff` is the largest difference among a
  numeric column's mismatches. Here 0.005 is rounding to cents, and the 284 nulls are the missing
  customers. A float difference inside the tolerance is never a mismatch, and the dim line under
  the table counts those.
- **Differing values** shows up to `sample_rows` per mismatched column. Values that changed come
  before nulls, which the table already counted. Strings keep their quotes, so `'retail '` shows
  its trailing space.
- **Only in** shows up to `sample_rows` rows found on one side only. Customer 1, the busiest, is one
  of the repeated rows.

### The tolerance lets float sums in another order match

Two transforms that add the same doubles in a different order produce totals that differ in the
last digits. So a float or double matches when:

```text
|a - b| <= abs_tol + rel_tol * |b|
```

That is numpy's `isclose` rule, with `b` as the reference for the relative part. The defaults,
`rel_tol=1e-9` and `abs_tol=0.0`, accept reordered sums with a wide margin. In the first
comparison above, totals of a few million moved by at most 3.35e-08, a relative difference near
1e-14.

- **Raise `abs_tol`** when values near zero should match. Against `b = 0`, a relative tolerance
  allows nothing.
- **Raise it to accept rounding you intended.** In the report above, rounding to cents moved
  `total` by up to 0.005, and `abs_tol=0.01` would accept it.
- **Leave it alone otherwise.** A tolerance wide enough to hide a difference you do not understand
  hides a bug.

Only floats and doubles take the tolerance, along with two numeric types compared as double.
Integers, decimals of one type, strings, dates and booleans compare exactly. Structs, arrays and
maps compare as their JSON text, so one changed field marks the whole value.

### Without keys, rows pair by their other columns, and statistics stand in for columns

```python
compare_outputs(join_first, agg_first)
```

Rows pair in three steps:

1. Every row with an exact twin on the other side matches. `exceptAll` both ways finds the rest.
2. The rest pair by their columns other than floats, null-safe, in order of their floats.
3. A pair whose floats all fall within the tolerance matches. The report counts those rows.

```text
df1 vs df2: outputs match, compared without keys

Schema
  same columns and types

Rows
  df1 5   df2 5
  only in df1 0   only in df2 0
  5 rows matched only within the tolerance
Row order, column order and nullability are ignored. Rows pair by their other columns, and floats and doubles match within the tolerance. Pass keys= to see which columns differ.
```

When the outputs differ, a table of column statistics stands in for the per-column report. It
lists each null count, approximate distinct count, min, max and sum that differs. A float's min,
max and sum compare under the tolerance.

One changed column breaks every row's pairing. On the broken refactor above, without keys:

```text
Rows
  old 2,000   new 2,002
  only in old 1,997   only in new 1,999
  3 rows matched only within the tolerance

Column statistics that differ
┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ column      ┃ statistic       ┃ old               ┃ new         ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ customer_id │ approx distinct │ 2023              │ 2021        │
│ customer_id │ sum             │ 2001000           │ 2000964     │
│ total       │ approx distinct │ 1912              │ 2049        │
│ total       │ min             │ 4822.272745286938 │ 4822.27     │
│ total       │ sum             │ 19785780.93574333 │ 25728745.26 │
│ n           │ sum             │ 359860            │ 467916      │
│ segment     │ nulls           │ 0                 │ 284         │
│ segment     │ approx distinct │ 5                 │ 6           │
└─────────────┴─────────────────┴───────────────────┴─────────────┘
```

Rounding moved nearly every `total` past the tolerance, so nearly every row is on one side only.
The keyed report named the cause in one row. When an unkeyed report shows thousands of one-sided
rows, look for a key.

The distinct counts are approximate, from `approx_count_distinct`, so they can differ by a few
percent from the true count. Two equal outputs still give equal estimates.

### A duplicated key is counted, and its rows pair by value

When a key appears more than once, the report counts those keys for each side. Rows sharing a key
pair in order of their values. So the same duplicates on both sides pair up and match, and each
extra copy on one side is a one-sided row.

### `ignore_columns` leaves out columns that should differ

```python
compare_outputs(old, new, keys='order_id', ignore_columns=['loaded_at', 'run_id'])
```

An ignored column is dropped from both sides before anything else, so it cannot be a key. Ignoring
every shared column raises a `ValueError`.

### Each side's plan runs more than once, so cache an expensive input

`compare_outputs` runs each DataFrame's plan two or three times: to count duplicate keys or take
statistics, then for the join it caches and reads several times. It unpersists its own cache when
it finishes, and never touches yours.

For a transform that takes minutes, cache or write each side first:

```python
old = old_pipeline(spark).cache()
new = new_pipeline(spark).cache()
compare_outputs(old, new, keys='customer_id').assert_matches()
old.unpersist()
new.unpersist()
```

Do not time these cached DataFrames with `compare_plans`, which would time the cache. Build the
DataFrames again for timing.

### The result holds every count and sample

`compare_outputs` returns an `OutputComparison`. A field ending in `_a` describes the first
DataFrame, and one ending in `_b` the second. A field the comparison did not measure is None: a
keyed comparison takes no column statistics, and one without keys has no matched rows.

| Attribute | Holds |
| --- | --- |
| `matches` | True when the schemas are the same, every row has a partner, and no column mismatches |
| `assert_matches()` | raises an `AssertionError` carrying the report, unless the outputs match |
| `names`, `keys` | as given, with `keys` empty when compared without keys |
| `schema` | `columns_only_in_a`, `columns_only_in_b`, `type_changes`, and `same` |
| `rows_a`, `rows_b` | each side's row count |
| `only_in_a_rows`, `only_in_b_rows` | rows left without a partner |
| `matched_rows` | rows paired by key. None without keys |
| `duplicate_keys_a`, `duplicate_keys_b` | keys appearing more than once. None without keys |
| `column_differences` | per column: `mismatches`, `null_only_in_a`, `null_only_in_b`, `max_abs_diff`, `within_tolerance`. None without keys |
| `differing_values` | sampled `key`, `column`, `a`, `b`. None without keys |
| `statistic_differences` | `column`, `statistic`, `a`, `b`. None with keys |
| `rows_within_tolerance` | rows that paired only within the tolerance. None with keys |
| `only_in_a_sample`, `only_in_b_sample` | sampled rows, as tuples in `sample_columns` order |

Sampled values are as compared: cast to the shared type, or as JSON text.

```python
result = compare_outputs(old, new, keys='customer_id', names=('old', 'new'))
changed = [d.column for d in result.column_differences if d.mismatches]
```

## `assert_matches` turns a comparison into a test

`assert_matches()` raises an `AssertionError` whose message is the whole report as plain text. A
failing test then names the columns that changed:

```python
import pytest
from pyspark.sql import SparkSession

from pysparring import compare_outputs


@pytest.fixture(scope='session')
def spark():
    session = SparkSession.builder.master('local[2]').config('spark.sql.shuffle.partitions', '2').getOrCreate()
    yield session
    session.stop()


def test_the_rewrite_keeps_every_total(spark):
    orders = spark.read.parquet('tests/data/orders')
    customers = spark.read.parquet('tests/data/customers')
    compare_outputs(
        totals_per_segment(orders, customers),
        totals_per_segment_rewritten(orders, customers),
        keys='segment',
        names=('current', 'rewrite'),
    ).assert_matches()
```

pytest captures the printed report and shows it only when the test fails. `pytest -s` shows it for
passing tests too. A small `spark.sql.shuffle.partitions` keeps a local test from splitting every
shuffle into the default 200 partitions.

Keep timing out of tests. A verdict depends on the machine, so it belongs in a notebook or a
script, never in an assertion.

## A refactor goes outputs first, then plans, then time

1. **Build both versions over the same input.** A sample of real tables works, built as in the
   next section.
2. **Compare the outputs with keys at the output's grain** until they match. Widen the tolerance
   only for a difference you can explain.
3. **Compare the plans without `runs`.** The diff and the operator table show what the rewrite
   changed: a shuffle removed, a join strategy switched, a filter pushed into the scan.
4. **Time them on data large enough that a run takes seconds.** Start with `runs=5`, and go to 8
   or more when the report reads "no clear difference" with a ratio above 1.1x.
5. **Pin the output in a test** with `assert_matches`, so the next change keeps it.

## A sample of real tables keeps the same keys in every table

Production tables are too large to compare repeatedly, and a sample has to keep its joins.

### Sample keys, not rows

`.sample(0.01)` keeps each row on its own coin flip. A joined row survives only when both of its
sides did, so 1% of each table keeps about 0.01% of the join. A filter on a hash of the join key
keeps the same keys in every table:

```python
import pyspark.sql.functions as sf


def in_sample(key, percent):
    """True for the same keys in every table: `percent` keys in every 100."""
    return sf.pmod(sf.xxhash64(key), sf.lit(100)) < percent


customers = spark.read.parquet('s3://my-bucket/customers').where(in_sample('customer_id', 1))
orders = spark.read.parquet('s3://my-bucket/orders').where(in_sample('customer_id', 1))
```

On 2,000,000 orders over 100,000 customers, `.sample(0.01)` on both tables kept about 160 joined
rows. The key hash kept 15,077. The hash depends on the key alone, so the same keys come back on
every run.

- Finer than 1%: `sf.pmod(sf.xxhash64(key), sf.lit(10000)) < 25` keeps 0.25%.
- A key of several columns hashes them together: `sf.xxhash64('region', 'customer_id')`.

### `pmod`, never `%`

`xxhash64` returns a signed number, and `%` keeps its sign. Half the keys give a remainder from -99
to 0, and `% 100 < 1` keeps every one of them: 50.3% of the customers instead of 1%.

### Hash the key at one type in every table

`xxhash64` hashes the value's bytes, so an `int` 42, a `bigint` 42 and a `string` '42' hash three
ways. An `int` key in one table and a `bigint` key in the other sample different customers. Cast
the key to one type inside the hash:

```python
in_sample(sf.col('customer_id').cast('bigint'), 1)
```

### A table without the key follows its parent by a semi join

`order_items` holds `order_id`, not `customer_id`. Sample orders by customer, then keep the items
whose order survived:

```python
items = spark.read.parquet('s3://my-bucket/order_items').join(orders.select('order_id'), 'order_id', 'left_semi')
```

A `left_semi` join returns only the left table's columns, and never repeats a left row. Further down
a chain, each table semi-joins to the one above it.

### A hot key is kept by hand, and its rows sampled by a second column

A 1% hash drops any one key 99 times in 100. So the skewed key that makes production slow is
usually missing from the sample, and so is the long task it causes. Keep the hot keys, and sample
their rows by another column:

```python
# Find them with orders.groupBy('customer_id').count().orderBy(sf.desc('count')).limit(10)
hot_ids = [1]
is_hot = sf.col('customer_id').isin(hot_ids)

customers = customers_full.where(in_sample('customer_id', 1) | is_hot)
orders = orders_full.where(in_sample('customer_id', 1) | (is_hot & in_sample('order_id', 1)))
```

### Filter partitions before hashing

Spark pushes only plain column comparisons down to Parquet, so the hash filter reads every row of
every file it scans. Filter on partition columns first, so the scan reads only those partitions:

```python
orders = spark.read.parquet('s3://my-bucket/orders').where(sf.col('order_date') >= '2026-10-01').where(in_sample('customer_id', 1))
```

### A sample can broadcast a join that production sort-merges

Spark sizes a table read from files by its file size. A sample written out and read back falls under
`spark.sql.autoBroadcastJoinThreshold`, 10 MB by default, so its plan broadcasts. A sample filtered
in the same job keeps the full table's size estimate, so its plan sort-merges, and adaptive
execution switches it to a broadcast at runtime. Either way, the sample runs a different join than
production. To see production's join on a sample, turn broadcasts off:

```python
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
```

### Timings on a sample rank the plans at that size

A plan's fixed cost is a larger share of a small run. A rewrite that saves a shuffle can tie on a
sample and win on the full table, as `agg_first` did between 400,000 and 2,000,000 orders. Time on
the largest sample whose runs you can afford.

## The report prints in color where it can, and as plain text where it cannot

- **In a terminal or a Jupyter notebook**, the report prints in color.
- **In a log, a file, or a remote session that returns text**, it prints without color. rich prints
  color only to a terminal or a Jupyter kernel. `FORCE_COLOR=1` forces it, and `NO_COLOR=1` turns
  it off.
- **`print(result)` and `str(result)`** give the report as plain text, 120 columns wide, for a log
  or a file. `rich.print(result)` prints it again in color.
- **`repr(result)` is one line**, such as
  `<PlanComparison join first → agg first → broadcast hint: 6 operator counts differ>`.
- **Long lines are never broken by rich.** The terminal wraps them, so a wrapped diff line keeps its
  `+` or `-`.

In a notebook, a call on a cell's last line prints the report, then the notebook shows the returned
object's one-line repr under it. Assign the result, `result = compare_plans(...)`, or end the line
with `;` to leave the repr out.

## Troubleshooting: each error names its cause

| You see | Cause | Fix |
| --- | --- | --- |
| `ValueError: runs=4 is too few to show a difference. Use runs=5 or more, or runs=0 to skip timing.` | the exact test cannot reach p < 0.01 below 5 runs each | `runs=5` or more, or `runs=0` |
| `ValueError: compare_plans needs at least two DataFrames` | one DataFrame passed | pass two or more |
| `ValueError: names must give each of the 2 DataFrames its own name, ...` | `names` has the wrong length or a repeat | one distinct name per DataFrame |
| `ValueError: names must be two different names, ...` | `compare_outputs` got more or fewer than two names, or one twice | two distinct names |
| `ValueError: key 'nope' is not a column of df1 or df2` | a key is missing on a side, or is in `ignore_columns` | check the spelling and the ignored columns |
| `ValueError: df1 and df2 share no columns left to compare` | no shared columns once `ignore_columns` is applied | ignore fewer columns, or rename to match |
| `RuntimeError: explain(mode="formatted") on old printed no physical plan to stdout, ...` | something replaced `DataFrame.explain` or sent its output somewhere other than Python's stdout | the message quotes what `explain` did print; call `df.explain(mode='formatted')` yourself to see it |
| `ERROR: Package 'pysparring' requires a different Python: 3.10.12 not in '>=3.11'` | Python 3.10, as in Glue 4.0 and the `apache/spark` images | Python 3.11 or later, or [the apache/spark recipe](#the-apachespark-images-need-python-311-installed-first) |
| `ModuleNotFoundError: No module named 'pyspark'` | pysparring never installs pyspark | install the pyspark that matches your cluster |
| Thousands of rows "only in" each side, without keys | one column changed on most rows, so they cannot pair | pass `keys` to see the column |
| `total: 5 rows differ by at most 3.35e-08, within tolerance` | float sums in another order | nothing; those rows match |
| "no clear difference" though one median is lower | too few runs, too little data, or a ratio under 1.1x | see [no clear difference](#no-clear-difference-says-the-runs-could-not-separate-them) |
| No color | stdout is neither a terminal nor a Jupyter kernel | `FORCE_COLOR=1`, or read it as plain text |

## What pysparring does not do

pysparring is the two functions. Each of these is left out on purpose, and waits until using the
two shows the need.

- **No command line, experiment files or saved reports.** It is a library. `str(result)` gives the
  report as text to save.
- **No cluster management.** It runs on whatever session built the DataFrames.
- **No final adaptive plan.** It prints the plan before running. The Spark UI's SQL tab shows the
  plan that ran.
- **No runtime metrics**, such as shuffle bytes or spill. The Spark UI's stage pages show them.
- **No cost estimate**, and no winner picked from a plan. Spark's `explain` carries no total cost.
- **No transform under several confs in one call.** Both functions take DataFrames, and every
  DataFrame in a call plans under the session's conf at that moment. Call `compare_plans` once per
  conf to see each.
- **No guessed keys.** You name them, or the comparison runs without them.
- **No diffs inside struct, array and map columns.** Each compares as its JSON text, whole.
- **No HTML report.**

[how-each-comparison-decides.md](how-each-comparison-decides.md) explains why each comparison is
built the way it is.
