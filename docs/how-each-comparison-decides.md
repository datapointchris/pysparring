# How each comparison decides

## A plan explains a difference and never decides one

Spark's `explain` carries no total cost. `explain("cost")` gives each node a `sizeInBytes` estimate,
and without table statistics those multiply upper bounds: one inner join was estimated at 402.5 GiB
over 28.2 MiB of input files.

Plan shape does not pick the faster transform either. In the fixtures, with broadcast joins off,
`join_then_aggregate` and `aggregate_then_join` both plan three `Exchange`s and one `SortMergeJoin`.
`aggregate_then_join` adds two `HashAggregate`s, and it ran in about half the time. Its
shuffles moved fewer bytes, a number that exists only after the query runs. So `compare_plans`
reports operator counts and a diff, and only measured time names a faster DataFrame.

*Rejected:* a weighted score over plan, time and output. Its weights would be a judgment the tool
cannot make.

## Ids are stripped by rewrites anchored to the syntax around them

Spark renumbers expression ids (`amount#10`), exchange ids (`[plan_id=639]`), codegen stages and
query stages on every build. Formatted mode also numbers every node, so one added node renumbers all
those printed after it. Without stripping these, a diff of two builds of one transform marks most
lines.

Each rewrite names the syntax that carries the id. An earlier normalizer rewrote every `(\d+)`, which
turned `count(1)` into `count(N)` and changed an expression. The fixtures hold one transform built
twice, and a test pins that the two differ as printed and match once stripped.

## Timing uses a no-op write, a warm-up and shuffled rounds, because each fixed a measured failure

- **The `noop` sink** runs the whole plan and writes nothing. `count()` lets the optimizer prune
  columns, and `collect()` adds the transfer to the driver.
- **Two discarded warm-up rounds.** The first runs pay JIT compilation and file listing. With one
  warm-up round, every arm's first measured run on Spark 3.5.9 and 4.2.0 took 1.4 to 3.4 times the
  median of its later runs. With two, it took 0.9 to 1.8 times. At 5 runs each, one slow sample
  overlapping the other DataFrame's lifts the smallest p-value from 0.0079 to 0.016, which hides a
  real difference.
- **Short runs keep speeding up after the warm-up.** Quarter-second runs got faster through all six
  measured rounds, because the JIT was still compiling. Interleaving gives every DataFrame the same
  decline, so it widens the spread without favoring one. A test paired by round would cancel it.
  The exact signed-rank test needs 8 runs to reach p < 0.01, against 5 for Mann-Whitney.
- **Rounds run every DataFrame once, in a seeded shuffled order.** Timing each DataFrame in its own
  block of runs produced a fake 1.8x win from machine drift, and interleaving the same runs showed no
  significant difference.
- **The cache is cleared before each run**, so no DataFrame reads what another cached.

## A faster DataFrame needs an exact test and a large enough ratio

The test is a two-sided Mann-Whitney U, computed exactly from the U distribution, so it needs no
scipy. A DataFrame is faster when p < 0.01 and the slower median is at least 1.1 times the faster,
which is asv's default factor. The ratio floor keeps a real but trivial difference from reading as a
win.

The smallest p-value the test can reach with n runs each is `2 / C(2n, n)`: 0.0079 at 5 runs and
0.029 at 4. So `runs` below 5 can never reach 0.01, and `compare_plans` refuses it.

*Rejected:* a bootstrap interval on the ratio of medians. With 5 runs it resamples only 5 values, so
its interval is unreliable at the sizes this tool runs.

## Outputs compare by key with a tolerance, because exact equality fails every aggregation over doubles

Two transforms that sum doubles in a different order produce totals that differ in the last digits.
In research, `exceptAll` both ways called two equivalent aggregations 100% different, because their
totals moved by 8.87e-7. So a float or double matches when `|a - b| <= abs_tol + rel_tol * |b|`,
numpy's `isclose` convention, and a difference inside that is reported as within tolerance.

With keys, rows pair by a null-safe full outer join. Rows sharing a key pair up in order of their
values, which is datacompy's technique. Each column then reports its own mismatches, so the report
names the column that changed. Without keys, only whole rows can be compared. Floats and doubles are
rounded to 6 places first, which misses a difference that straddles a rounding boundary. Column
statistics stand in for the per-column report. A float's min, max and sum compare under the same
tolerance, because two equivalent aggregations put them apart in the 15th digit.

*Rejected:*

- pyspark's `assertDataFrameEqual` and chispa. Both collect both DataFrames to the driver, and both
  mark a differing row without saying which column differs.
- datacompy as a dependency. Its 1.1.0 always installed pandas, numpy and Polars, pinned
  `polars<1.45`, and its Spark extra installs a second pyspark.

## What v1 leaves out, and what each would need

- **The adaptive plan Spark ran.** After a no-op write, a classic session does not keep the final
  plan on the DataFrame, and a Connect session needs the REST API or the event log to reach it.
- **Shuffle bytes and other runtime metrics.** They need the Spark UI's REST API, which a managed
  Connect session may not expose.
- **One transform under several confs.** That needs the transform as a function, to rebuild it under
  each conf, where v1 takes DataFrames.
