# How each comparison decides

## A plan explains a difference and never decides one

Spark's `explain` carries no total cost. `explain("cost")` gives each node a `sizeInBytes` estimate,
and without table statistics that estimate is an upper bound, not a cost.

Plan shape does not pick the faster transform either. In the fixtures, with broadcast joins off,
`join_then_aggregate` and `aggregate_then_join` both plan three `Exchange`s and one `SortMergeJoin`.
`aggregate_then_join` adds two `HashAggregate`s, and `timings-interleaved.json` shows it running in
about half the time. Its first shuffle carries partial totals per customer instead of every order,
and the plan prints no size for either. So `compare_plans` reports operator counts and a diff, and
only measured time names a faster DataFrame.

*Rejected:* a weighted score over plan, time and output. Its weights would be a judgment the tool
cannot make.

## Ids are stripped by rewrites anchored to the syntax around them

Spark renumbers expression ids (`amount#10`), exchange ids (`[plan_id=639]`), codegen stages and
query stages on every build. Formatted mode also numbers every node, so one added node renumbers all
those printed after it, along with every `Reuses operator id` pointing at them. Without stripping
these, a diff of two builds of one transform marks most lines.

Each rewrite names the syntax that carries the id. Rewriting every `(\d+)` would turn `count(1)`
into `count(N)` and change an expression. The fixtures hold one transform built twice, and a test
pins that the two differ as printed and match once stripped. An operator is counted by its detail
header without a trailing bracket, so `ReusedExchange [Reuses operator id: N]` counts as
`ReusedExchange`.

## Timing uses a no-op write, a warm-up and shuffled rounds, because each fixed a measured failure

- **The `noop` sink** runs the whole plan and writes nothing. `count()` lets the optimizer prune
  columns, and `collect()` adds the transfer to the driver.
- **Three discarded warm-up rounds.** The first runs pay JIT compilation and file listing. At 5
  runs each, one slow sample overlapping the other DataFrame's lifts the smallest p-value from
  0.0079 to 0.016, which hides a real difference. Each arm's first measured run in the fixtures,
  against the median of its other runs:

  | warm-up rounds | first measured run | the 3.5.9 broadcast-off pair, 2x apart, at 5 runs |
  | --- | --- | --- |
  | 1 | 1.4 to 3.4 times | not recorded |
  | 2 | 1.0 to 2.9 times | no clear difference, p = 0.095 |
  | 3 | 1.0 to 1.5 times | faster, p = 0.0079 |

  `tests/test_plans.py` checks the verdict on every release's fixture.
- **A slow round after the warm-up still costs power**, because interleaving lands it on every
  DataFrame at once. A test paired by round would cancel it, but the exact signed-rank test needs 8
  runs to reach p < 0.01.
- **Rounds run every DataFrame once, in a seeded shuffled order.** In a block of runs per DataFrame,
  drift in the machine lands on one DataFrame and reads as a win. A round puts every DataFrame
  through the same moment.
- **The session's cache is left alone.** A DataFrame built on a cached table is timed reading the
  cache, and the report names it. Clearing the cache would empty what the caller cached, and time
  a plan other than the one printed.

## A faster DataFrame needs an exact test and a large enough ratio

The test is a two-sided Mann-Whitney U, computed exactly from the U distribution, so it needs no
scipy. A DataFrame is faster when p < 0.01 and the slower median is at least 1.1 times the faster,
which is asv's default factor. The ratio floor keeps a real but trivial difference from reading as a
win.

The smallest p-value the test can reach with n runs each is `2 / C(2n, n)`: 0.0079 at 5 runs and
0.029 at 4. So `runs` below 5 can never reach 0.01, and `compare_plans` refuses it. The floor is
computed from the significance level, so a change to one moves the other.

*Rejected:* a bootstrap interval on the ratio of medians. With 5 runs it resamples only 5 values, so
its interval is unreliable at the sizes this tool runs.

## Outputs compare by key with a tolerance, because exact equality fails every aggregation over doubles

Two transforms that sum doubles in a different order produce totals that differ in the last digits,
and `exceptAll` both ways then calls every such row different. So a float or double matches when
`|a - b| <= abs_tol + rel_tol * |b|`, numpy's `isclose` convention, and a difference inside that is
reported as within tolerance. One function in `outputs.py` builds that test for both modes.

With keys, rows pair by a null-safe full outer join. Rows sharing a key pair up in order of their
values, which is datacompy's technique. Each column then reports its own mismatches, so the report
names the column that changed.

Without keys, `exceptAll` both ways first removes every row with an exact twin. The rows left pair
by a null-safe join on their other columns, in order of their floats, and a pair whose floats all
fall within the tolerance matches. Column statistics stand in for the per-column report, and a
float's min, max and sum compare under the same tolerance.

*Rejected:*

- pyspark's `assertDataFrameEqual` and chispa. Both collect both DataFrames to the driver, and both
  mark a differing row without saying which column differs.
- datacompy as a dependency. Its 1.1.0 metadata requires pandas, numpy and `polars[pandas]<1.45`,
  and its `spark` extra requires `pyspark[connect]`, a second pyspark.
- Rounding floats to a fixed number of places before `exceptAll`. Totals near 1e9 differ past the
  sixth decimal, and two values either side of a rounding boundary differ after rounding.

## What the two functions leave out, and what each would need

- **The adaptive plan Spark ran.** After a no-op write, a classic session does not keep the final
  plan on the DataFrame, and a Connect session needs the REST API or the event log to reach it.
- **Shuffle bytes and other runtime metrics.** They need the Spark UI's REST API, which a managed
  Connect session may not expose.
- **One transform under several confs.** That needs the transform as a function, to rebuild it under
  each conf, where both functions take DataFrames.
