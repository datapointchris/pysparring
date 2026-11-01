# Spark fixtures

Real Spark output the tests read. `regenerate.sh` captures it from Spark runs in docker.

## One job captures each Spark release

`job/capture_plans.py` runs under `spark-submit` on `local[4]` in each `apache/spark:<version>-python3`
image. Its data is an orders table with one hot customer, joined to a customers table in five
segments. Both are written to Parquet and read back, so the scans show pushdown. Two transforms
compute the same totals per segment:

- `join_then_aggregate` joins, then aggregates per segment.
- `aggregate_then_join` aggregates per customer, joins, then aggregates per segment.

Each runs with broadcast joins allowed (`broadcast_on`) and turned off (`broadcast_off`).

| File | Holds |
| --- | --- |
| `spark-<version>/explain-<transform>-<config>.txt` | the plan as `explain("formatted")` prints it before running |
| `spark-<version>/explain-join_then_aggregate-broadcast_off-rebuilt.txt` | the same transform built a second time, so its ids differ and its plan does not |
| `spark-<version>/timings-interleaved.json` | milliseconds per `transform:config` arm, on `compare_plans`' schedule: rounds in a shuffled order, two discarded warm-up rounds, then 6 measured |

These directories hold Spark's bytes. Formatted mode ends a scan's header with a space, and every
plan ends in a blank line. The repo's hooks would rewrite both, so they skip `fixtures/spark-*/`.

## Regenerating rewrites every timing and id

```bash
fixtures/regenerate.sh          # every release
fixtures/regenerate.sh 4.2.0    # only this one
```

The timings are what the regenerating machine measured, and Spark assigns its ids afresh each run.
So a test pinning one of those values changes in the commit that regenerates.
