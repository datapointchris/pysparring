# Spark fixtures

Real Spark output the tests read. `regenerate.sh` captures it from Spark runs in docker, then
`copy_committed_subset.py` keeps the files the tests read. The rest of a capture stays out of the
repo.

## Each capture runs a job from `job/` in a Spark image

`job/capture_plans.py` runs under `spark-submit` on `local[4]` in each `apache/spark:<version>-python3`
image. Its data is an orders table with one hot customer, joined to a customers table in five
segments. Both are written to Parquet and read back, so the scans show pushdown. Two transforms
compute the same totals per segment:

- `join_then_aggregate` joins, then aggregates per segment.
- `aggregate_then_join` aggregates per customer, joins, then aggregates per segment.

Each runs with broadcast joins allowed (`broadcast_on`) and turned off (`broadcast_off`).

`job/connect_probe.py` runs `join_then_aggregate` in a Spark Connect session.
`job/run-connect-probe.sh` starts the server in `apache/spark:4.2.0-python3`, and the client runs
in the same container.

## Each directory holds one capture's committed files

| File | Holds |
| --- | --- |
| `spark-<version>/explain-<transform>-<config>-<mode>-initial.txt` | the plan before execution, in `formatted` and `simple` mode |
| `spark-<version>/explain-<transform>-<config>-<mode>-final.txt` | the plan after `collect`, where AQE's final plan sits above the initial one |
| `spark-<version>/specimen-<shape>-formatted-initial.txt` | one plan per shape a heuristic rule looks for, planned with broadcast joins off |
| `spark-<version>/rest-sql-executions-excerpt.json` | the fewest executions from the REST API's `sql` endpoint, with node details and plan text, that between them show every metric form and operator in the run |
| `spark-<version>/timings-interleaved.json` | a noop write's seconds per `transform:config` arm, from rounds that run each arm once in shuffled order, the warm-up round dropped |
| `spark-connect-4.2.0/executionInfo-metrics.json` | the plan nodes `df.executionInfo` carries after `collect`, each metric a raw number with its type |
| `spark-connect-4.2.0/facts.json` | what the probe found: no `_jdf`, the node counts after `collect` and after a noop write, `observe`'s flows, and whether the REST API answers the client |

`job/list_static_confs.py` asks each image's Spark registry which conf keys `spark.conf.set`
refuses. The `static-conf-keys` capture runs it under each Spark release in `SPARK_RELEASES`, and
`copy_committed_subset.py` merges the lists into `library/src/pysparring/static_conf_keys.txt`. The library ships that
file, and refuses a conf overlay that sets one of its keys. `docs/experiment-model.md` says why the
list comes from the registry rather than `spark.conf.isModifiable`.

On 4.2.0 a Python UDF defaults to Arrow, and the image carries no pyarrow, so Spark falls back.
The UDF specimen shows `BatchEvalPython` on both versions.

These directories hold Spark's bytes. Formatted mode ends a scan's header with a space, and every
plan ends in a blank line. The repo's hooks would rewrite both, so they skip `fixtures/spark-*/`.

## Regenerating rewrites every timing and id

```bash
fixtures/regenerate.sh                                      # every capture
fixtures/regenerate.sh spark-4.2.0                          # only this one
CAPTURE_DIR=/var/tmp/spark-capture fixtures/regenerate.sh   # and keep the full capture there
```

The timings are what the regenerating machine measured, and Spark assigns its ids afresh each run.
So a test pinning one of those values changes in the commit that regenerates.

The full capture also holds what no test reads yet:

- every explain mode before execution, and the JSON plans;
- the normalized diffs between the transforms and between the configs;
- sequential timings and each transform's result rows;
- the REST API's stages, and the status store's final plan of a noop write;
- sparkMeasure's stage report, and a `facts.json` per classic session.

A test that needs one of them adds its file to `copy_committed_subset.py`.
