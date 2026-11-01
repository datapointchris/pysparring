# pysparring

A Python library of two functions over PySpark DataFrames. `compare_plans` diffs physical plans and
times them, and `compare_outputs` says which columns of two outputs differ and how. `docs/README.md`
indexes the user guide and why each is shaped the way it is.

## The library is these two functions, and its scope is closed

Not in scope: experiment files, a CLI, clusters and backends, S3 staging, cost, generated scenario
datasets, heuristic rules, saved reports, runtime metrics, the final adaptive plan, one transform
under several confs, guessing keys, diffs inside struct and array columns, and HTML output. A
feature is not added because it looks useful. It waits until using the two functions shows the
need for it.

```text
src/pysparring/   plans.py, outputs.py, significance.py and rendering.py
tests/            pure-Python tests over the fixtures
fixtures/         real Spark output the tests read, and the docker job that captures it
docs/             the user guide, and why the design is what it is
```

- **pyspark is never declared.** The user's environment supplies the one that matches its cluster:
  local `pyspark`, `pyspark[connect]` or `pyspark-client`. Declaring one installs a second over it.
  So every pyspark import sits inside a function or under `TYPE_CHECKING`, and the tests run
  without it.
- **rich is the one dependency.** It prints color in a terminal or notebook, and plain text in a log
  or an assertion message.
- **The code runs on Python 3.11**, the Python an AWS Glue 5.0 or 5.1 driver runs.
  `.python-version` pins the project there, so a later feature fails locally and in CI.
- **A test reads Spark's behavior from a fixture**, never from a string written to look like Spark's
  output. Behavior that needs a live session is verified by running it in the Spark images, not by
  a committed test. The `apache/spark` images run Python 3.10, so a run there installs Ubuntu's
  `python3.11` first and points `PYSPARK_PYTHON` at it.

## Commands

```bash
task setup      # uv sync
task test       # pytest
task lint       # ruff and mypy, writing nothing
task build      # wheel and sdist into dist/

fixtures/regenerate.sh [VERSION...]   # rewrite the fixtures from Spark runs in docker
```

The tag is the version. A release is cut from `main` by conventional commits, and nothing is
committed back.
