# pysparring

Runs PySpark transform candidates against each other across datasets, Spark configurations and
clusters, and compares their outputs, physical plans and timing.

## Two distributions share one uv workspace

```text
pyproject.toml  the workspace, the release config, and every Python tool's config for both members
library/        pysparring: imported as `pysparring`, Python >=3.11, no scripts
cli/            pysparring-cli: imported as `pysparring_cli`, Python >=3.13, the `pysparring` command
```

- **The dependency runs one way: the CLI imports the library, never the reverse.** Typer and rich
  are imported only under `cli/`. A test suite imports the library through its pytest plugin, so
  the library must never pull them in.
- **pyspark is never declared, by either distribution.** The project's environment supplies the
  pyspark that matches its cluster: local `pyspark`, `pyspark[connect]` for a Glue 5.1 session, or
  `pyspark-client` for Glue 6.0. Declaring one installs a second pyspark over it.
- **The harness imports only pyspark and the standard library.** It ships to a remote driver as one
  file, so nothing else from this repo installs on a cluster.
- **A library dependency arrives in the change that first imports it.** It must install on CPython
  3.11 from a wheel. An optional engine is imported lazily and declared nowhere, and its
  missing-import error names the distribution to install.
- **The member `pyproject.toml` files carry packaging only.** ruff, mypy and pytest read their
  config from the root file.

## Library code must run on Python 3.11

A Glue 5.0 or 5.1 driver runs Python 3.11, and the harness runs there. So nothing under `library/`
may use what arrived later: `type` statements, `def f[T]` generics, `typing.override`,
`itertools.batched`, or an f-string reusing its own quote character.

`uv sync` resolves the workspace at 3.13, so nothing local catches these by default. The
`library-on-python-3.11.yml` workflow does, by compiling, type-checking and testing the library on
3.11.

## The tag is the version of both distributions

Both read their version from the tag through uv-dynamic-versioning, and the CLI requires the library
at exactly its own version. A release is cut from `main` by conventional commits, and nothing is
committed back.

## Commands

```bash
task setup      # uv sync --all-packages
task test       # pytest
task lint       # ruff and mypy, writing nothing
task build      # wheels and sdists into dist/
```
