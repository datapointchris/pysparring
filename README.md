# pysparring

Put PySpark transform candidates in the ring together, and find out which one wins, on which data,
and why.

pysparring is built to run two or more transform functions over the same inputs, across datasets,
Spark configurations and clusters. For every combination it is meant to report whether the outputs
agree, how the physical plans differ, and which candidate ran faster, with the statistics to say
whether a timing difference is real.

Its inputs are meant to be Parquet files, DataFrames, or synthetic datasets generated from a
profile. A scenario such as heavy key skew or a share of malformed rows is meant to show whether the
winner changes when the data does.

## Status

Experimental, and so far `pysparring --version` is its whole command line. Each release's notes list
what it does.

## Install

pysparring installs into the project whose transforms it compares, so a run uses that project's
pyspark. It never installs pyspark itself.

```bash
uv add --dev --index https://pypi.ichrisbirch.com/simple/ pysparring-cli
uv run pysparring --version
```

Every release is published to that package index, and nothing else is. The `pysparring-cli`
distribution is the command and needs Python 3.13 or later. The `pysparring` library it pins needs
Python 3.11 or later, the Python an AWS Glue 5.0 or 5.1 driver runs.

## Develop

```bash
task setup      # install both distributions into .venv
task test
task lint
```

`library/` holds the `pysparring` library and `cli/` the `pysparring-cli` command, under one uv
workspace. Both release together from one tag.

## License

MIT
