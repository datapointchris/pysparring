# pysparring

Run PySpark transform candidates against each other across datasets, Spark configurations and
clusters, and compare their outputs, physical plans and timing.

This is the library. The `pysparring` command is the separate `pysparring-cli` distribution, so a
project importing the library installs none of the CLI's dependencies.

```bash
uv add --dev --index https://pypi.ichrisbirch.com/simple/ pysparring
```

Every release is published to that package index, and nothing else is. PyPI does not carry
pysparring, so a package of that name there is someone else's. The library needs Python 3.11 or
later. It does not install pyspark: the project's environment supplies the one matching its
cluster.

Source, documentation and issues: <https://github.com/datapointchris/pysparring>.
