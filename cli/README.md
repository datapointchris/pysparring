# pysparring-cli

The `pysparring` command: run PySpark transform candidates against each other, and compare their
outputs, physical plans and timing.

```bash
uv add --dev --index https://pypi.ichrisbirch.com/simple/ pysparring-cli
uv run pysparring --version     # prints the installed release
```

Every release is published to that package index, and nothing else is. The command needs Python
3.13 or later. It pins the `pysparring` library to its own release, so the two always match. It
installs into the project whose transforms it runs, so a run uses that project's pyspark.

Source, documentation and issues: <https://github.com/datapointchris/pysparring>.
