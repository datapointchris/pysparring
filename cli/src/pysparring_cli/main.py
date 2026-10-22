import importlib.metadata
import json
from typing import Annotated

import typer
from pysparring.schemas import schema_names
from pysparring.schemas import schema_text

app = typer.Typer(no_args_is_help=True)
schema_app = typer.Typer(no_args_is_help=True, help='The JSON Schemas pysparring ships, so a program can check a document it reads.')
app.add_typer(schema_app, name='schema')


def installed_commit() -> str | None:
    """The commit a git install was built from, which uv records in direct_url.json."""
    direct_url = importlib.metadata.distribution('pysparring-cli').read_text('direct_url.json')
    if not direct_url:
        return None
    return json.loads(direct_url).get('vcs_info', {}).get('commit_id')


def version_callback(asked: bool) -> None:
    if not asked:
        return
    commit = installed_commit()
    typer.echo(f'pysparring {importlib.metadata.version("pysparring-cli")}{f" @ {commit[:8]}" if commit else ""}')
    raise typer.Exit()


@app.callback()
def root(
    version: Annotated[
        bool | None,
        typer.Option('--version', callback=version_callback, is_eager=True, help='Show the installed version and exit.'),
    ] = None,
) -> None:
    """Run PySpark transform candidates against each other, and compare their outputs, plans and timing."""


@schema_app.command('show', no_args_is_help=True)
def schema_show(
    name: Annotated[str, typer.Argument(metavar='NAME', help=f'A schema name: {", ".join(schema_names())}.', show_default=False)],
) -> None:
    """Print one JSON Schema, draft 2020-12."""
    if name not in schema_names():
        raise typer.BadParameter(f'{name!r} is not a schema. The schemas are {", ".join(schema_names())}.', param_hint='NAME')
    typer.echo(schema_text(name), nl=False)
