from pysparring.schemas import schema_names
from pysparring.schemas import schema_text
from pysparring_cli.main import app
from typer.testing import CliRunner


def test_schemas_list_and_show_print_the_schemas_the_library_ships() -> None:
    runner = CliRunner()
    listed = runner.invoke(app, ['schemas', 'list'])
    assert (listed.exit_code, listed.output.split()) == (0, list(schema_names()))
    shown = runner.invoke(app, ['schemas', 'show', 'experiment'])
    assert (shown.exit_code, shown.output) == (0, schema_text('experiment'))
    assert runner.invoke(app, ['schemas', 'show', 'experiments']).exit_code == 2
