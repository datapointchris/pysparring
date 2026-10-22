from pysparring.schemas import schema_text
from pysparring_cli.main import app
from typer.testing import CliRunner


def test_schema_show_prints_the_schema_the_library_ships() -> None:
    runner = CliRunner()
    shown = runner.invoke(app, ['schema', 'show', 'experiment'])
    assert (shown.exit_code, shown.output) == (0, schema_text('experiment'))
    assert runner.invoke(app, ['schema', 'show', 'experiments']).exit_code == 2
