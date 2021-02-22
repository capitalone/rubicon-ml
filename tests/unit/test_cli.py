from unittest.mock import patch

import click
from click.testing import CliRunner

from rubicon.cli import cli


def mock_click_output(**server_args):
    click.echo("Running the mock server")


@patch("rubicon.ui.dashboard.Dashboard.run_server")
@patch("rubicon.ui.dashboard.Dashboard.__init__")
def test_cli(mock_init, mock_run_server):
    mock_init.return_value = None
    mock_run_server.side_effect = mock_click_output

    runner = CliRunner()
    result = runner.invoke(cli, ["ui", "--root-dir", "/path/to/root"],)

    assert result.exit_code == 0
    assert "Running the mock server" in result.output
