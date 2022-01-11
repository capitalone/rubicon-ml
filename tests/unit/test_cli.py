import warnings
from unittest.mock import patch

import click
from click.testing import CliRunner

from rubicon_ml.cli import cli


def mock_click_output(**server_args):
    click.echo("Running the mock server")


@patch("rubicon_ml.viz.dashboard.Dashboard.serve")
@patch("rubicon_ml.client.rubicon.Rubicon.projects")
@patch("rubicon_ml.client.rubicon.Rubicon.get_project")
def test_cli(mock_get_project, mock_projects, mock_run_server, project_client):
    mock_get_project.return_value = project_client
    mock_projects.return_value = [project_client]
    mock_run_server.side_effect = mock_click_output

    with warnings.catch_warnings(record=True) as caught_warnings:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["ui", "--root-dir", "/path/to/root", "--page-size", 100],
        )

        assert result.exit_code == 0
        assert "Running the mock server" in result.output

        assert len(caught_warnings) == 2
        assert "`--page-size` option will be deprecated" in str(caught_warnings[0])
        assert "`--project-name` will be a required option" in str(caught_warnings[1])
