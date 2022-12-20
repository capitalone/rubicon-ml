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


def test_search_cli(project_client, control_env_vars):

    project = project_client
    NUM_EXPERIMENTS = 4
    QUERY = "$..experiment[*].metric"
    TEST_COLOR = "yellow"
    for _ in range(NUM_EXPERIMENTS):
        tags = ["a", "b", "c"]
        ex = project.log_experiment(tags=tags)

        for feature in ["f", "g", "h", "i"]:
            ex.log_feature(name=feature)

        for parameter in [("d", 100), ("e", 1000), ("f", 1000)]:
            name, value = parameter
            ex.log_parameter(name=name, value=value)

        for metric in ["j", "k"]:
            value = 1
            tags = ["l", "m", "n"]
            ex.log_metric(name=metric, value=value, tags=tags)

        ex.log_artifact(name="o", data_bytes=b"o")

    project.log_artifact(name="p", data_bytes=b"p")

    runner = CliRunner()
    result_a = runner.invoke(
        cli,
        [
            "search",
            "--root-dir",
            project.repository.root_dir,
            "--project-name",
            project.name,
            QUERY,
        ],
    )

    runner = CliRunner()
    result_b = runner.invoke(
        cli, ["search", QUERY], env={"RUBICON_PROJECT_NAME": None, "RUBICON_ROOT_DIR": None}
    )

    runner = CliRunner()
    result_c = runner.invoke(
        cli,
        [
            "search",
            "--root-dir",
            project.repository.root_dir,
            "--project-name",
            project.name,
            "--color",
            TEST_COLOR,
            QUERY,
        ],
    )

    assert result_a.exit_code == 0
    assert "No --root-dir or --project-name provided. Exiting..." in result_b.output
    assert result_b.exit_code == 0
    assert result_c.exit_code == 0
