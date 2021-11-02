from unittest import mock

import pytest

from rubicon_ml import domain
from rubicon_ml.client import Project, Rubicon
from rubicon_ml.exceptions import RubiconException


class MockCompletedProcess:
    def __init__(self, stdout="", returncode=0):
        self.stdout = stdout
        self.returncode = returncode


def test_properties():
    domain_project = domain.Project(
        "Test Project",
        description="a test project",
        github_url="github.com",
        training_metadata=domain.utils.TrainingMetadata([("test/path", "SELECT * FROM test")]),
    )
    project = Project(domain_project)

    assert project.name == "Test Project"
    assert project.description == "a test project"
    assert project.github_url == "github.com"
    assert project.training_metadata == domain_project.training_metadata.training_metadata[0]
    assert project.created_at == domain_project.created_at
    assert project.id == domain_project.id


def test_get_branch_name(project_client):
    project = project_client

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = MockCompletedProcess(stdout=b"branch-name\n")

        expected = [
            mock.call(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True),
        ]

        assert project._get_branch_name() == "branch-name"
        assert mock_run.mock_calls == expected


def test_get_commit_hash(project_client):
    project = project_client

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = MockCompletedProcess(stdout=b"abcd0000\n")

        expected = [
            mock.call(["git", "rev-parse", "HEAD"], capture_output=True),
        ]

        assert project._get_commit_hash() == "abcd0000"
        assert mock_run.mock_calls == expected


def test_create_experiment_with_auto_git():
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = MockCompletedProcess(stdout=b"test", returncode=0)

        rubicon = Rubicon("memory", "test-root", auto_git_enabled=True)
        project = rubicon.create_project("Test Project A")
        project.log_experiment()

        expected = [
            mock.call(["git", "rev-parse", "--git-dir"], capture_output=True),
            mock.call(["git", "remote", "-v"], capture_output=True),
            mock.call(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True),
            mock.call(["git", "rev-parse", "HEAD"], capture_output=True),
        ]

    assert mock_run.mock_calls == expected

    rubicon.repository.filesystem.store = {}


def test_experiments_log_and_retrieval(project_client):
    project = project_client
    experiment1 = project.log_experiment(
        name="exp1", training_metadata=[("test/path", "SELECT * FROM test")]
    )
    experiment2 = project.log_experiment(name="exp2")

    assert experiment1._domain.project_name == project.name
    assert len(project.experiments()) == 2
    assert experiment1.id in [e.id for e in project.experiments()]
    assert experiment2.id in [e.id for e in project.experiments()]


def test_experiment_by_id(rubicon_and_project_client):
    project = rubicon_and_project_client[1]
    _experiment = project.log_experiment(tags=["x"])
    project.log_experiment(tags=["y"])

    experiment = project.experiment(id=_experiment.id)

    assert experiment.id == _experiment.id


def test_experiment_by_name(project_client):
    project = project_client
    project.log_experiment(name="exp1")
    experiment = project.experiment(name="exp1")

    assert experiment.name == "exp1"


def test_experiment_name_not_found_error(project_client):
    project = project_client
    with pytest.raises(RubiconException) as e:
        project.experiment(name="exp1")

    assert "No experiment found with name exp1." in str(e)


def test_experiments_tagged_and(project_client):
    project = project_client
    experiment = project.log_experiment(tags=["x", "y"])
    project.log_experiment(tags=["x"])
    project.log_experiment(tags=["y"])

    experiments = project.experiments(tags=["x", "y"], qtype="and")

    assert len(experiments) == 1
    assert experiment.id in [e.id for e in experiments]


def test_experiments_tagged_or(project_client):
    project = project_client
    experiment_a = project.log_experiment(tags=["x"])
    experiment_b = project.log_experiment(tags=["y"])
    project.log_experiment(tags=["z"])

    experiments = project.experiments(tags=["x", "y"], qtype="or")

    assert len(experiments) == 2
    assert experiment_a.id in [e.id for e in experiments]
    assert experiment_b.id in [e.id for e in experiments]


def test_dataframes_recursive(project_client, test_dataframe):
    project = project_client
    experiment = project.log_experiment()

    df = test_dataframe
    dataframe_a = project.log_dataframe(df)
    dataframe_b = experiment.log_dataframe(df)

    dataframes = project.dataframes(recursive=True)

    assert len(dataframes) == 2
    assert dataframe_a.id in [d.id for d in dataframes]
    assert dataframe_b.id in [d.id for d in dataframes]


def test_to_dask_df(rubicon_and_project_client_with_experiments):
    project = rubicon_and_project_client_with_experiments[1]
    ddf = project.to_dask_df()

    # compute to pandas df so we can use iloc for easier testing
    df = ddf.compute()

    # check that all experiments made it into df
    assert len(df) == 10

    # check the cols within the df
    exp_details = ["id", "name", "description", "model_name", "commit_hash", "tags", "created_at"]
    for detail in exp_details:
        assert detail in df.columns


def test_to_dask_df_grouped_by_commit_hash(rubicon_and_project_client_with_experiments):
    project = rubicon_and_project_client_with_experiments[1]
    ddfs = project.to_dask_df(group_by="commit_hash")

    # compute to pandas df so we can use iloc for easier testing
    dfs = [ddf.compute() for ddf in ddfs.values()]

    # check df was broken into 4 groups
    assert len(dfs) == 4

    for df in dfs:
        # check the cols within the df
        exp_details = [
            "id",
            "name",
            "description",
            "model_name",
            "commit_hash",
            "tags",
            "created_at",
        ]
        for detail in exp_details:
            assert detail in df.columns


def test_to_dask_df_grouped_by_invalid_group(rubicon_and_project_client_with_experiments):
    project = rubicon_and_project_client_with_experiments[1]
    with pytest.raises(ValueError) as e:
        project.to_dask_df(group_by="INVALID")

    assert "`group_by` must be one of" in str(e)
