import os
import time
import warnings
from unittest import mock
from unittest.mock import patch

import pytest

from rubicon_ml import domain
from rubicon_ml.client import Project, Rubicon
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.utils import slugify


def _raise_error():
    raise RubiconException()


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


def test_get_identifiers(project_client):
    project = project_client
    project_name, experiment_id = project._get_identifiers()

    assert project_name == project.name
    assert experiment_id is None


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


@mock.patch("rubicon_ml.repository.BaseRepository.get_experiments")
def test_get_experiments_multiple_backend_error(mock_get_experiments, project_composite_client):
    project = project_composite_client

    mock_get_experiments.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        project.experiments()
    assert "all configured storage backends failed" in str(e)


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


def test_get_experiment_fails_both_set(project_client):
    project = project_client
    project.log_experiment(name="exp1")
    with pytest.raises(ValueError) as e:
        project.experiment(name="foo", id=123)

    assert "`name` OR `id` required." in str(e.value)


def test_get_experiment_fails_neither_set(project_client):
    project = project_client
    project.log_experiment(name="exp1")
    with pytest.raises(ValueError) as e:
        project.experiment(name=None, id=None)

    assert "`name` OR `id` required." in str(e.value)


@mock.patch("rubicon_ml.repository.BaseRepository.get_experiment")
def test_get_experiment_multiple_backend_error(mock_get_experiment, project_composite_client):
    project = project_composite_client

    mock_get_experiment.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        project.experiment("exp1")
    assert "all configured storage backends failed" in str(e)


def test_experiment_warning(project_client, test_dataframe):
    project = project_client
    experiment_a = project.log_experiment(name="exp1")
    experiment_b = project.log_experiment(name="exp1")

    with warnings.catch_warnings(record=True) as w:
        experiment_c = project.experiment(name="exp1")
        assert (
            "Multiple experiments found with name 'exp1'. Returning most recently logged"
        ) in str(w[0].message)
    assert experiment_c.id != experiment_a.id
    assert experiment_c.id == experiment_b.id


def test_experiment_name_not_found_error(project_client):
    project = project_client
    with pytest.raises(RubiconException) as e:
        project.experiment(name="exp1")

    assert "No experiment found with name 'exp1'." in str(e)


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
    ddf = project.to_df(df_type="dask")

    # compute to pandas df so we can use iloc for easier testing
    df = ddf.compute()

    # check that all experiments made it into df
    assert len(df) == 10

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


def test_to_pandas_df(rubicon_and_project_client_with_experiments):
    project = rubicon_and_project_client_with_experiments[1]
    df = project.to_df(df_type="pandas")

    # check that all experiments made it into df
    assert len(df) == 10

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


def test_to_dask_df_grouped_by_commit_hash(rubicon_and_project_client_with_experiments):
    project = rubicon_and_project_client_with_experiments[1]
    ddfs = project.to_df(df_type="dask", group_by="commit_hash")

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


def test_to_pandas_df_grouped_by_commit_hash(
    rubicon_and_project_client_with_experiments,
):
    project = rubicon_and_project_client_with_experiments[1]
    dfs = project.to_df(df_type="pandas", group_by="commit_hash")

    # check df was broken into 4 groups
    assert len(dfs) == 4

    for df in dfs.values():
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


def test_to_df_grouped_by_invalid_group(rubicon_and_project_client_with_experiments):
    project = rubicon_and_project_client_with_experiments[1]
    with pytest.raises(ValueError) as e:
        project.to_df(group_by="INVALID")

    assert "`group_by` must be one of" in str(e)


def test_archive_no_experiments(project_client):
    project = project_client
    with pytest.raises(ValueError):
        project.archive()


def test_archive_bad_experiments_argument(project_client):
    project = project_client
    experiment1 = project.log_experiment(name="experiment1")
    with pytest.raises(ValueError):
        project.archive(experiment1)

    project.log_experiment(name="experiment2")
    with pytest.raises(ValueError):
        project.archive(["experiment1", "experiment2"])


def test_archive_all_experiments(rubicon_local_filesystem_client_with_project):
    project = rubicon_local_filesystem_client_with_project[1]
    project.log_experiment(name="experiment1")
    project.log_experiment(name="experiment2")
    project.log_experiment(name="experiment3")
    archive_path = project.archive()

    assert project.repository._exists(archive_path)


def test_archive_select_experiments(rubicon_local_filesystem_client_with_project):
    project = rubicon_local_filesystem_client_with_project[1]
    experiment1 = project.log_experiment(name="experiment1")
    experiment2 = project.log_experiment(name="experiment2")
    project.log_experiment(name="experiment3")
    zip_archive_filename = project.archive([experiment1, experiment2])

    assert project.repository._exists(zip_archive_filename)


def test_archive_bad_remote_rubicon():
    rubiconA = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconA"),
    )

    rubiconB = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconB"),
    )

    rubiconA.get_or_create_project("ArchiveTesting")
    projectB = rubiconB.get_or_create_project("ArchiveTesting")
    projectB.log_experiment(name="experiment1")
    projectB.log_experiment(name="experiment2")
    with pytest.raises(ValueError):
        projectB.archive(remote_rubicon=os.path.dirname(os.path.realpath(__file__)))

    rubiconA.repository.filesystem.rm(rubiconA.config.root_dir, recursive=True)
    rubiconB.repository.filesystem.rm(rubiconB.config.root_dir, recursive=True)


def test_archive_remote_rubicon():
    rubiconA = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconA"),
    )

    rubiconB = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconB"),
    )

    projectA = rubiconA.get_or_create_project("ArchiveTesting")
    projectB = rubiconB.get_or_create_project("ArchiveTesting")
    projectB.log_experiment(name="experiment1")
    projectB.log_experiment(name="experiment2")
    zip_archive_filename = projectB.archive(remote_rubicon=rubiconA)

    assert projectA.repository._exists(zip_archive_filename)

    rubiconA.repository.filesystem.rm(rubiconA.config.root_dir, recursive=True)
    rubiconB.repository.filesystem.rm(rubiconB.config.root_dir, recursive=True)


def test_experiments_from_archive_bad_argument():
    rubiconA = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconA"),
    )

    rubiconB = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconB"),
    )
    projectA = rubiconA.get_or_create_project("ArchiveTesting")
    projectA.log_experiment(name="experiment1")
    projectA.log_experiment(name="experiment2")
    # no archive created

    projectB = rubiconB.get_or_create_project("ArchiveTesting")
    with pytest.raises(ValueError):
        projectB.experiments_from_archive(
            remote_rubicon=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconA")
        )
    rubiconA.repository.filesystem.rm(rubiconA.config.root_dir, recursive=True)
    rubiconB.repository.filesystem.rm(rubiconB.config.root_dir, recursive=True)


def test_experiments_from_archive():
    root_dirA = os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconA")
    rubiconA = Rubicon(
        persistence="filesystem",
        root_dir=root_dirA,
    )

    root_dirB = os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconB")
    rubiconB = Rubicon(
        persistence="filesystem",
        root_dir=root_dirB,
    )

    projectA = rubiconA.get_or_create_project("ArchiveTesting")
    projectA.log_experiment(name="experiment1")
    projectA.log_experiment(name="experiment2")
    projectA.archive()

    projectB = rubiconB.get_or_create_project("ArchiveTesting")
    projectB.log_experiment(name="experiment3")
    projectB.log_experiment(name="experiment4")

    experiments_dirB = os.path.join(root_dirB, slugify(projectB.name), "experiments")
    og_num_exps_B = len(projectB.repository._ls(experiments_dirB))
    assert og_num_exps_B == 2

    projectB.experiments_from_archive(remote_rubicon=rubiconA)

    new_num_expsB = len(projectB.repository._ls(experiments_dirB))
    assert new_num_expsB == 4
    rubiconA.repository.filesystem.rm(rubiconA.config.root_dir, recursive=True)
    rubiconB.repository.filesystem.rm(rubiconB.config.root_dir, recursive=True)


def test_experiments_from_archive_latest_only():
    root_dirA = os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconA")
    rubiconA = Rubicon(
        persistence="filesystem",
        root_dir=root_dirA,
    )

    root_dirB = os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconB")
    rubiconB = Rubicon(
        persistence="filesystem",
        root_dir=root_dirB,
    )

    projectA = rubiconA.get_or_create_project("ArchiveTesting")
    projectA.log_experiment(name="experiment1")
    projectA.log_experiment(name="experiment2")
    projectA.log_experiment(name="experiment3")
    projectA.archive()

    projectB = rubiconB.get_or_create_project("ArchiveTesting")
    projectB.log_experiment(name="experiment4")
    projectB.log_experiment(name="experiment5")

    experiment6 = projectA.log_experiment(name="experiment6")
    experiment7 = projectA.log_experiment(name="experiment7")
    time.sleep(1)
    projectA.archive([experiment6, experiment7])

    experiments_dirB = os.path.join(root_dirB, slugify(projectB.name), "experiments")
    og_num_exps_B = len(projectB.repository._ls(experiments_dirB))
    assert og_num_exps_B == 2

    projectB.experiments_from_archive(remote_rubicon=rubiconA, latest_only=True)

    new_num_expsB = len(projectB.repository._ls(experiments_dirB))
    assert new_num_expsB == 4
    rubiconA.repository.filesystem.rm(rubiconA.config.root_dir, recursive=True)
    rubiconB.repository.filesystem.rm(rubiconB.config.root_dir, recursive=True)


@patch("fsspec.open")
def test_archive_remote_rubicon_s3(mock_open):
    print("buffer")
    rubicon_a = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubiconA"),
    )
    s3_repo = "s3://bucket/root/path/to/data"

    rubicon_b = Rubicon(persistence="filesystem", root_dir=s3_repo)

    projectA = rubicon_a.get_or_create_project("ArchiveTesting")
    projectA.log_experiment(name="experiment1")
    projectA.log_experiment(name="experiment2")

    zip_archive_filename = projectA.archive(remote_rubicon=rubicon_b)

    mock_open.assert_called_once_with(zip_archive_filename, "wb")
    rubicon_a.repository.filesystem.rm(rubicon_a.config.root_dir, recursive=True)


def test_wrong_json_schema_experiment(rubicon_local_filesystem_client_with_project):
    """Test that our new error catchers work for bad json schemas."""
    rubicon, project = rubicon_local_filesystem_client_with_project
    experiment_location = rubicon.repository._get_experiment_metadata_root(project.name)
    os.mkdir(experiment_location)
    with open(os.path.join(experiment_location, "bad_experiment.json"), "w") as f:
        f.write("bad json")

    assert project.experiments() == []


def test_no_json_schema_experiment(rubicon_local_filesystem_client_with_project):
    """Test that our new error catchers work when we are missing json schemas."""
    rubicon, project = rubicon_local_filesystem_client_with_project
    experiment_location = rubicon.repository._get_experiment_metadata_root(project.name)
    os.mkdir(experiment_location)

    assert project.experiments() == []
