import os
from unittest.mock import MagicMock

import pytest

from rubicon.repository import MemoryRepository


class AsynchronousMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


@pytest.fixture
def asyn_repo_w_mock_filesystem():
    from rubicon.repository.asynchronous import AsynchronousBaseRepository

    asyn_repo = AsynchronousBaseRepository("s3://test-bucket")
    asyn_repo._persist_bytes = AsynchronousMock()
    asyn_repo._persist_domain = AsynchronousMock()
    asyn_repo.filesystem = AsynchronousMock()

    # Necessary because we are using the synchronous `rm` due to some
    # bugs/missing features in the asynchronous `fsspec._rm_file`.
    asyn_repo.filesystem.rm = MagicMock()

    # Necessary because we are using the synchronous `invalidate_cache`
    # as `fsspec`'s async S3 repo doesn't seem to do it on its own.
    asyn_repo.filesystem.invalidate_cache = MagicMock()

    return asyn_repo


@pytest.fixture
def asyn_s3_repo_w_mock_filesystem():
    from rubicon.repository.asynchronous import S3Repository

    asyn_s3_repo = S3Repository("s3://test-bucket")
    asyn_s3_repo.filesystem = AsynchronousMock()

    return asyn_s3_repo


@pytest.fixture
def asyn_client_w_mock_repo():
    from rubicon.client.asynchronous import Rubicon

    rubicon = Rubicon(persistence="filesystem", root_dir="s3://test-bucket")
    rubicon.repository = AsynchronousMock()

    return rubicon


class MockCompletedProcess:
    """Use to mock a CompletedProcess result from
    `subprocess.run()`.
    """

    def __init__(self, stdout="", returncode=0):
        self.stdout = stdout
        self.returncode = returncode


@pytest.fixture
def mock_completed_process_empty():
    return MockCompletedProcess(stdout=b"\n")


@pytest.fixture
def mock_completed_process_git():
    return MockCompletedProcess(stdout=b"origin github.com (fetch)\n")


@pytest.fixture
def rubicon_client():
    """Setup an instance of rubicon configured to log to memory
    and clean it up afterwards.
    """
    from rubicon import Rubicon

    rubicon = Rubicon(persistence="memory", root_dir="./")

    # teardown after yield
    yield rubicon
    rubicon.repository.filesystem.rm(rubicon.config.root_dir, recursive=True)


@pytest.fixture
def rubicon_local_filesystem_client():
    """Setup an instance of rubicon configured to log to the
    filesystem and clean it up afterwards.
    """
    from rubicon import Rubicon

    rubicon = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubicon"),
    )

    # teardown after yield
    yield rubicon
    rubicon.repository.filesystem.rm(rubicon.config.root_dir, recursive=True)


@pytest.fixture
def project_client(rubicon_client):
    """Setup an instance of rubicon configured to log to memory
    with a default project and clean it up afterwards.
    """
    rubicon = rubicon_client

    project_name = "Test Project"
    project = rubicon.get_or_create_project(
        project_name, description="In memory project for testing."
    )

    return project


@pytest.fixture
def rubicon_and_project_client(rubicon_client):
    """Setup an instance of rubicon configured to log to memory
    with a default project and clean it up afterwards. Expose
    both the rubicon instance and the project.
    """
    rubicon = rubicon_client

    project_name = "Test Project"
    project = rubicon.get_or_create_project(
        project_name, description="In memory project for testing."
    )

    return (rubicon, project)


@pytest.fixture
def rubicon_and_project_client_with_experiments(rubicon_and_project_client):
    """Setup an instance of rubicon configured to log to memory
    with a default project with experiments and clean it up afterwards.
    Expose both the rubicon instance and the project.
    """
    rubicon, project = rubicon_and_project_client

    for e in range(0, 10):
        experiment = project.log_experiment(
            tags=["testing"],
            commit_hash=str(int(e / 3)),
            training_metadata=("training", "metadata"),
        )
        experiment.log_parameter("n_estimators", e + 1)
        experiment.log_feature("age")
        experiment.log_metric("accuracy", (80 + e))

    return (rubicon, project)


@pytest.fixture
def dashboard_setup(rubicon_and_project_client_with_experiments):
    """Setup an instance of the rubicon dashboard with a default project
    and experiment data.
    """
    from rubicon.ui.dashboard import Dashboard

    rubicon, project = rubicon_and_project_client_with_experiments
    dashboard = Dashboard(rubicon.config.persistence, rubicon.config.root_dir)

    return dashboard

@pytest.fixture
def dashboard_setup_without_parameters_or_metrics(rubicon_and_project_client):
    """Setup an instance of the rubicon dashboard with a default project and
    the bare minimum experiment data.
    """
    from rubicon.ui.dashboard import Dashboard

    rubicon, project = rubicon_and_project_client

    for i in range(0, 3):
        project.log_experiment(f"exp-{i}")
    
    dashboard = Dashboard(rubicon.config.persistence, rubicon.config.root_dir)

    return dashboard


@pytest.fixture
def test_dataframe():
    """Create a test dataframe which can be logged to a project or experiment.
    """
    import pandas as pd
    from dask import dataframe as dd

    return dd.from_pandas(pd.DataFrame.from_records([[0, 1]], columns=["a", "b"]), npartitions=1,)


@pytest.fixture
def memory_repository():
    """Setup an in-memory repository and clean it up afterwards.
    """
    root_dir = "/in-memory-root"
    repository = MemoryRepository(root_dir)

    yield repository
    repository.filesystem.rm(root_dir, recursive=True)
