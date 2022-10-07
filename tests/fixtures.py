import os
import random

import numpy as np
import pandas as pd
import pytest

from rubicon_ml.repository import MemoryRepository


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
    from rubicon_ml import Rubicon

    rubicon = Rubicon(persistence="memory", root_dir="./")

    # teardown after yield
    yield rubicon
    rubicon.repository.filesystem.rm(rubicon.config.root_dir, recursive=True)


@pytest.fixture
def rubicon_local_filesystem_client():
    """Setup an instance of rubicon configured to log to the
    filesystem and clean it up afterwards.
    """
    from rubicon_ml import Rubicon

    rubicon = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubicon"),
    )

    # teardown after yield
    yield rubicon
    rubicon.repository.filesystem.rm(rubicon.config.root_dir, recursive=True)


@pytest.fixture
def rubicon_local_filesystem_client_with_project(rubicon_local_filesystem_client):
    rubicon = rubicon_local_filesystem_client

    project_name = "Test Project"
    project = rubicon.get_or_create_project(project_name, description="testing")

    return rubicon, project


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
        project_name,
        description="In memory project for testing.",
        github_url="test.github.url.git",
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
        experiment.log_feature("year")
        experiment.log_metric("accuracy", (80 + e))

    return (rubicon, project)


@pytest.fixture
def test_dataframe():
    """Create a test dataframe which can be logged to a project or experiment."""
    import pandas as pd
    from dask import dataframe as dd

    return dd.from_pandas(
        pd.DataFrame.from_records([[0, 1]], columns=["a", "b"]),
        npartitions=1,
    )


@pytest.fixture
def memory_repository():
    """Setup an in-memory repository and clean it up afterwards."""
    root_dir = "/in-memory-root"
    repository = MemoryRepository(root_dir)

    yield repository
    repository.filesystem.rm(root_dir, recursive=True)


@pytest.fixture
def fake_estimator_cls():
    """A fake estimator that exposes the same API as a sklearn
    estimator so we can test without relying on sklearn.
    """

    class FakeEstimator:
        def __init__(self, params=None):
            if params is None:
                params = {"max_df": 0.75, "lowercase": True, "ngram_range": (1, 2)}

            self.params = params

        def get_params(self):
            return self.params

        def fit(self):
            pass

        def transform(self):
            pass

    return FakeEstimator


@pytest.fixture
def viz_experiments(rubicon_and_project_client):
    """Returns a list of experiments with the parameters, metrics, and dataframes
    required to test the `viz` module.
    """
    rubicon, project = rubicon_and_project_client

    dates = pd.date_range(start="1/1/2010", end="12/1/2020", freq="MS")

    for i in range(0, 10):
        experiment = project.log_experiment(
            commit_hash="1234567",
            model_name="test model name",
            name="test name",
            tags=["test tag"],
        )

        experiment.log_parameter(name="test param 0", value=random.choice([True, False]))
        experiment.log_parameter(name="test param 1", value=random.randrange(2, 10, 2))
        experiment.log_parameter(
            name="test param 2", value=random.choice(["A", "B", "C", "D", "E"])
        )

        experiment.log_metric(name="test metric 0", value=random.random())
        experiment.log_metric(name="test metric 1", value=random.random())

        experiment.log_metric(name="test metric 2", value=[random.random() for _ in range(0, 5)])
        experiment.log_metric(name="test metric 3", value=[random.random() for _ in range(0, 5)])

        data = np.array(
            [
                list(dates),
                np.linspace(random.randint(0, 15000), random.randint(0, 15000), len(dates)),
            ]
        )
        data_df = pd.DataFrame.from_records(data.T, columns=["test x", "test y"])

        experiment.log_dataframe(data_df, name="test dataframe")

    return project.experiments()
