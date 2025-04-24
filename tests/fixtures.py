import datetime
import os
import random
import uuid

import dask.array as da
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
from dask.distributed import Client
from sklearn.datasets import make_classification

from rubicon_ml import Rubicon
from rubicon_ml.repository import MemoryRepository


class _AnotherObject:
    """Another object to log for schema testing."""

    def __init__(self):
        self.another_parameter = 100
        self.another_metric = 100


class _ObjectToLog:
    """An object to log for schema testing."""

    def __init__(self):
        """Initialize an object to log."""

        self.object_ = _AnotherObject()
        self.feature_names_ = ["var_001", "var_002"]
        self.other_feature_names_ = ["var_003", "var_004"]
        self.feature_importances_ = [0.75, 0.25]
        self.feature_name_ = "var_005"
        self.other_feature_name_ = "var_006"
        self.feature_importance_ = 1.0
        self.dataframe = pd.DataFrame([[100, 0], [0, 100]], columns=["x", "y"])
        self.parameter = 100
        self.metric = 100

    def metric_function(self):
        return self.metric

    def artifact_function(self):
        return self

    def dataframe_function(self):
        return pd.DataFrame([[100, 0], [0, 100]], columns=["x", "y"])

    def erroring_function(self):
        raise RuntimeError("raised from `_ObjectToLog.erroring_function`")


class _MockCompletedProcess:
    """Use to mock a CompletedProcess result from `subprocess.run()`."""

    def __init__(self, stdout="", returncode=0):
        self.stdout = stdout
        self.returncode = returncode


@pytest.fixture
def mock_completed_process_empty():
    return _MockCompletedProcess(stdout=b"\n")


@pytest.fixture
def mock_completed_process_git():
    return _MockCompletedProcess(stdout=b"origin github.com (fetch)\n")


@pytest.fixture
def rubicon_client():
    """Setup an instance of rubicon configured to log to memory
    and clean it up afterwards.
    """
    from rubicon_ml import Rubicon

    rubicon = Rubicon(persistence="memory", root_dir="/")

    # teardown after yield
    yield rubicon

    rubicon.repository.filesystem.rm(rubicon.config.root_dir, recursive=True)


@pytest.fixture
def rubicon_composite_client():
    """Setup an instance of rubicon configured to log to two memory
    backends and clean it up afterwards.
    """
    from rubicon_ml import Rubicon

    rubicon = Rubicon(
        composite_config=[
            {"persistence": "memory", "root_dir": "a"},
            {"persistence": "memory", "root_dir": "b"},
        ],
    )

    # teardown after yield
    yield rubicon

    for i, repository in enumerate(rubicon.repositories):
        repository.filesystem.rm(
            rubicon.configs[i].root_dir,
            recursive=True,
        )


@pytest.fixture
def rubicon_local_filesystem_client():
    """Setup an instance of rubicon configured to log to the
    filesystem and clean it up afterwards.
    """
    from rubicon_ml import Rubicon

    rubicon = Rubicon(
        persistence="filesystem",
        root_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "rubicon"),
        storage_option_a="test",  # should be ignored when logging local dfs
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
def project_composite_client(rubicon_composite_client):
    """Setup an instance of rubicon configured to log to two memory
    backends with a default project and clean it up afterwards.
    """
    rubicon = rubicon_composite_client

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
    repository = MemoryRepository(root_dir, storage_option_a="test")

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
            name="test param 2",
            value=random.choice(["A", "B", "C", "D", "E"]),
            tags=["a", "b"],
        )

        experiment.log_metric(name="test metric 0", value=random.random())
        experiment.log_metric(name="test metric 1", value=random.random())

        experiment.log_metric(name="test metric 2", value=[random.random() for _ in range(0, 5)])
        experiment.log_metric(
            name="test metric 3",
            value=[random.random() for _ in range(0, 5)],
            tags=["a", "b"],
        )

        data = np.array(
            [
                list(dates),
                np.linspace(random.randint(0, 15000), random.randint(0, 15000), len(dates)),
            ]
        )
        data_df = pd.DataFrame.from_records(data.T, columns=["test x", "test y"])

        experiment.log_dataframe(data_df, name="test dataframe")

    return project.experiments()


@pytest.fixture
def viz_experiments_no_dataframes(rubicon_and_project_client):
    """Returns a list of experiments with the parameters, metrics, and dataframes
    required to test the `viz` module.
    """
    _, project = rubicon_and_project_client

    # dates = pd.date_range(start="1/1/2010", end="12/1/2020", freq="MS")

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
            name="test param 2",
            value=random.choice(["A", "B", "C", "D", "E"]),
            tags=["a", "b"],
        )

        experiment.log_metric(name="test metric 0", value=random.random())
        experiment.log_metric(name="test metric 1", value=random.random())

        experiment.log_metric(name="test metric 2", value=[random.random() for _ in range(0, 5)])
        experiment.log_metric(
            name="test metric 3",
            value=[random.random() for _ in range(0, 5)],
            tags=["a", "b"],
        )

    return project.experiments()


@pytest.fixture
def objects_to_log():
    """Returns objects for testing."""

    return _ObjectToLog(), _AnotherObject()


@pytest.fixture
def another_object_schema():
    """Returns a schema representing ``_AnotherObject``."""

    return {
        "parameters": [{"name": "another_parameter", "value_attr": "another_parameter"}],
        "metrics": [{"name": "another_metric", "value_attr": "another_metric"}],
    }


@pytest.fixture
def artifact_schema():
    """Returns a schema for testing artifacts."""

    return {
        "artifacts": [
            "self",
            {"name": "object_", "data_object_attr": "object_"},
            {"name": "object_b", "data_object_func": "artifact_function"},
        ]
    }


@pytest.fixture
def dataframe_schema():
    """Returns a schema for testing dataframes."""

    return {
        "dataframes": [
            {"name": "dataframe", "df_attr": "dataframe"},
            {"name": "dataframe_b", "df_func": "dataframe_function"},
        ]
    }


@pytest.fixture
def feature_schema():
    """Returns a schema for testing features."""

    return {
        "features": [
            {
                "names_attr": "feature_names_",
                "importances_attr": "feature_importances_",
            },
            {"names_attr": "other_feature_names_"},
            {"name_attr": "feature_name_", "importance_attr": "feature_importance_"},
            {"name_attr": "other_feature_name_"},
        ]
    }


@pytest.fixture
def metric_schema():
    """Returns a schema for testing metrics."""

    return {
        "metrics": [
            {"name": "metric_a", "value_attr": "metric"},
            {"name": "metric_b", "value_env": "METRIC"},
            {"name": "metric_c", "value_func": "metric_function"},
        ],
    }


@pytest.fixture
def parameter_schema():
    """Returns a schema for testing parameters."""

    return {
        "parameters": [
            {"name": "parameter_a", "value_attr": "parameter"},
            {"name": "parameter_b", "value_env": "PARAMETER"},
        ],
    }


@pytest.fixture
def nested_schema():
    """Returns a schema for testing nested schema."""

    return {"schema": [{"name": "tests___AnotherObject", "attr": "object_"}]}


@pytest.fixture
def optional_schema():
    """Returns a schema for testing optional attributes."""

    return {
        "artifacts": [
            {
                "name": "object",
                "data_object_attr": "missing_object",
                "optional": "true",
            },
            {
                "name": "object_b",
                "data_object_func": "missing_object_func",
                "optional": "true",
            },
        ],
        "dataframes": [
            {"name": "dataframe", "df_attr": "missing_dataframe", "optional": "true"},
            {
                "name": "dataframe_b",
                "df_func": "missing_dataframe_func",
                "optional": "true",
            },
        ],
        "features": [
            {"names_attr": "missing_feature_names", "optional": "true"},
            {"name_attr": "missing_feature_name", "optional": "true"},
        ],
        "metrics": [
            {"name": "metric_a", "value_attr": "missing_metric", "optional": "true"},
            {"name": "metric_b", "value_env": "MISSING_METRIC", "optional": "true"},
            {
                "name": "metric_c",
                "value_func": "missing_metric_func",
                "optional": "true",
            },
        ],
        "parameters": [
            {
                "name": "parameter_a",
                "value_attr": "missing_parameter",
                "optional": "true",
            },
            {
                "name": "parameter_b",
                "value_env": "MISSING_PARAMETER",
                "optional": "true",
            },
        ],
        "schema": [
            {
                "name": "MissingObject",
                "attr": "another_missing_object",
                "optional": "true",
            }
        ],
    }


@pytest.fixture
def hierarchical_schema():
    """Returns a schema for testing hierarchical schema."""

    return {"children": [{"name": "AnotherObject", "attr": "children"}]}


@pytest.fixture
def rubicon_project():
    """Returns an in-memory rubicon project for testing."""

    rubicon = Rubicon(persistence="memory", root_dir="/tmp")

    random_name = str(uuid.uuid4())
    return rubicon.create_project(name=random_name)


@pytest.fixture
def make_classification_array():
    """Returns classification data generated by scikit-learn as an array."""

    X, y = make_classification(
        n_samples=1000,
        n_features=10,
        n_informative=5,
        n_redundant=5,
        n_classes=2,
        class_sep=1,
        random_state=3211,
    )

    return X, y


@pytest.fixture
def make_classification_df(make_classification_array):
    """Returns classification data generated by scikit-learn as dataframes."""

    X, y = make_classification_array
    X_df = pd.DataFrame(X, columns=[f"var_{i}" for i in range(10)])

    return X_df, y


@pytest.fixture
def dask_client():
    """Returns a dask client and shuts it down upon test completion."""

    client = Client()

    yield client

    client.shutdown()


@pytest.fixture
def make_classification_dask_array(make_classification_array):
    """Returns classification data generated by scikit-learn as a dask array."""

    X, y = make_classification_array
    X_da, y_da = da.from_array(X), da.from_array(y)

    return X_da, y_da


@pytest.fixture
def make_classification_dask_df(make_classification_df):
    """Returns classification data generated by scikit-learn as dataframes."""

    X, y = make_classification_df
    X_df, y_da = dd.from_pandas(X, npartitions=1), da.from_array(y)

    return X_df, y_da


@pytest.fixture
def project_json():
    """JSON representation of a project."""
    return {
        "name": "test project",
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test project description",
        "github_url": "github.com",
        "id": "ccf6b8f8-a166-4084-a51f-4f2b6afd2ad9",
        "training_metadata": [["training", "metadata"]],
    }


@pytest.fixture
def experiment_json():
    """JSON representation of an experiment."""
    return {
        "project_name": "test project",
        "branch_name": "test-branch",
        "comments": ["comment a", "comment b"],
        "commit_hash": "abcde01",
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test experiment description",
        "id": "69e374cd-220b-4cda-9608-52277b38a976",
        "model_name": "test model",
        "name": "test experiment",
        "tags": ["tag_a", "tag_b"],
        "training_metadata": [["training", "metadata"]],
    }


@pytest.fixture
def feature_json():
    """JSON representation of a feature."""
    return {
        "name": "test feature",
        "comments": ["comment a", "comment b"],
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test feature description",
        "id": str(uuid.uuid4()),
        "importance": 1.0,
        "tags": ["tag_a", "tag_b"],
    }


@pytest.fixture
def metric_json():
    """JSON representation of a metric."""
    return {
        "name": "test metric",
        "value": 1.0,
        "comments": ["comment a", "comment b"],
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test metric description",
        "directionality": "score",
        "id": str(uuid.uuid4()),
        "tags": ["tag_a", "tag_b"],
    }


@pytest.fixture
def parameter_json():
    """JSON representation of a parameter."""
    return {
        "name": "test parameter",
        "value": 1.0,
        "comments": ["comment a", "comment b"],
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test parameter description",
        "id": str(uuid.uuid4()),
        "tags": ["tag_a", "tag_b"],
    }


@pytest.fixture
def artifact_project_json():
    """JSON representation of an artifact belonging to a project."""
    return {
        "name": "test artifact",
        "comments": ["comment a", "comment b"],
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test parameter description",
        "id": str(uuid.uuid4()),
        "parent_id": "ccf6b8f8-a166-4084-a51f-4f2b6afd2ad9",
        "tags": ["tag_a", "tag_b"],
    }


@pytest.fixture
def artifact_experiment_json():
    """JSON representation of an artifact belonging to an experiment."""
    return {
        "name": "test artifact",
        "comments": ["comment a", "comment b"],
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test parameter description",
        "id": str(uuid.uuid4()),
        "parent_id": "69e374cd-220b-4cda-9608-52277b38a976",
        "tags": ["tag_a", "tag_b"],
    }


@pytest.fixture
def dataframe_project_json():
    """JSON representation of a dataframe belonging to a project."""
    return {
        "comments": ["comment a", "comment b"],
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test parameter description",
        "id": str(uuid.uuid4()),
        "name": "test dataframe",
        "parent_id": "ccf6b8f8-a166-4084-a51f-4f2b6afd2ad9",
        "tags": ["tag_a", "tag_b"],
    }


@pytest.fixture
def dataframe_experiment_json():
    """JSON representation of a dataframe belonging to an experiment."""
    return {
        "comments": ["comment a", "comment b"],
        "created_at": datetime.datetime(2024, 1, 1),
        "description": "test parameter description",
        "id": str(uuid.uuid4()),
        "name": "test dataframe",
        "parent_id": "69e374cd-220b-4cda-9608-52277b38a976",
        "tags": ["tag_a", "tag_b"],
    }
