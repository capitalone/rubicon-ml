import os
import tempfile

import fsspec

from rubicon_ml import domain
from rubicon_ml.repository import LocalRepository
from rubicon_ml.repository.utils import json, slugify


def test_read_regression(
    artifact_json,
    dataframe_json,
    experiment_json,
    feature_json,
    metric_json,
    parameter_json,
    project_json,
):
    """Tests that `rubicon_ml` can read each domain entity from the filesystem."""
    filesystem = fsspec.filesystem("file")

    with tempfile.TemporaryDirectory() as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = LocalRepository(root_dir=root_dir)

        expected_project_path = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "metadata.json",
        )

        filesystem.mkdirs(os.path.dirname(expected_project_path), exist_ok=True)
        with filesystem.open(expected_project_path, "w") as file:
            file.write(json.dumps(project_json))

        project = repository.get_project(project_json["name"]).__dict__

        assert project == project_json


def test_read_write_regression(
    artifact_json,
    dataframe_json,
    experiment_json,
    feature_json,
    metric_json,
    parameter_json,
    project_json,
):
    """Tests that `rubicon_ml` can read each domain entity that it wrote."""
    with tempfile.TemporaryDirectory() as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = LocalRepository(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        project = repository.get_project(project_json["name"]).__dict__

        assert project == project_json


def test_write_regression(
    artifact_json,
    dataframe_json,
    experiment_json,
    feature_json,
    metric_json,
    parameter_json,
    project_json,
):
    """Tests that `rubicon_ml` can write each domain entity to the filesystem."""
    filesystem = fsspec.filesystem("file")

    with tempfile.TemporaryDirectory() as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = LocalRepository(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))

        expected_project_path = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "metadata.json",
        )

        with filesystem.open(expected_project_path, "r") as file:
            project = json.loads(file.read())

        assert project == project_json
