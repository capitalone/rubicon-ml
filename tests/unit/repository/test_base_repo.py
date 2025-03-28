import sys
import uuid
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest
from dask import dataframe as dd

from rubicon_ml import domain
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import MemoryRepository
from rubicon_ml.repository.utils import json, slugify

# -------- Helpers --------


def _create_project(repository):
    project = domain.Project(f"Test Project {uuid.uuid4()}")
    repository.create_project(project)

    return project


def _create_experiment(repository, project=None, tags=[], comments=[]):
    if project is None:
        project = _create_project(repository)

    experiment = domain.Experiment(
        name=f"Test Experiment {uuid.uuid4()}",
        project_name=project.name,
        tags=[],
        comments=[],
    )
    repository.create_experiment(experiment)

    return experiment


def _create_artifact(repository, project=None, artifact_data=None):
    if project is None:
        project = _create_project(repository)

    if artifact_data is None:
        artifact_data = b"test artifact data"

    artifact = domain.Artifact(name=f"{uuid.uuid4}.txt", parent_id=project.id)
    repository.create_artifact(artifact, artifact_data, project.name)

    return artifact


def _create_pandas_dataframe(repository, project=None, dataframe_data=None, multi_index=False):
    if project is None:
        project = _create_project(repository)

    if dataframe_data is None:
        dataframe_data = pd.DataFrame(
            [[0, 1, "a"], [1, 1, "b"], [2, 2, "c"], [3, 2, "d"]],
            columns=["a", "b", "c"],
        )
        if multi_index:
            dataframe_data = dataframe_data.set_index(["b", "a"])  # Set multiindex

    dataframe = domain.Dataframe(parent_id=project.id)
    repository.create_dataframe(dataframe, dataframe_data, project.name)

    return dataframe


def _create_dask_dataframe(repository, project=None):
    if project is None:
        project = _create_project(repository)

    df = pd.DataFrame([[0, 1, "a"], [1, 1, "b"], [2, 2, "c"], [3, 2, "d"]], columns=["a", "b", "c"])
    ddf = dd.from_pandas(df, npartitions=1)

    dataframe = domain.Dataframe(parent_id=project.id)
    repository.create_dataframe(dataframe, ddf, project.name)

    return dataframe


def _create_polars_dataframe(repository, project=None):
    if project is None:
        project = _create_project(repository)

    df = pl.DataFrame(
        {
            "a": [0, 1, 2, 3],
            "b": [1, 1, 2, 2],
            "c": ["a", "b", "c", "d"],
        }
    )

    dataframe = domain.Dataframe(parent_id=project.id)
    repository.create_dataframe(dataframe, df, project.name)

    return dataframe


def _create_feature(repository, experiment=None):
    if experiment is None:
        experiment = _create_experiment(repository)

    feature = domain.Feature(name=f"Test Feature {uuid.uuid4()}")
    repository.create_feature(feature, experiment.project_name, experiment.id)

    return feature


def _create_metric(repository, experiment=None):
    if experiment is None:
        experiment = _create_experiment(repository)

    metric = domain.Metric(name=f"Test Metric {uuid.uuid4()}", value=24)
    repository.create_metric(metric, experiment.project_name, experiment.id)

    return metric


def _create_parameter(repository, experiment=None):
    if experiment is None:
        experiment = _create_experiment(repository)

    parameter = domain.Parameter(name=f"Test Parameter {uuid.uuid4()}", value=8)
    repository.create_parameter(parameter, experiment.project_name, experiment.id)

    return parameter


# -------- Projects --------


def test_create_project(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    project_dir = slugify(project.name)

    project_metadata_path = f"{repository.root_dir}/{project_dir}/metadata.json"
    open_file = repository.filesystem.open(project_metadata_path)
    with open_file as f:
        project_json = json.load(f)

    assert project.id == project_json["id"]
    assert project.name == project_json["name"]
    assert project.description == project_json["description"]


def test_create_project_throws_error_if_duplicate(memory_repository):
    repository = memory_repository
    project = _create_project(repository)

    with pytest.raises(RubiconException) as e:
        repository.create_project(domain.Project(project.name))

    assert f"'{project.name}' already exists" in str(e)


def test_get_project(memory_repository):
    repository = memory_repository
    written_project = _create_project(repository)
    project = repository.get_project(written_project.name)

    assert project.id == written_project.id
    assert project.name == written_project.name


def test_get_project_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    with pytest.raises(RubiconException) as e:
        repository.get_project(f"Test Project {uuid.uuid4()}")

    assert "No project with name 'Test Project " in str(e)


def test_get_projects(memory_repository):
    repository = memory_repository
    written_projects = [_create_project(repository) for i in range(0, 3)]
    projects = repository.get_projects()

    assert len(projects) == 3

    project_ids = [p.id for p in written_projects]
    for project in projects:
        assert project.id in project_ids
    assert projects[0].created_at < projects[1].created_at
    assert projects[1].created_at < projects[2].created_at


def test_get_projects_with_no_results(memory_repository):
    repository = memory_repository
    projects = repository.get_projects()

    assert projects == []


# ------ Experiments -------


def test_create_experiment(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    experiment_dir = f"{slugify(experiment.project_name)}/experiments/{experiment.id}"

    experiment_metadata_path = f"{repository.root_dir}/{experiment_dir}/metadata.json"
    open_file = repository.filesystem.open(experiment_metadata_path)
    with open_file as f:
        experiment_json = json.load(f)

    assert experiment.id == experiment_json["id"]
    assert experiment.name == experiment_json["name"]
    assert experiment.project_name == experiment_json["project_name"]


def test_get_experiment(memory_repository):
    repository = memory_repository
    written_experiment = _create_experiment(repository)
    experiment = repository.get_experiment(written_experiment.project_name, written_experiment.id)

    assert experiment.id == written_experiment.id
    assert experiment.name == written_experiment.name
    assert experiment.project_name == written_experiment.project_name


def test_get_experiment_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    missing_experiment_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        repository.get_experiment(project.name, missing_experiment_id)

    assert f"No experiment with id `{missing_experiment_id}`" in str(e)


def test_get_experiments(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    written_experiments = [_create_experiment(repository, project=project) for _ in range(0, 3)]
    experiments = repository.get_experiments(project.name)

    assert len(experiments) == 3

    experiment_ids = [e.id for e in written_experiments]
    for experiment in experiments:
        assert experiment.id in experiment_ids
        experiment_ids.remove(experiment.id)
    assert experiments[0].created_at < experiments[1].created_at
    assert experiments[1].created_at < experiments[2].created_at


def test_get_experiments_with_no_results(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    experiments = repository.get_experiments(project.name)

    assert experiments == []


# ------- Artifacts --------


def test_get_artifact_with_project_parent_root(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    artifact_root = repository._get_artifact_metadata_root(project.name)

    assert artifact_root == f"{repository.root_dir}/{slugify(project.name)}/artifacts"


def test_get_artifact_with_experiment_parent_root(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    experiment = _create_experiment(repository, project=project)
    artifact_root = repository._get_artifact_metadata_root(project.name, experiment.id)

    assert (
        artifact_root
        == f"{repository.root_dir}/{slugify(project.name)}/experiments/{experiment.id}/artifacts"
    )


def test_create_artifact(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    artifact = _create_artifact(repository, project=project)

    artifact_root = f"{repository.root_dir}/{slugify(project.name)}/artifacts/{artifact.id}"
    artifact_metadata_path = f"{artifact_root}/metadata.json"
    artifact_data_path = f"{artifact_root}/data"

    open_file = repository.filesystem.open(artifact_metadata_path)
    with open_file as f:
        artifact_json = json.load(f)

    assert repository.filesystem.exists(artifact_data_path)
    assert artifact.id == artifact_json["id"]
    assert artifact.name == artifact_json["name"]


def test_get_artifact(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    written_artifact = _create_artifact(repository, project=project)
    artifact = repository.get_artifact_metadata(project.name, written_artifact.id)

    assert artifact.id == written_artifact.id
    assert artifact.name == written_artifact.name
    assert artifact.parent_id == written_artifact.parent_id


def test_get_artifact_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    missing_artifact_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        repository.get_artifact_metadata(project.name, missing_artifact_id)

    assert f"No artifact with id `{missing_artifact_id}`" in str(e)


def test_get_artifacts(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    written_artifacts = [_create_artifact(repository, project=project) for _ in range(0, 3)]
    artifacts = repository.get_artifacts_metadata(project.name)

    artifact_ids = [a.id for a in written_artifacts]
    for artifact in artifacts:
        assert artifact.id in artifact_ids
        artifact_ids.remove(artifact.id)

    assert artifacts[0].created_at < artifacts[1].created_at
    assert artifacts[1].created_at < artifacts[2].created_at


def test_get_artifacts_no_results(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    artifacts = repository.get_artifacts_metadata(project.name)

    assert artifacts == []


def test_get_artifact_data(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    artifact_data = b"test artifact data"

    artifact = _create_artifact(repository, project=project, artifact_data=artifact_data)
    data = repository.get_artifact_data(project.name, artifact.id)

    assert artifact_data == data


def test_get_artifact_data_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    missing_artifact_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        repository.get_artifact_data(project.name, missing_artifact_id)

    assert f"No data for artifact with id `{missing_artifact_id}`" in str(e)


def test_delete_artifact(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    artifact_data = b"test artifact data"
    artifact = _create_artifact(repository, project=project, artifact_data=artifact_data)

    repository.delete_artifact(project.name, artifact.id)

    with pytest.raises(RubiconException) as e:
        repository.get_artifact_metadata(project.name, artifact.id)

    assert f"No artifact with id `{artifact.id}`" in str(e)


def test_delete_artifact_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    missing_artifact_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        repository.delete_artifact(project.name, missing_artifact_id)

    assert f"No artifact with id `{missing_artifact_id}`" in str(e)


# ------- Dataframes -------


@patch("pandas.DataFrame.to_parquet")
def test_persist_dataframe(mock_to_parquet, memory_repository):
    repository = memory_repository
    df = pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])
    path = "./local/root"

    # calls `BaseRepository._persist_dataframe` despite class using `MemoryRepository`
    super(MemoryRepository, repository)._persist_dataframe(df, path)

    mock_to_parquet.assert_called_once_with(
        f"{path}/data.parquet",
        engine="pyarrow",
        storage_options={},
    )


@patch("polars.DataFrame.write_parquet")
def test_persist_dataframe_polars(mock_write_parquet, memory_repository):
    repository = memory_repository
    df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    path = "./local/root"

    # calls `BaseRepository._persist_dataframe` despite class using `MemoryRepository`
    super(MemoryRepository, repository)._persist_dataframe(df, path)

    mock_write_parquet.assert_called_once_with(f"{path}")


@patch("pandas.read_parquet")
def test_read_dataframe(mock_read_parquet, memory_repository):
    repository = memory_repository
    path = "./local/root"

    # calls `BaseRepository._read_dataframe` despite class using `MemoryRepository`
    super(MemoryRepository, repository)._read_dataframe(path)

    mock_read_parquet.assert_called_once_with(
        f"{path}/data.parquet",
        engine="pyarrow",
        storage_options={},
    )


def test_read_dataframe_value_error(memory_repository):
    repository = memory_repository
    path = "./local/root"

    with pytest.raises(ValueError) as e:
        # calls `BaseRepository._read_dataframe` despite class using `MemoryRepository`
        super(MemoryRepository, repository)._read_dataframe(path, df_type="INVALID")

    assert "`df_type` must be one of " in str(e)


def test_read_dataframe_import_error(memory_repository):
    repository = memory_repository
    path = "./local/root"

    with patch.dict(sys.modules, {"dask": None}):
        with pytest.raises(RubiconException) as e:
            # calls `BaseRepository._read_dataframe` despite class using `MemoryRepository`
            super(MemoryRepository, repository)._read_dataframe(path, df_type="dask")

    assert "`rubicon_ml` requires `dask` to be installed" in str(e)


def test_get_dataframe_with_project_parent_root(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframe_root = repository._get_dataframe_metadata_root(project.name)

    assert dataframe_root == f"{repository.root_dir}/{slugify(project.name)}/dataframes"


def test_get_dataframe_with_experiment_parent_root(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    experiment = _create_experiment(repository, project=project)
    dataframe_root = repository._get_dataframe_metadata_root(project.name, experiment.id)

    assert (
        dataframe_root
        == f"{repository.root_dir}/{slugify(project.name)}/experiments/{experiment.id}/dataframes"
    )


def test_create_pandas_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframe = _create_pandas_dataframe(repository, project=project)

    dataframe_root = f"{repository.root_dir}/{slugify(project.name)}/dataframes/{dataframe.id}"
    dataframe_metadata_path = f"{dataframe_root}/metadata.json"
    dataframe_data_path = f"{dataframe_root}/data"

    open_file = repository.filesystem.open(dataframe_metadata_path)
    with open_file as f:
        dataframe_json = json.load(f)

    assert repository.filesystem.exists(dataframe_data_path)
    assert dataframe.id == dataframe_json["id"]


def test_create_pandas_multi_index_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframe = _create_pandas_dataframe(repository, project=project, multi_index=True)

    dataframe_root = f"{repository.root_dir}/{slugify(project.name)}/dataframes/{dataframe.id}"
    dataframe_metadata_path = f"{dataframe_root}/metadata.json"
    dataframe_data_path = f"{dataframe_root}/data"

    open_file = repository.filesystem.open(dataframe_metadata_path)
    with open_file as f:
        dataframe_json = json.load(f)

    assert repository.filesystem.exists(dataframe_data_path)
    assert dataframe.id == dataframe_json["id"]


def test_create_dask_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframe = _create_dask_dataframe(repository, project=project)

    dataframe_root = f"{repository.root_dir}/{slugify(project.name)}/dataframes/{dataframe.id}"
    dataframe_metadata_path = f"{dataframe_root}/metadata.json"
    dataframe_data_path = f"{dataframe_root}/data"

    open_file = repository.filesystem.open(dataframe_metadata_path)
    with open_file as f:
        dataframe_json = json.load(f)

    assert repository.filesystem.exists(dataframe_data_path)
    assert dataframe.id == dataframe_json["id"]


def test_create_polars_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframe = _create_polars_dataframe(repository, project=project)

    dataframe_root = f"{repository.root_dir}/{slugify(project.name)}/dataframes/{dataframe.id}"
    dataframe_metadata_path = f"{dataframe_root}/metadata.json"
    dataframe_data_path = f"{dataframe_root}/data"

    open_file = repository.filesystem.open(dataframe_metadata_path)
    with open_file as f:
        dataframe_json = json.load(f)

    assert repository.filesystem.exists(dataframe_data_path)
    assert dataframe.id == dataframe_json["id"]


def test_get_polars_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    written_dataframe = _create_polars_dataframe(repository, project=project)
    dataframe = repository.get_dataframe_metadata(project.name, written_dataframe.id)

    data = repository.get_dataframe_data(project.name, written_dataframe.id, df_type="polars")
    assert not data.is_empty()

    assert dataframe.id == written_dataframe.id
    assert dataframe.parent_id == written_dataframe.parent_id


def test_get_pandas_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    written_dataframe = _create_pandas_dataframe(repository, project=project)
    dataframe = repository.get_dataframe_metadata(project.name, written_dataframe.id)

    data = repository.get_dataframe_data(project.name, written_dataframe.id)
    assert not data.empty

    assert dataframe.id == written_dataframe.id
    assert dataframe.parent_id == written_dataframe.parent_id


def test_get_pandas_multi_index_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    written_dataframe = _create_pandas_dataframe(repository, project=project, multi_index=True)
    dataframe = repository.get_dataframe_metadata(project.name, written_dataframe.id)

    data = repository.get_dataframe_data(project.name, written_dataframe.id)
    assert not data.empty

    assert dataframe.id == written_dataframe.id
    assert dataframe.parent_id == written_dataframe.parent_id


def test_get_dask_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    written_dataframe = _create_dask_dataframe(repository, project=project)
    dataframe = repository.get_dataframe_metadata(project.name, written_dataframe.id)

    data = repository.get_dataframe_data(project.name, written_dataframe.id, df_type="dask")
    assert not data.compute().empty

    assert dataframe.id == written_dataframe.id
    assert dataframe.parent_id == written_dataframe.parent_id


def test_get_dataframe_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    missing_dataframe_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        repository.get_dataframe_metadata(project.name, missing_dataframe_id)

    assert f"No dataframe with id `{missing_dataframe_id}`" in str(e)


def test_get_dataframes(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    written_dataframes = [
        _create_pandas_dataframe(repository, project=project) for _ in range(0, 3)
    ]
    dataframes = repository.get_dataframes_metadata(project.name)

    dataframe_ids = [d.id for d in written_dataframes]
    for dataframe in dataframes:
        assert dataframe.id in dataframe_ids
        dataframe_ids.remove(dataframe.id)
    assert len(dataframes) == 3
    assert dataframes[0].created_at < dataframes[1].created_at
    assert dataframes[1].created_at < dataframes[2].created_at


def test_get_dataframes_no_results(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframes = repository.get_dataframes_metadata(project.name)

    assert dataframes == []


def test_get_dataframe_data(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframe_data = pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])
    dataframe_data = dd.from_pandas(dataframe_data, npartitions=1)

    dataframe = _create_pandas_dataframe(repository, project=project, dataframe_data=dataframe_data)
    data = repository.get_dataframe_data(project.name, dataframe.id)

    assert dataframe_data.compute().equals(data.compute())


def test_get_dataframe_data_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    missing_dataframe_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        repository.get_dataframe_data(project.name, missing_dataframe_id)

    assert f"No data for dataframe with id `{missing_dataframe_id}`" in str(e)


def test_delete_dataframe(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframe = _create_pandas_dataframe(repository, project=project)

    repository.delete_dataframe(project.name, dataframe.id)

    with pytest.raises(RubiconException) as e:
        repository.get_dataframe_metadata(project.name, dataframe.id)

    assert f"No dataframe with id `{dataframe.id}`" in str(e)


def test_delete_dataframes_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    missing_dataframe_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        repository.delete_dataframe(project.name, missing_dataframe_id)

    assert f"No dataframe with id `{missing_dataframe_id}`" in str(e)


# -------- Features --------


def test_create_feature(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    feature = _create_feature(repository, experiment=experiment)
    feature_dir = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/features/{slugify(feature.name)}"

    feature_metadata_path = f"{repository.root_dir}/{feature_dir}/metadata.json"
    open_file = repository.filesystem.open(feature_metadata_path)
    with open_file as f:
        feature_json = json.load(f)

    assert feature.id == feature_json["id"]
    assert feature.name == feature_json["name"]


def test_create_feature_throws_error_if_duplicate(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    feature = _create_feature(repository, experiment=experiment)

    with pytest.raises(RubiconException) as e:
        repository.create_feature(feature, experiment.project_name, experiment.id)

    assert f"'{feature.name}' already exists" in str(e)


def test_get_feature(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    written_feature = _create_feature(repository, experiment=experiment)
    feature = repository.get_feature(experiment.project_name, experiment.id, written_feature.name)

    assert feature.id == written_feature.id
    assert feature.name == written_feature.name


def test_get_feature_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    missing_feature_name = "missing feature"

    with pytest.raises(RubiconException) as e:
        repository.get_feature(experiment.project_name, experiment.id, missing_feature_name)

    assert f"No feature with name '{missing_feature_name}'" in str(e)


def test_get_features(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    written_features = [_create_feature(repository, experiment=experiment) for _ in range(0, 3)]
    features = repository.get_features(experiment.project_name, experiment.id)

    assert len(features) == 3

    feature_ids = [f.id for f in written_features]
    for feature in features:
        assert feature.id in feature_ids
        feature_ids.remove(feature.id)

    assert features[0].created_at < features[1].created_at
    assert features[1].created_at < features[2].created_at


def test_get_features_with_no_results(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    features = repository.get_features(experiment.project_name, experiment.id)

    assert features == []


# -------- Metrics ---------


def test_create_metric(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    metric = _create_metric(repository, experiment=experiment)
    metric_dir = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/metrics/{slugify(metric.name)}"

    metric_metadata_path = f"{repository.root_dir}/{metric_dir}/metadata.json"
    open_file = repository.filesystem.open(metric_metadata_path)
    with open_file as f:
        metric_json = json.load(f)

    assert metric.id == metric_json["id"]
    assert metric.name == metric_json["name"]


def test_create_metric_throws_error_if_duplicate(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    metric = _create_metric(repository, experiment=experiment)

    with pytest.raises(RubiconException) as e:
        repository.create_metric(metric, experiment.project_name, experiment.id)

    assert f"'{metric.name}' already exists" in str(e)


def test_get_metric(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    written_metric = _create_metric(repository, experiment=experiment)
    metric = repository.get_metric(experiment.project_name, experiment.id, written_metric.name)

    assert metric.id == written_metric.id
    assert metric.name == written_metric.name


def test_get_metric_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    missing_metric_name = "missing metric"

    with pytest.raises(RubiconException) as e:
        repository.get_metric(experiment.project_name, experiment.id, missing_metric_name)

    assert f"No metric with name '{missing_metric_name}'" in str(e)


def test_get_metrics(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    written_metrics = [_create_metric(repository, experiment=experiment) for _ in range(0, 3)]
    metrics = repository.get_metrics(experiment.project_name, experiment.id)

    assert len(metrics) == 3

    metric_ids = [m.id for m in written_metrics]
    for metric in metrics:
        assert metric.id in metric_ids
        metric_ids.remove(metric.id)
    assert metrics[0].created_at < metrics[1].created_at
    assert metrics[1].created_at < metrics[2].created_at


def test_get_metrics_with_no_results(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    metrics = repository.get_metrics(experiment.project_name, experiment.id)

    assert metrics == []


# ------- Parameters -------


def test_create_parameter(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    parameter = _create_parameter(repository, experiment=experiment)
    parameter_dir = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/parameters/{slugify(parameter.name)}"

    parameter_metadata_path = f"{repository.root_dir}/{parameter_dir}/metadata.json"
    open_file = repository.filesystem.open(parameter_metadata_path)
    with open_file as f:
        parameter_json = json.load(f)

    assert parameter.id == parameter_json["id"]
    assert parameter.name == parameter_json["name"]


def test_create_parameter_throws_error_if_duplicate(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    parameter = _create_parameter(repository, experiment=experiment)

    with pytest.raises(RubiconException) as e:
        repository.create_parameter(parameter, experiment.project_name, experiment.id)

    assert f"'{parameter.name}' already exists" in str(e)


def test_get_parameter(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    written_parameter = _create_parameter(repository, experiment=experiment)
    parameter = repository.get_parameter(
        experiment.project_name, experiment.id, written_parameter.name
    )

    assert parameter.id == written_parameter.id
    assert parameter.name == written_parameter.name


def test_get_parameter_throws_error_if_not_found(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    missing_parameter_name = "missing parameter"

    with pytest.raises(RubiconException) as e:
        repository.get_parameter(experiment.project_name, experiment.id, missing_parameter_name)

    assert f"No parameter with name '{missing_parameter_name}'" in str(e)


def test_get_parameters(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    written_parameters = [_create_parameter(repository, experiment=experiment) for _ in range(0, 3)]
    parameters = repository.get_parameters(experiment.project_name, experiment.id)

    assert len(parameters) == 3

    parameter_ids = [p.id for p in written_parameters]
    for parameter in parameters:
        assert parameter.id in parameter_ids
        parameter_ids.remove(parameter.id)
    assert parameters[0].created_at < parameters[1].created_at
    assert parameters[1].created_at < parameters[2].created_at


def test_get_parameters_with_no_results(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    parameters = repository.get_parameters(experiment.project_name, experiment.id)

    assert parameters == []


# ---------- Tags ----------


def test_get_experiment_tags_root(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    experiment_tags_root = repository._get_tag_metadata_root(
        experiment.project_name,
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    assert (
        experiment_tags_root
        == f"{repository.root_dir}/{slugify(experiment.project_name)}/experiments/{experiment.id}"
    )


def test_get_dataframe_tags_with_project_parent_root(memory_repository):
    repository = memory_repository
    project = _create_project(repository)
    dataframe = _create_pandas_dataframe(repository, project=project)
    dataframe_tags_root = repository._get_tag_metadata_root(
        project.name,
        entity_identifier=dataframe.id,
        entity_type=dataframe.__class__.__name__,
    )

    assert (
        dataframe_tags_root
        == f"{repository.root_dir}/{slugify(project.name)}/dataframes/{dataframe.id}"
    )


def test_get_dataframe_tags_with_experiment_parent_root(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    dataframe = domain.Dataframe(parent_id=experiment.id)
    dataframe_data = pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])

    repository.create_dataframe(dataframe, dataframe_data, experiment.project_name, experiment.id)

    dataframe_tags_root = repository._get_tag_metadata_root(
        experiment.project_name,
        experiment_id=experiment.id,
        entity_identifier=dataframe.id,
        entity_type=dataframe.__class__.__name__,
    )

    assert (
        dataframe_tags_root
        == f"{repository.root_dir}/{slugify(experiment.project_name)}/"
        + f"experiments/{experiment.id}/dataframes/{dataframe.id}"
    )


def test_get_root_without_experiment_or_dataframe_throws_error(memory_repository):
    repository = memory_repository
    project = _create_project(repository)

    with pytest.raises(ValueError) as e:
        repository._get_tag_metadata_root(project.name)

    assert "`experiment_id` and `entity_identifier` can not both be `None`" in str(e)


def test_add_tags(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    repository.add_tags(
        experiment.project_name,
        ["wow"],
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    tags_glob = f"{repository.root_dir}/{slugify(experiment.project_name)}/experiments/{experiment.id}/tags_*.json"
    tags_files = repository.filesystem.glob(tags_glob)

    assert len(tags_files) == 1

    open_file = repository.filesystem.open(tags_files[0])
    with open_file as f:
        tags_json = json.load(f)

    assert ["wow"] == tags_json["added_tags"]


def test_remove_tags(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository, tags=["wow"])
    repository.remove_tags(
        experiment.project_name,
        ["wow"],
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    tags_glob = f"{repository.root_dir}/{slugify(experiment.project_name)}/experiments/{experiment.id}/tags_*.json"
    tags_files = repository.filesystem.glob(tags_glob)

    assert len(tags_files) == 1

    open_file = repository.filesystem.open(tags_files[0])
    with open_file as f:
        tags_json = json.load(f)

    assert ["wow"] == tags_json["removed_tags"]


def test_get_tags(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository, tags=["wow"])
    repository.add_tags(
        experiment.project_name,
        ["cool"],
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )
    repository.remove_tags(
        experiment.project_name,
        ["wow"],
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    tags = repository.get_tags(
        experiment.project_name,
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    assert {"added_tags": ["cool"]} in tags
    assert {"removed_tags": ["wow"]} in tags


def test_get_tags_with_no_results(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)

    tags = repository.get_tags(
        experiment.project_name,
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    assert tags == []


def test_add_comments(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    repository.add_comments(
        experiment.project_name,
        ["this is a comment"],
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    comments_glob = f"{repository.root_dir}/{slugify(experiment.project_name)}/experiments/{experiment.id}/comments_*.json"
    comments_files = repository.filesystem.glob(comments_glob)

    assert len(comments_files) == 1

    open_file = repository.filesystem.open(comments_files[0])
    with open_file as f:
        comments_json = json.load(f)

    assert ["this is a comment"] == comments_json["added_comments"]


def test_remove_comments(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository, comments=["this is a comment"])
    repository.remove_comments(
        experiment.project_name,
        ["this is a comment"],
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    comments_glob = f"{repository.root_dir}/{slugify(experiment.project_name)}/experiments/{experiment.id}/comments_*.json"
    comments_files = repository.filesystem.glob(comments_glob)

    assert len(comments_files) == 1

    open_file = repository.filesystem.open(comments_files[0])
    with open_file as f:
        comments_json = json.load(f)

    assert ["this is a comment"] == comments_json["removed_comments"]


def test_get_comments(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)
    repository.add_comments(
        experiment.project_name,
        ["this is a comment"],
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    comments = repository.get_comments(
        experiment.project_name,
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    assert {"added_comments": ["this is a comment"]} in comments


def test_get_comments_with_no_results(memory_repository):
    repository = memory_repository
    experiment = _create_experiment(repository)

    comments = repository.get_comments(
        experiment.project_name,
        experiment_id=experiment.id,
        entity_type=experiment.__class__.__name__,
    )

    assert comments == []
