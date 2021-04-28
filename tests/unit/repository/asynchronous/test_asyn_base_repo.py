import asyncio
import uuid
from unittest.mock import ANY, MagicMock, call

import pytest
from dask import dataframe as dd

from rubicon import domain
from rubicon.exceptions import RubiconException
from rubicon.repository.utils import json, slugify

# -------- Projects --------


def test_create_project(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._exists.return_value = False

    project = domain.Project(f"Test Project {uuid.uuid4()}")

    project_dir = slugify(project.name)
    project_metadata_path = f"{asyn_repo_w_mock_filesystem.root_dir}/{project_dir}/metadata.json"

    asyncio.run(asyn_repo_w_mock_filesystem.create_project(project))

    filesystem_expected = [call._exists(project_metadata_path), call.invalidate_cache()]
    repo_expected = [call._persist_domain(project, project_metadata_path)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == repo_expected


def test_create_project_throws_error_if_duplicate(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._exists.return_value = True

    project = domain.Project(f"Test Project {uuid.uuid4()}")

    with pytest.raises(RubiconException) as e:
        asyncio.run(asyn_repo_w_mock_filesystem.create_project(domain.Project(project.name)))

    assert f"'{project.name}' already exists" in str(e)


def test_get_project(asyn_repo_w_mock_filesystem):
    written_project = domain.Project(f"Test Project {uuid.uuid4()}")
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = json.dumps(written_project)

    project = asyncio.run(asyn_repo_w_mock_filesystem.get_project(written_project.name))

    assert project.id == written_project.id
    assert project.name == written_project.name


def test_get_project_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = FileNotFoundError()

    project_name = f"Test Project {uuid.uuid4()}"

    with pytest.raises(RubiconException) as e:
        asyncio.run(asyn_repo_w_mock_filesystem.get_project(project_name))

    assert "No project with name 'Test Project " in str(e)


def test_get_projects(asyn_repo_w_mock_filesystem):
    written_projects = [domain.Project(f"Test Project {uuid.uuid4()}") for _ in range(0, 3)]

    project_dirs = [
        f"{asyn_repo_w_mock_filesystem.root_dir}/{slugify(p.name)}" for p in written_projects
    ]

    asyn_repo_w_mock_filesystem.filesystem._ls.return_value = [
        {"name": path, "StorageClass": "DIRECTORY"} for path in project_dirs
    ]
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = [
        json.dumps(e) for e in written_projects
    ]

    projects = asyncio.run(asyn_repo_w_mock_filesystem.get_projects())

    assert len(projects) == 3

    project_ids = [p.id for p in written_projects]
    for project in projects:
        assert project.id in project_ids
        project_ids.remove(project.id)


def test_get_projects_with_no_results(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._ls.side_effect = FileNotFoundError()

    projects = asyncio.run(asyn_repo_w_mock_filesystem.get_projects())

    assert projects == []


# ------ Experiments -------


def _create_experiment_domain(project=None, tags=[]):
    if project is None:
        project = domain.Project(f"Test Project {uuid.uuid4()}")

    return domain.Experiment(
        name=f"Test Experiment {uuid.uuid4()}", project_name=project.name, tags=[]
    )


def test_create_experiment(asyn_repo_w_mock_filesystem):
    experiment = _create_experiment_domain()

    experiment_dir = f"{slugify(experiment.project_name)}/experiments/{experiment.id}"
    experiment_metadata_path = (
        f"{asyn_repo_w_mock_filesystem.root_dir}/{experiment_dir}/metadata.json"
    )

    asyncio.run(asyn_repo_w_mock_filesystem.create_experiment(experiment))

    filesystem_expected = [call.invalidate_cache()]
    repo_expected = [call._persist_domain(experiment, experiment_metadata_path)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == repo_expected


def test_get_experiment(asyn_repo_w_mock_filesystem):
    written_experiment = _create_experiment_domain()
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = json.dumps(written_experiment)

    experiment = asyncio.run(
        asyn_repo_w_mock_filesystem.get_experiment(
            written_experiment.project_name, written_experiment.id
        )
    )

    assert experiment.id == written_experiment.id
    assert experiment.name == written_experiment.name
    assert experiment.project_name == written_experiment.project_name


def test_get_experiment_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    missing_experiment_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        asyncio.run(asyn_repo_w_mock_filesystem.get_experiment(project.name, missing_experiment_id))

    assert f"No experiment with id `{missing_experiment_id}`" in str(e)


def test_get_experiments(asyn_repo_w_mock_filesystem):
    project = domain.Project(f"Test Project {uuid.uuid4()}")
    written_experiments = [_create_experiment_domain(project=project) for _ in range(0, 3)]

    experiment_dirs = [
        f"{asyn_repo_w_mock_filesystem.root_dir}/{slugify(e.project_name)}/experiments/{e.id}"
        for e in written_experiments
    ]

    asyn_repo_w_mock_filesystem.filesystem._ls.return_value = [
        {"name": path, "StorageClass": "DIRECTORY"} for path in experiment_dirs
    ]
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = [
        json.dumps(e) for e in written_experiments
    ]

    experiments = asyncio.run(asyn_repo_w_mock_filesystem.get_experiments(project.name))

    assert len(experiments) == 3

    experiment_ids = [e.id for e in written_experiments]
    for experiment in experiments:
        assert experiment.id in experiment_ids
        experiment_ids.remove(experiment.id)


def test_get_experiments_with_no_results(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._ls.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    experiments = asyncio.run(asyn_repo_w_mock_filesystem.get_experiments(project.name))

    assert experiments == []


# -------- Features --------


def _create_feature_domain(experiment=None):
    if experiment is None:
        project = domain.Project(f"Test Project {uuid.uuid4()}")
        experiment = _create_experiment_domain(project)

    return experiment, domain.Feature(name="test feature")


def test_create_feature(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._exists.return_value = False

    experiment, feature = _create_feature_domain()

    feature_dir = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/features/{slugify(feature.name)}"
    feature_metadata_path = f"{asyn_repo_w_mock_filesystem.root_dir}/{feature_dir}/metadata.json"

    asyncio.run(
        asyn_repo_w_mock_filesystem.create_feature(feature, experiment.project_name, experiment.id)
    )

    filesystem_expected = [call._exists(feature_metadata_path), call.invalidate_cache()]
    repo_expected = [call._persist_domain(feature, feature_metadata_path)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == repo_expected


def test_create_feature_throws_error_if_duplicate(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._exists.return_value = True

    experiment, feature = _create_feature_domain()

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.create_feature(
                feature, experiment.project_name, experiment.id
            )
        )

    assert f"'{feature.name}' already exists" in str(e)


def test_get_feature(asyn_repo_w_mock_filesystem):
    experiment, written_feature = _create_feature_domain()
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = json.dumps(written_feature)

    feature = asyncio.run(
        asyn_repo_w_mock_filesystem.get_feature(
            experiment.project_name, experiment.id, written_feature.name
        )
    )

    assert feature.id == written_feature.id
    assert feature.name == written_feature.name


def test_get_feature_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = FileNotFoundError()

    experiment = _create_experiment_domain()
    missing_feature_name = "missing feature name"

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.get_feature(
                experiment.project_name, experiment.id, missing_feature_name
            )
        )

    assert f"No feature with name '{missing_feature_name}'" in str(e)


def test_get_features(asyn_repo_w_mock_filesystem):
    experiment = _create_experiment_domain()
    written_features = [_create_feature_domain(experiment=experiment)[1] for _ in range(0, 3)]

    feature_root = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/features"
    feature_dirs = [
        f"{asyn_repo_w_mock_filesystem.root_dir}/{feature_root}/{f.id}" for f in written_features
    ]

    asyn_repo_w_mock_filesystem.filesystem._ls.return_value = [
        {"name": path, "StorageClass": "DIRECTORY"} for path in feature_dirs
    ]
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = [
        json.dumps(f) for f in written_features
    ]

    features = asyncio.run(
        asyn_repo_w_mock_filesystem.get_features(experiment.project_name, experiment.id)
    )

    assert len(features) == 3

    feature_ids = [f.id for f in written_features]
    for feature in features:
        assert feature.id in feature_ids
        feature_ids.remove(feature.id)


def test_get_features_with_no_results(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._ls.side_effect = FileNotFoundError()

    experiment = _create_experiment_domain()
    features = asyncio.run(
        asyn_repo_w_mock_filesystem.get_features(experiment.project_name, experiment.id)
    )

    assert features == []


# ------- Parameters -------


def _create_parameter_domain(experiment=None):
    if experiment is None:
        project = domain.Project(f"Test Project {uuid.uuid4()}")
        experiment = _create_experiment_domain(project)

    return experiment, domain.Parameter(name="test parameter", value=0)


def test_create_parameter(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._exists.return_value = False

    experiment, parameter = _create_parameter_domain()

    parameter_dir = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/parameters/{slugify(parameter.name)}"
    parameter_metadata_path = (
        f"{asyn_repo_w_mock_filesystem.root_dir}/{parameter_dir}/metadata.json"
    )

    asyncio.run(
        asyn_repo_w_mock_filesystem.create_parameter(
            parameter, experiment.project_name, experiment.id
        )
    )

    filesystem_expected = [call._exists(parameter_metadata_path), call.invalidate_cache()]
    repo_expected = [call._persist_domain(parameter, parameter_metadata_path)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == repo_expected


def test_create_parameter_throws_error_if_duplicate(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._exists.return_value = True

    experiment, parameter = _create_parameter_domain()

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.create_parameter(
                parameter, experiment.project_name, experiment.id
            )
        )

    assert f"'{parameter.name}' already exists" in str(e)


def test_get_parameter(asyn_repo_w_mock_filesystem):
    experiment, written_parameter = _create_parameter_domain()
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = json.dumps(written_parameter)

    parameter = asyncio.run(
        asyn_repo_w_mock_filesystem.get_parameter(
            experiment.project_name, experiment.id, written_parameter.name
        )
    )

    assert parameter.id == written_parameter.id
    assert parameter.name == written_parameter.name


def test_get_parameter_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = FileNotFoundError()

    experiment = _create_experiment_domain()
    missing_parameter_name = "missing parameter name"

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.get_parameter(
                experiment.project_name, experiment.id, missing_parameter_name
            )
        )

    assert f"No parameter with name '{missing_parameter_name}'" in str(e)


def test_get_parameters(asyn_repo_w_mock_filesystem):
    experiment = _create_experiment_domain()
    written_parameters = [_create_parameter_domain(experiment=experiment)[1] for _ in range(0, 3)]

    parameter_root = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/parameters"
    parameter_dirs = [
        f"{asyn_repo_w_mock_filesystem.root_dir}/{parameter_root}/{p.id}"
        for p in written_parameters
    ]

    asyn_repo_w_mock_filesystem.filesystem._ls.return_value = [
        {"name": path, "StorageClass": "DIRECTORY"} for path in parameter_dirs
    ]
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = [
        json.dumps(p) for p in written_parameters
    ]

    parameters = asyncio.run(
        asyn_repo_w_mock_filesystem.get_parameters(experiment.project_name, experiment.id)
    )

    assert len(parameters) == 3

    parameter_ids = [p.id for p in written_parameters]
    for parameter in parameters:
        assert parameter.id in parameter_ids
        parameter_ids.remove(parameter.id)


def test_get_parameters_with_no_results(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._ls.side_effect = FileNotFoundError()

    experiment = _create_experiment_domain()
    parameters = asyncio.run(
        asyn_repo_w_mock_filesystem.get_parameters(experiment.project_name, experiment.id)
    )

    assert parameters == []


# -------- Metrics ---------


def _create_metric_domain(experiment=None):
    if experiment is None:
        project = domain.Project(f"Test Project {uuid.uuid4()}")
        experiment = _create_experiment_domain(project)

    return experiment, domain.Metric(name="test metric", value=0)


def test_create_metric(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._exists.return_value = False

    experiment, metric = _create_metric_domain()

    metric_dir = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/metrics/{slugify(metric.name)}"
    metric_metadata_path = f"{asyn_repo_w_mock_filesystem.root_dir}/{metric_dir}/metadata.json"

    asyncio.run(
        asyn_repo_w_mock_filesystem.create_metric(metric, experiment.project_name, experiment.id)
    )

    filesystem_expected = [call._exists(metric_metadata_path), call.invalidate_cache()]
    repo_expected = [call._persist_domain(metric, metric_metadata_path)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == repo_expected


def test_create_metric_throws_error_if_duplicate(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._exists.return_value = True

    experiment, metric = _create_metric_domain()

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.create_metric(
                metric, experiment.project_name, experiment.id
            )
        )

    assert f"'{metric.name}' already exists" in str(e)


def test_get_metric(asyn_repo_w_mock_filesystem):
    experiment, written_metric = _create_metric_domain()
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = json.dumps(written_metric)

    metric = asyncio.run(
        asyn_repo_w_mock_filesystem.get_metric(
            experiment.project_name, experiment.id, written_metric.name
        )
    )

    assert metric.id == written_metric.id
    assert metric.name == written_metric.name


def test_get_metric_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = FileNotFoundError()

    experiment = _create_experiment_domain()
    missing_metric_name = "missing metric name"

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.get_metric(
                experiment.project_name, experiment.id, missing_metric_name
            )
        )

    assert f"No metric with name '{missing_metric_name}'" in str(e)


def test_get_metrics(asyn_repo_w_mock_filesystem):
    experiment = _create_experiment_domain()
    written_metrics = [_create_metric_domain(experiment=experiment)[1] for _ in range(0, 3)]

    metric_root = f"{slugify(experiment.project_name)}/experiments/{experiment.id}/metrics"
    metric_dirs = [
        f"{asyn_repo_w_mock_filesystem.root_dir}/{metric_root}/{m.id}" for m in written_metrics
    ]

    asyn_repo_w_mock_filesystem.filesystem._ls.return_value = [
        {"name": path, "StorageClass": "DIRECTORY"} for path in metric_dirs
    ]
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = [
        json.dumps(m) for m in written_metrics
    ]

    metrics = asyncio.run(
        asyn_repo_w_mock_filesystem.get_metrics(experiment.project_name, experiment.id)
    )

    assert len(metrics) == 3

    metric_ids = [m.id for m in written_metrics]
    for metric in metrics:
        assert metric.id in metric_ids
        metric_ids.remove(metric.id)


def test_get_metrics_with_no_results(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._ls.side_effect = FileNotFoundError()

    experiment = _create_experiment_domain()
    metrics = asyncio.run(
        asyn_repo_w_mock_filesystem.get_metrics(experiment.project_name, experiment.id)
    )

    assert metrics == []


# ------- Artifacts --------


def _create_artifact_domain(project=None, tags=[]):
    if project is None:
        project = domain.Project(f"Test Project {uuid.uuid4()}")

    return project, domain.Artifact(name=f"Test Artifact {uuid.uuid4()}", parent_id=project.id)


def test_create_artifact(asyn_repo_w_mock_filesystem):
    project, artifact = _create_artifact_domain()

    artifact_dir = f"{slugify(project.name)}/artifacts/{artifact.id}"
    artifact_metadata_path = f"{asyn_repo_w_mock_filesystem.root_dir}/{artifact_dir}/metadata.json"
    artifact_data_path = f"{asyn_repo_w_mock_filesystem.root_dir}/{artifact_dir}/data"

    data = b"test artifact data"
    asyncio.run(asyn_repo_w_mock_filesystem.create_artifact(artifact, data, project.name))

    filesystem_expected = [call.invalidate_cache()]
    persist_domain_expected = [call._persist_domain(artifact, artifact_metadata_path)]
    persist_bytes_expected = [call._persist_bytes(data, artifact_data_path)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == persist_domain_expected
    assert asyn_repo_w_mock_filesystem._persist_bytes.mock_calls == persist_bytes_expected


def test_get_artifact_metadata(asyn_repo_w_mock_filesystem):
    project, written_artifact = _create_artifact_domain()
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = json.dumps(written_artifact)

    artifact = asyncio.run(
        asyn_repo_w_mock_filesystem.get_artifact_metadata(project.name, written_artifact.id)
    )

    assert artifact.id == written_artifact.id
    assert project.id == written_artifact.parent_id


def test_get_artifact_metadata_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    missing_artifact_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.get_artifact_metadata(project.name, missing_artifact_id)
        )

    assert f"No artifact with id `{missing_artifact_id}`" in str(e)


def test_get_artifacts_metadata(asyn_repo_w_mock_filesystem):
    project = domain.Project(f"Test Project {uuid.uuid4()}")
    written_artifacts = [_create_artifact_domain(project=project)[1] for _ in range(0, 3)]

    artifact_dirs = [
        f"{asyn_repo_w_mock_filesystem.root_dir}/{slugify(project.name)}/artifacts/{a.id}"
        for a in written_artifacts
    ]

    asyn_repo_w_mock_filesystem.filesystem._ls.return_value = [
        {"name": path, "StorageClass": "DIRECTORY"} for path in artifact_dirs
    ]
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = [
        json.dumps(a) for a in written_artifacts
    ]

    artifacts = asyncio.run(asyn_repo_w_mock_filesystem.get_artifacts_metadata(project.name))

    assert len(artifacts) == 3

    artifact_ids = [a.id for a in written_artifacts]
    for artifact in artifacts:
        assert artifact.id in artifact_ids
        artifact_ids.remove(artifact.id)


def test_get_artifacts_metadata_with_no_results(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._ls.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    artifacts = asyncio.run(asyn_repo_w_mock_filesystem.get_artifacts_metadata(project.name))

    assert artifacts == []


def test_get_artifact_data(asyn_repo_w_mock_filesystem):
    written_artifact_data = b"test artifact data"
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = written_artifact_data

    artifact_data = asyncio.run(
        asyn_repo_w_mock_filesystem.get_artifact_data("mock project", "mock artifact id")
    )

    assert artifact_data == written_artifact_data


def test_get_artifact_data_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    missing_artifact_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.get_artifact_data(project.name, missing_artifact_id)
        )

    assert f"No data for artifact with id `{missing_artifact_id}`" in str(e)


def test_delete_artifact(asyn_repo_w_mock_filesystem):
    project, artifact = _create_artifact_domain()

    artifact_dir = (
        f"{asyn_repo_w_mock_filesystem.root_dir}/{slugify(project.name)}/artifacts/{artifact.id}"
    )

    asyncio.run(asyn_repo_w_mock_filesystem.delete_artifact(project.name, artifact.id))

    filesystem_expected = [call._rm(artifact_dir, recursive=True), call.invalidate_cache()]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected


def test_delete_artifact_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._rm.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    missing_artifact_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        asyncio.run(asyn_repo_w_mock_filesystem.delete_artifact(project.name, missing_artifact_id))

    assert f"No artifact with id `{missing_artifact_id}`" in str(e)


# ------- Dataframes -------


def _create_dataframe_domain(project=None, tags=[]):
    if project is None:
        project = domain.Project(f"Test Project {uuid.uuid4()}")

    return project, domain.Dataframe(parent_id=project.id)


def test_create_dataframe(asyn_repo_w_mock_filesystem):
    project, dataframe = _create_dataframe_domain()

    dataframe_dir = f"{slugify(project.name)}/dataframes/{dataframe.id}"
    dataframe_metadata_path = (
        f"{asyn_repo_w_mock_filesystem.root_dir}/{dataframe_dir}/metadata.json"
    )

    mock_dataframe = MagicMock(spec=dd.DataFrame)
    asyncio.run(
        asyn_repo_w_mock_filesystem.create_dataframe(dataframe, mock_dataframe, project.name)
    )

    filesystem_expected = [call.invalidate_cache()]
    repo_expected = [call._persist_domain(dataframe, dataframe_metadata_path)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == repo_expected


def test_get_dataframe_metadata(asyn_repo_w_mock_filesystem):
    project, written_dataframe = _create_dataframe_domain()
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = json.dumps(written_dataframe)

    dataframe = asyncio.run(
        asyn_repo_w_mock_filesystem.get_dataframe_metadata(project.name, written_dataframe.id)
    )

    assert dataframe.id == written_dataframe.id
    assert project.id == written_dataframe.parent_id


def test_get_dataframe_metadata_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    missing_dataframe_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.get_dataframe_metadata(project.name, missing_dataframe_id)
        )

    assert f"No dataframe with id `{missing_dataframe_id}`" in str(e)


def test_get_dataframes_metadata(asyn_repo_w_mock_filesystem):
    project = domain.Project(f"Test Project {uuid.uuid4()}")
    written_dataframes = [_create_dataframe_domain(project=project)[1] for _ in range(0, 3)]

    dataframe_dirs = [
        f"{asyn_repo_w_mock_filesystem.root_dir}/{slugify(project.name)}/dataframes/{d.id}"
        for d in written_dataframes
    ]

    asyn_repo_w_mock_filesystem.filesystem._ls.return_value = [
        {"name": path, "StorageClass": "DIRECTORY"} for path in dataframe_dirs
    ]
    asyn_repo_w_mock_filesystem.filesystem._cat_file.side_effect = [
        json.dumps(d) for d in written_dataframes
    ]

    dataframes = asyncio.run(asyn_repo_w_mock_filesystem.get_dataframes_metadata(project.name))

    assert len(dataframes) == 3

    dataframe_ids = [d.id for d in written_dataframes]
    for dataframe in dataframes:
        assert dataframe.id in dataframe_ids
        dataframe_ids.remove(dataframe.id)


def test_get_dataframes_metadata_with_no_results(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._ls.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    dataframes = asyncio.run(asyn_repo_w_mock_filesystem.get_dataframes_metadata(project.name))

    assert dataframes == []


def test_delete_dataframe(asyn_repo_w_mock_filesystem):
    project, dataframe = _create_dataframe_domain()

    dataframe_dir = (
        f"{asyn_repo_w_mock_filesystem.root_dir}/{slugify(project.name)}/dataframes/{dataframe.id}"
    )

    asyncio.run(asyn_repo_w_mock_filesystem.delete_dataframe(project.name, dataframe.id))

    filesystem_expected = [call._rm(dataframe_dir, recursive=True), call.invalidate_cache()]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected


def test_delete_dataframe_throws_error_if_not_found(asyn_repo_w_mock_filesystem):
    asyn_repo_w_mock_filesystem.filesystem._rm.side_effect = FileNotFoundError()

    project = domain.Project(f"Test Project {uuid.uuid4()}")
    missing_dataframe_id = uuid.uuid4()

    with pytest.raises(RubiconException) as e:
        asyncio.run(
            asyn_repo_w_mock_filesystem.delete_dataframe(project.name, missing_dataframe_id)
        )

    assert f"No dataframe with id `{missing_dataframe_id}`" in str(e)


# ---------- Tags ----------


def test_add_tags(asyn_repo_w_mock_filesystem):
    experiment = _create_experiment_domain()

    tags = ["x"]
    asyncio.run(
        asyn_repo_w_mock_filesystem.add_tags(
            experiment.project_name, tags, experiment_id=experiment.id
        )
    )

    filesystem_expected = [call.invalidate_cache()]
    repo_expected = [call({"added_tags": tags}, ANY)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == repo_expected


def test_remove_tags(asyn_repo_w_mock_filesystem):
    experiment = _create_experiment_domain()

    tags = ["x"]
    asyncio.run(
        asyn_repo_w_mock_filesystem.remove_tags(
            experiment.project_name, tags, experiment_id=experiment.id
        )
    )

    filesystem_expected = [call.invalidate_cache()]
    repo_expected = [call({"removed_tags": tags}, ANY)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert asyn_repo_w_mock_filesystem._persist_domain.mock_calls == repo_expected


def test_get_tags(asyn_repo_w_mock_filesystem):
    tags = ["x"]
    experiment = _create_experiment_domain(tags=tags)

    asyn_repo_w_mock_filesystem.filesystem._lsdir.return_value = [
        {"name": "test/tags_", "LastModified": 0}
    ]
    asyn_repo_w_mock_filesystem.filesystem._cat_file.return_value = '{"test":"test"}'

    asyncio.run(
        asyn_repo_w_mock_filesystem.get_tags(experiment.project_name, experiment_id=experiment.id)
    )

    filesystem_expected = [call._lsdir(ANY), call._cat_file(ANY)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert tags == tags


def test_get_tags_with_no_results(asyn_repo_w_mock_filesystem):
    experiment = _create_experiment_domain(tags=["x"])

    asyn_repo_w_mock_filesystem.filesystem._lsdir.return_value = []

    tags = asyncio.run(
        asyn_repo_w_mock_filesystem.get_tags(experiment.project_name, experiment_id=experiment.id)
    )

    filesystem_expected = [call._lsdir(ANY)]

    assert asyn_repo_w_mock_filesystem.filesystem.mock_calls == filesystem_expected
    assert tags == []
