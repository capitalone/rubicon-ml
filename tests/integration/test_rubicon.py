import uuid

import pandas as pd
import pytest

from rubicon_ml import Rubicon

filesystems = [
    pytest.param(Rubicon(persistence="memory")),
    pytest.param(
        Rubicon(persistence="filesystem", root_dir="./test-rubicon"),
        marks=pytest.mark.write_files,
    ),
    pytest.param(
        Rubicon(persistence="filesystem", root_dir="s3://change-me"),
        marks=pytest.mark.write_files,
    ),
    pytest.param(
        Rubicon(
            composite_config=[
                {"persistence": "memory", "root_dir": "./memory/root"},
                {"persistence": "filesystem", "root_dir": "./test-rubicon"},
            ]
        ),
        marks=pytest.mark.write_files,
    ),
]


@pytest.mark.parametrize("rubicon", filesystems)
def test_rubicon(rubicon, request):
    for repository in rubicon.repositories:
        if "change-me" in repository.root_dir:
            root_dir = request.config.getoption("s3-path")

            if root_dir is None:
                pytest.fail("`root_dir` cannot be None. Run `pytest` with `--s3-path`.")

            repository.root_dir = root_dir

    written_project = rubicon.create_project(name=f"Test Project {uuid.uuid4()}")
    written_experiment = written_project.log_experiment(name=f"Test Experiment {uuid.uuid4()}")

    written_experiment.add_tags(["x", "y"])
    written_experiment.remove_tags(["x"])

    written_feature = written_experiment.log_feature(name=f"Test Feature {uuid.uuid4()}")
    written_parameter = written_experiment.log_parameter(
        name=f"Test Parameter {uuid.uuid4()}", value=8
    )
    written_metric = written_experiment.log_metric(name=f"Test Feature {uuid.uuid4()}", value=24)

    written_project_artifact = written_project.log_artifact(
        name=f"Test Artifact {uuid.uuid4()}", data_bytes=b"test artifact data"
    )
    written_experiment_artifact = written_experiment.log_artifact(
        name=f"Test Artifact {uuid.uuid4()}", data_bytes=b"test artifact data"
    )

    written_project_dataframe = written_project.log_dataframe(
        df=pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])
    )

    json_dict = {"hello": "world", "numbers": [1, 2, 3]}

    written_project_json = written_project.log_json(
        name=f"Test JSON {uuid.uuid4()}.json", json_object=json_dict
    )
    written_experiment_json = written_experiment.log_json(
        name=f"Test JSON {uuid.uuid4()}.json", json_object=json_dict
    )

    written_project_dataframe.add_tags(["x", "y"])
    written_project_dataframe.remove_tags(["x"])

    read_project = rubicon.get_project(name=written_project.name)
    assert written_project.id == read_project.id

    read_experiments = read_project.experiments()
    assert len(read_experiments) == 1
    assert written_experiment.id == read_experiments[0].id

    read_experiment = read_experiments[0]
    assert written_experiment.tags == read_experiment.tags

    read_features = read_experiment.features()
    assert len(read_features) == 1
    assert written_feature.id == read_features[0].id

    read_parameters = read_experiment.parameters()
    assert len(read_parameters) == 1
    assert written_parameter.id == read_parameters[0].id
    assert written_parameter.value == read_parameters[0].value

    read_metrics = read_experiment.metrics()
    assert len(read_metrics) == 1
    assert written_metric.id == read_metrics[0].id
    assert written_metric.value == read_metrics[0].value

    read_project_artifacts = read_project.artifacts()
    assert len(read_project_artifacts) == 2
    assert written_project_artifact.id == read_project_artifacts[0].id
    assert written_project_artifact.data == read_project_artifacts[0].data
    assert written_project_json.id == read_project_artifacts[1].id
    assert written_project_json.data == read_project_artifacts[1].data

    read_project.delete_artifacts([artifact.id for artifact in read_project_artifacts])
    assert len(read_project.artifacts()) == 0

    read_experiment_artifacts = read_experiment.artifacts()
    assert len(read_experiment_artifacts) == 2
    assert written_experiment_artifact.id == read_experiment_artifacts[0].id
    assert written_experiment_artifact.data == read_experiment_artifacts[0].data
    assert written_experiment_json.id == read_experiment_artifacts[1].id
    assert written_experiment_json.data == read_experiment_artifacts[1].data
    assert json_dict == read_experiment_artifacts[1].get_json()

    read_project_dataframes = read_project.dataframes()
    assert len(read_project_dataframes) == 1
    assert written_project_dataframe.id == read_project_dataframes[0].id
    assert written_project_dataframe.get_data().equals(read_project_dataframes[0].get_data())
    assert written_project_dataframe.tags == read_project_dataframes[0].tags

    read_project.delete_dataframes([read_project_dataframes[0].id])
    assert len(read_project.dataframes()) == 0

    for repository in rubicon.repositories:
        repository.filesystem.rm(repository.root_dir, recursive=True)
