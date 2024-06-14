import numpy as np
import pandas as pd
from prefect import flow

from rubicon_ml import Rubicon
from rubicon_ml.client import (
    Artifact,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
)
from rubicon_ml.workflow.prefect import (
    create_experiment_task,
    get_or_create_project_task,
    log_artifact_task,
    log_dataframe_task,
    log_feature_task,
    log_metric_task,
    log_parameter_task,
)


def test_flow():
    persistence = "memory"
    root_dir = "./"
    project_name = "Prefect Integration Test"
    df = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]), columns=["a", "b", "c"])
    artifact = b"byte artifact"

    @flow
    def rubicon_flow():
        project_t = get_or_create_project_task(persistence, root_dir, project_name)
        experiment_t = create_experiment_task(project_t)
        log_feature_task(experiment_t, "test feature")
        log_parameter_task(experiment_t, "test parameter", 0)
        log_metric_task(experiment_t, "test metric", 0)
        log_dataframe_task(experiment_t, df, description="a test df")
        log_artifact_task(experiment_t, data_bytes=artifact, name="my artifact")

    outputs = rubicon_flow()

    assert len(outputs) == 7

    expected_output_types = [
        Project,
        Experiment,
        Feature,
        Parameter,
        Metric,
        Dataframe,
        Artifact,
    ]
    for output, expected_output_type in zip(outputs, expected_output_types):
        assert output.type == "COMPLETED"
        assert isinstance(output.result(), expected_output_type)

    # use Rubicon to grab the logged data
    rubicon = Rubicon(persistence, root_dir)
    project = rubicon.get_project(project_name)
    assert project.name == project_name

    experiments = project.experiments()
    assert len(experiments) == 1

    experiment = experiments[0]

    # features
    features = experiment.features()
    assert len(features) == 1
    assert features[0].name == "test feature"

    # metrics
    metrics = experiment.metrics()
    assert len(metrics) == 1
    assert metrics[0].name == "test metric"
    assert metrics[0].value == 0

    # parameters
    parameters = experiment.parameters()
    assert len(parameters) == 1
    assert parameters[0].name == "test parameter"
    assert parameters[0].value == 0

    # dataframes
    dataframes = experiment.dataframes()
    assert len(dataframes) == 1
    assert dataframes[0].description == "a test df"
    assert df.equals(dataframes[0].get_data())

    # artifacts
    artifacts = experiment.artifacts()
    assert len(artifacts) == 1
    assert artifacts[0].name == "my artifact"
    assert artifacts[0].data == artifact
