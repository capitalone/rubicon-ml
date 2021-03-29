import numpy as np
import pandas as pd
from prefect import Flow

from rubicon import Rubicon
from rubicon.client import (
    Artifact,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
)
from rubicon.workflow.prefect import (
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
    with Flow("testing-rubicon-tasks") as flow:
        project_t = get_or_create_project_task(persistence, root_dir, project_name)
        experiment_t = create_experiment_task(project_t)
        feature_t = log_feature_task(experiment_t, "test feature")
        parameter_t = log_parameter_task(experiment_t, "test parameter", 0)
        metric_t = log_metric_task(experiment_t, "test metric", 0)
        dataframe_t = log_dataframe_task(experiment_t, df, description="a test df")
        artifact_t = log_artifact_task(experiment_t, data_bytes=artifact, name="my artifact")

    assert len(flow.tasks) == 7

    state = flow.run()

    assert state.is_successful()

    # outside of the flow, the objects are Task references
    assert state.result[project_t].is_successful()
    assert state.result[experiment_t].is_successful()
    assert state.result[feature_t].is_successful()
    assert state.result[parameter_t].is_successful()
    assert state.result[metric_t].is_successful()
    assert state.result[dataframe_t].is_successful()
    assert state.result[artifact_t].is_successful()

    assert isinstance(state.result[project_t].result, Project)
    assert isinstance(state.result[experiment_t].result, Experiment)
    assert isinstance(state.result[feature_t].result, Feature)
    assert isinstance(state.result[parameter_t].result, Parameter)
    assert isinstance(state.result[metric_t].result, Metric)
    assert isinstance(state.result[dataframe_t].result, Dataframe)
    assert isinstance(state.result[artifact_t].result, Artifact)

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
    assert df.equals(dataframes[0].data)

    # artifacts
    artifacts = experiment.artifacts()
    assert len(artifacts) == 1
    assert artifacts[0].name == "my artifact"
    assert artifacts[0].data == artifact
