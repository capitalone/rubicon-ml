import pandas as pd
import pytest

from rubicon_ml.client import (
    Artifact,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
    Rubicon,
    RubiconJSON,
)


def test_experiment_to_json_single_experiment(rubicon_and_project_client):
    _, project = rubicon_and_project_client
    experiment = project.log_experiment("first experiment", tags=["a", "b"])
    experiment.log_parameter("n estimators", 3)
    experiment.log_feature("year")
    experiment.log_metric("accuracy", 0.87)
    experiment.log_metric("runtime(s)", 45)
    experiment.log_artifact(name="example artifact", data_bytes=b"a")
    experiment.log_dataframe(pd.DataFrame([[0, 1], [1, 0]]))

    experiment_as_json = RubiconJSON(experiments=experiment)
    json = experiment_as_json.json

    assert isinstance(json, dict)
    assert isinstance(json["experiment"], list)
    assert len(json["experiment"]) == 1
    assert isinstance(json["experiment"][0], dict)
    assert json["experiment"][0]["project_name"] == "Test Project"

    assert isinstance(json["experiment"][0]["tags"], list)
    assert isinstance(json["experiment"][0]["feature"], list)
    assert isinstance(json["experiment"][0]["parameter"], list)
    assert isinstance(json["experiment"][0]["metric"], list)
    assert isinstance(json["experiment"][0]["artifact"], list)
    assert isinstance(json["experiment"][0]["dataframe"], list)

    assert json["experiment"][0]["tags"] == ["a", "b"]
    assert len(json["experiment"][0]["metric"]) == 2


def test_experiment_to_json_multiple_experiments(rubicon_and_project_client_with_experiments):
    _, project = rubicon_and_project_client_with_experiments

    experiments_as_json = RubiconJSON(experiments=project.experiments())
    json = experiments_as_json.json

    assert isinstance(json, dict)
    assert isinstance(json["experiment"], list)
    assert len(json["experiment"]) == 10
    assert [isinstance(experiment, dict) for experiment in json["experiment"]]
    assert [isinstance(experiment["tags"], list) for experiment in json["experiment"]]
    assert [isinstance(experiment["feature"], list) for experiment in json["experiment"]]
    assert [isinstance(experiment["parameter"], list) for experiment in json["experiment"]]
    assert [isinstance(experiment["metric"], list) for experiment in json["experiment"]]


def test_project_to_json_single_project(rubicon_and_project_client_with_experiments):
    _, project = rubicon_and_project_client_with_experiments

    project_as_json = RubiconJSON(projects=project)
    json = project_as_json.json

    assert isinstance(json, dict)
    assert isinstance(json["project"], list)
    assert len(json["project"]) == 1
    assert json["project"][0]["name"] == "Test Project"


def test_project_to_json_multiple_projects(rubicon_and_project_client_with_experiments):
    rubicon, _ = rubicon_and_project_client_with_experiments
    project = rubicon.get_or_create_project(name="Second Test Project")
    project.log_artifact(name="example artifact", data_bytes=b"a")
    project.log_dataframe(pd.DataFrame([[0, 1], [1, 0]]))

    projects_as_json = RubiconJSON(projects=rubicon.projects())
    json = projects_as_json.json

    assert isinstance(json, dict)
    assert isinstance(json["project"], list)
    assert len(json["project"]) == 2
    assert json["project"][0]["name"] == "Test Project"
    assert json["project"][1]["name"] == "Second Test Project"


def test_rubicon_to_json_single_rubicon(rubicon_and_project_client_with_experiments):
    rubicon, _ = rubicon_and_project_client_with_experiments

    rubicon_as_json = RubiconJSON(rubicon_objects=rubicon)
    json = rubicon_as_json.json
    assert isinstance(json, dict)
    assert isinstance(json["project"], list)
    assert len(json["project"]) == 1
    assert json["project"][0]["name"] == "Test Project"


def test_rubicon_to_json_multiple_rubicons(rubicon_and_project_client_with_experiments):
    rubicon, _ = rubicon_and_project_client_with_experiments
    rubicon2 = Rubicon(persistence="memory")
    rubicon2.get_or_create_project(name="Test Project for Second Rubicon")

    rubicon_as_json = RubiconJSON(rubicon_objects=[rubicon, rubicon2])
    json = rubicon_as_json.json
    assert isinstance(json, dict)
    assert isinstance(json["project"], list)
    assert len(json["project"]) == 2
    assert json["project"][0]["name"] == "Test Project"
    assert json["project"][1]["name"] == "Test Project for Second Rubicon"


def test_convert_to_json_invalid_rubicon_objects_argument(rubicon_client):
    with pytest.raises(ValueError):
        RubiconJSON(rubicon_objects=5)

    with pytest.raises(ValueError):
        RubiconJSON(rubicon_objects=[rubicon_client, 5])


def test_convert_to_json_invalid_projects_argument(project_client):
    with pytest.raises(ValueError):
        RubiconJSON(projects=5)

    with pytest.raises(ValueError):
        RubiconJSON(rubicon_objects=[project_client, 5])


def test_convert_to_json_invalid_experiments_argument(project_client):
    with pytest.raises(ValueError):
        RubiconJSON(experiments=5)

    experiment = project_client.log_experiment(name="Test Experiment")
    with pytest.raises(ValueError):
        RubiconJSON(experiments=[experiment, 5])


def test_convert_to_json_rubicon_and_projects_input(rubicon_and_project_client_with_experiments):
    rubicon, _ = rubicon_and_project_client_with_experiments
    rubicon2 = Rubicon(persistence="memory")
    additional_project = rubicon2.get_or_create_project(name="Second Test Project")

    conversion_to_json = RubiconJSON(rubicon_objects=rubicon, projects=additional_project)
    json = conversion_to_json.json

    assert isinstance(json, dict)
    assert isinstance(json["project"], list)
    assert len(json["project"]) == 2
    assert json["project"][0]["name"] == "Test Project"
    assert json["project"][1]["name"] == "Second Test Project"


def test_convert_to_json_projects_and_experiments_input(
    rubicon_and_project_client_with_experiments,
):
    _, project = rubicon_and_project_client_with_experiments
    rubicon2 = Rubicon(persistence="memory")
    additional_project = rubicon2.get_or_create_project(name="Second Test Project")
    experiment1 = additional_project.log_experiment(name="additional experiment 1")
    experiment2 = additional_project.log_experiment(name="additional experiment 2")

    conversion_to_json = RubiconJSON(projects=project, experiments=[experiment1, experiment2])
    json = conversion_to_json.json

    assert isinstance(json, dict)
    assert isinstance(json["project"], list)
    assert len(json["experiment"]) == 2


def test_search_return_type_none(rubicon_and_project_client_with_experiments):
    rubicon, _ = rubicon_and_project_client_with_experiments
    rubicon_json = RubiconJSON(rubicon_objects=rubicon)

    res = rubicon_json.search("$..experiment[*].metric")

    assert len(res) == 10
    assert [len(metric.value) == 1 for metric in res]


def test_search_return_type_incorrect(rubicon_and_project_client_with_experiments):
    rubicon, _ = rubicon_and_project_client_with_experiments
    rubicon_json = RubiconJSON(rubicon_objects=rubicon)

    with pytest.raises(ValueError) as e:
        rubicon_json.search("$..experiment[*].metric", return_type="rubicon")

    assert (
        "`return_type` must be artifact, dataframe, experiment, feature, metric, parameter, or project."
        in str(e)
    )


def test_search_return_type_artifact(rubicon_and_project_client):
    _, project = rubicon_and_project_client
    ex = project.log_experiment(name="test")
    ex.log_artifact(name="artifact1", data_bytes=b"a1")
    ex.log_artifact(name="artifact2", data_bytes=b"a2")

    rubicon_json = RubiconJSON(projects=project)
    res = rubicon_json.search("$..experiment[*].artifact", return_type="artifact")

    assert [isinstance(item, Artifact) for item in res]


def test_search_return_type_dataframe(rubicon_and_project_client):
    _, project = rubicon_and_project_client
    ex = project.log_experiment(name="test")
    ex.log_dataframe(pd.DataFrame([[0, 1], [1, 0]]))
    ex.log_dataframe(pd.DataFrame([[0, 2], [2, 0]]))

    rubicon_json = RubiconJSON(projects=project)
    res = rubicon_json.search("$..experiment[*].dataframe", return_type="dataframe")

    assert [isinstance(item, Dataframe) for item in res]


def test_search_return_type_experiment(rubicon_and_project_client_with_experiments):
    _, project = rubicon_and_project_client_with_experiments
    rubicon_json = RubiconJSON(projects=project)
    res = rubicon_json.search("$..experiment[*]", return_type="experiment")

    assert [isinstance(item, Experiment) for item in res]


def test_search_return_type_feature(rubicon_and_project_client_with_experiments):
    _, project = rubicon_and_project_client_with_experiments
    rubicon_json = RubiconJSON(projects=project)
    res = rubicon_json.search("$..experiment[*].feature", return_type="feature")

    assert [isinstance(item, Feature) for item in res]


def test_search_return_type_metric(rubicon_and_project_client_with_experiments):
    _, project = rubicon_and_project_client_with_experiments
    rubicon_json = RubiconJSON(projects=project)
    res = rubicon_json.search("$..experiment[*].metric", return_type="metric")

    assert [isinstance(item, Metric) for item in res]


def test_search_return_type_parameter(rubicon_and_project_client_with_experiments):
    _, project = rubicon_and_project_client_with_experiments
    rubicon_json = RubiconJSON(projects=project)
    res = rubicon_json.search("$..experiment[*].parameter", return_type="parameter")

    assert [isinstance(item, Parameter) for item in res]


def test_search_return_type_project(rubicon_and_project_client_with_experiments):
    rubicon, _ = rubicon_and_project_client_with_experiments
    rubicon_json = RubiconJSON(rubicon_objects=rubicon)
    res = rubicon_json.search("$..project[*]", return_type="project")

    assert [isinstance(item, Project) for item in res]
