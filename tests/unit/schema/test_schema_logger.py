"""Testing ``schema_logger``."""

import os
from copy import deepcopy
from unittest import mock

import pandas as pd
import pytest

import rubicon_ml
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.schema import logger
from rubicon_ml.schema.registry import RUBICON_SCHEMA_REGISTRY


def test_safe_getattr_raises_error(objects_to_log):
    """Testing ``_safe_getattr`` raises an error when not optional."""

    object_to_log, _ = objects_to_log
    missing_attr_name = "missing_attr"

    with pytest.raises(AttributeError) as e:
        logger._safe_getattr(
            object_to_log,
            missing_attr_name,
            optional=False,
        )

    assert f"no attribute '{missing_attr_name}'" in str(e)


def test_safe_call_func_raises_error(objects_to_log):
    """Testing ``_safe_call_func`` raises an error when not optional."""

    object_to_log, _ = objects_to_log
    missing_func_name = "missing_func"

    with pytest.raises(AttributeError) as e:
        logger._safe_call_func(
            object_to_log,
            missing_func_name,
            optional=False,
        )

    assert f"no attribute '{missing_func_name}'" in str(e)


def test_safe_call_func_reraises_error(objects_to_log):
    """Testing ``_safe_call_func`` reraises an error when not optional."""

    object_to_log, _ = objects_to_log
    erroring_func_name = "erroring_function"

    with pytest.raises(Exception) as e:
        logger._safe_call_func(
            object_to_log,
            erroring_func_name,
            optional=False,
        )

    assert "raised from `_ObjectToLog.erroring_function`" in str(e)


def test_safe_environ_raises_error(objects_to_log):
    """Testing ``_safe_environ`` raises an error when not optional."""

    object_to_log, _ = objects_to_log
    missing_environ_name = "missing_environ"

    with pytest.raises(RubiconException) as e:
        logger._safe_environ(
            missing_environ_name,
            optional=False,
        )

    assert f"'{missing_environ_name}' not set" in str(e)


def test_log_inferred_schema(objects_to_log, rubicon_project, another_object_schema):
    """Testing ``Project.log_with_schema`` can log inferred schema."""

    _, another_object = objects_to_log
    schema_to_patch = {"tests___AnotherObject": lambda: another_object_schema}

    with mock.patch.dict(RUBICON_SCHEMA_REGISTRY, schema_to_patch, clear=True):
        experiment = rubicon_project.log_with_schema(another_object)

    parameter = experiment.parameter(name=another_object_schema["parameters"][0]["name"])
    metric = experiment.metric(name=another_object_schema["metrics"][0]["name"])

    assert rubicon_project.schema_ == another_object_schema
    assert parameter.value == getattr(
        another_object,
        another_object_schema["parameters"][0]["value_attr"],
    )
    assert metric.value == getattr(
        another_object,
        another_object_schema["metrics"][0]["value_attr"],
    )


def test_log_artifacts_with_schema(objects_to_log, rubicon_project, artifact_schema):
    """Testing ``Project.log_with_schema`` can log artifacts."""

    object_to_log, another_object = objects_to_log
    object_b = object_to_log
    otl_cls, ao_cls, obj_b_cls = (
        object_to_log.__class__,
        another_object.__class__,
        object_b.__class__,
    )

    def custom_logging_func(self, obj, test_param):
        self.custom_logging_func_called = True

    artifact_schema["artifacts"].append({"self": "custom_logging_func", "test_param": "test"})

    with mock.patch.object(
        rubicon_ml.client.experiment.Experiment,
        "custom_logging_func",
        custom_logging_func,
        create=True,
    ):
        rubicon_project.set_schema(artifact_schema)
        experiment = rubicon_project.log_with_schema(object_to_log)

    otl_artifact = experiment.artifact(name=otl_cls.__name__)
    ao_artifact = experiment.artifact(name=artifact_schema["artifacts"][1]["name"])
    obj_b_artifact = experiment.artifact(name=artifact_schema["artifacts"][2]["name"])

    assert experiment.custom_logging_func_called
    assert isinstance(otl_artifact.get_data(unpickle=True), otl_cls)
    assert isinstance(ao_artifact.get_data(unpickle=True), ao_cls)
    assert isinstance(obj_b_artifact.get_data(unpickle=True), obj_b_cls)


def test_log_dataframes_with_schema(objects_to_log, rubicon_project, dataframe_schema):
    """Testing ``Project.log_with_schema`` can log dataframes."""

    object_to_log, _ = objects_to_log

    rubicon_project.set_schema(dataframe_schema)
    experiment = rubicon_project.log_with_schema(object_to_log)

    dataframe = experiment.dataframe(name=dataframe_schema["dataframes"][0]["name"])
    dataframe_b = experiment.dataframe(name=dataframe_schema["dataframes"][1]["name"])

    assert isinstance(dataframe.get_data(), pd.DataFrame)
    assert isinstance(dataframe_b.get_data(), pd.DataFrame)
    assert dataframe.get_data().equals(object_to_log.dataframe)
    assert dataframe.get_data().equals(object_to_log.dataframe_function())


def test_log_features_with_schema(objects_to_log, rubicon_project, feature_schema):
    """Testing ``Project.log_with_schema`` can log features."""

    object_to_log, _ = objects_to_log

    rubicon_project.set_schema(feature_schema)
    experiment = rubicon_project.log_with_schema(object_to_log)

    expected_feature_names = getattr(object_to_log, feature_schema["features"][0]["names_attr"])
    expected_feature_names.extend(
        getattr(object_to_log, feature_schema["features"][1]["names_attr"])
    )
    expected_feature_names.append(
        getattr(object_to_log, feature_schema["features"][2]["name_attr"])
    )
    expected_feature_names.append(
        getattr(object_to_log, feature_schema["features"][3]["name_attr"])
    )

    expected_feature_importances = getattr(
        object_to_log, feature_schema["features"][0].get("importances_attr")
    )
    expected_feature_importances.extend([None, None])
    expected_feature_importances.append(
        getattr(object_to_log, feature_schema["features"][2].get("importance_attr"))
    )
    expected_feature_importances.append(None)

    for name, importance in zip(expected_feature_names, expected_feature_importances):
        feature = experiment.feature(name=name)

        assert feature.importance == importance


def test_log_metrics_with_schema(objects_to_log, rubicon_project, metric_schema):
    """Testing ``Project.log_with_schema`` can log metrics."""

    object_to_log, _ = objects_to_log

    rubicon_project.set_schema(metric_schema)

    with mock.patch.dict(os.environ, {"METRIC": "metric env value"}, clear=True):
        experiment = rubicon_project.log_with_schema(object_to_log)

    metric_a = experiment.metric(name=metric_schema["metrics"][0]["name"])
    metric_b = experiment.metric(name=metric_schema["metrics"][1]["name"])
    metric_c = experiment.metric(name=metric_schema["metrics"][2]["name"])

    assert metric_a.value == getattr(object_to_log, metric_schema["metrics"][0]["value_attr"])
    assert metric_b.value == "metric env value"

    method = getattr(object_to_log, metric_schema["metrics"][2]["value_func"])
    assert metric_c.value == method()


def test_log_parameters_with_schema(objects_to_log, rubicon_project, parameter_schema):
    """Testing ``Project.log_with_schema`` can log parameters."""

    object_to_log, _ = objects_to_log

    rubicon_project.set_schema(parameter_schema)

    with mock.patch.dict(os.environ, {"PARAMETER": "param env value"}, clear=True):
        experiment = rubicon_project.log_with_schema(object_to_log)

    parameter_a = experiment.parameter(name=parameter_schema["parameters"][0]["name"])
    parameter_b = experiment.parameter(name=parameter_schema["parameters"][1]["name"])

    assert parameter_a.value == getattr(
        object_to_log, parameter_schema["parameters"][0]["value_attr"]
    )
    assert parameter_b.value == "param env value"


@pytest.mark.parametrize("infer", [True, False])
def test_log_nested_schema(
    objects_to_log, rubicon_project, another_object_schema, nested_schema, infer
):
    """Testing ``Project.log_with_schema`` can log nested schema."""

    if infer:
        nested_schema["schema"][0]["name"] = "infer"

    object_to_log, another_object = objects_to_log
    schema_to_patch = {"tests___AnotherObject": lambda: another_object_schema}

    with mock.patch.dict(RUBICON_SCHEMA_REGISTRY, schema_to_patch, clear=True):
        rubicon_project.set_schema(nested_schema)
        experiment = rubicon_project.log_with_schema(object_to_log)

    parameter = experiment.parameter(name=another_object_schema["parameters"][0]["name"])
    metric = experiment.metric(name=another_object_schema["metrics"][0]["name"])

    assert parameter.value == getattr(
        another_object,
        another_object_schema["parameters"][0]["value_attr"],
    )
    assert metric.value == getattr(
        another_object,
        another_object_schema["metrics"][0]["value_attr"],
    )


def test_log_extended_schema(objects_to_log, rubicon_project, another_object_schema):
    """Testing ``Project.log_with_schema`` can log extended schema."""

    _, another_object = objects_to_log

    feature_name_attr = "extended_schema_feature"
    feature_name_value = "extended schema feature"
    setattr(another_object, feature_name_attr, feature_name_value)

    schema_to_patch = {"AnotherObject": lambda: another_object_schema}
    extended_schema = {
        "extends": "AnotherObject",
        "features": [{"name_attr": feature_name_attr}],
    }

    with mock.patch.dict(RUBICON_SCHEMA_REGISTRY, schema_to_patch, clear=True):
        rubicon_project.set_schema(extended_schema)
        experiment = rubicon_project.log_with_schema(another_object)

    feature = experiment.feature(name=feature_name_value)
    parameter = experiment.parameter(name=another_object_schema["parameters"][0]["name"])
    metric = experiment.metric(name=another_object_schema["metrics"][0]["name"])

    assert feature.name == feature_name_value
    assert parameter.value == getattr(
        another_object,
        another_object_schema["parameters"][0]["value_attr"],
    )
    assert metric.value == getattr(
        another_object,
        another_object_schema["metrics"][0]["value_attr"],
    )


def test_log_optional_schema(objects_to_log, rubicon_project, optional_schema):
    """Testing ``Project.log_with_schema`` can log optional schema."""

    object_to_log, _ = objects_to_log
    schema_to_patch = {"MissingObject": lambda: {}}

    with mock.patch.dict(RUBICON_SCHEMA_REGISTRY, schema_to_patch, clear=True):
        rubicon_project.set_schema(optional_schema)
        experiment = rubicon_project.log_with_schema(object_to_log)

    assert len(experiment.artifacts()) == 0
    assert len(experiment.dataframes()) == 0
    assert len(experiment.features()) == 0

    assert len(experiment.parameters()) == 2
    for parameter in experiment.parameters():
        assert parameter.value is None

    assert len(experiment.metrics()) == 3
    for metric in experiment.metrics():
        assert metric.value is None


def test_log_with_children(
    objects_to_log, rubicon_project, another_object_schema, hierarchical_schema
):
    """Testing ``Project.log_with_schema`` can log hierarchical schema."""

    object_to_log, another_object = objects_to_log
    schema_to_patch = {"AnotherObject": lambda: another_object_schema}

    num_children = 4
    object_to_log.children = [deepcopy(another_object) for _ in range(num_children)]

    with mock.patch.dict(RUBICON_SCHEMA_REGISTRY, schema_to_patch, clear=True):
        rubicon_project.set_schema(hierarchical_schema)
        parent_experiment = rubicon_project.log_with_schema(object_to_log)

    assert len(rubicon_project.experiments()) == 1 + num_children
    assert len(rubicon_project.experiments(tags=["parent"])) == 1

    child_experiments = rubicon_project.experiments(tags=["child"])
    assert len(child_experiments) == num_children

    for child_experiment in child_experiments:
        assert f"parent_id:{parent_experiment.id}" in child_experiment.tags


def test_log_with_schema_and_experiment_kwargs(
    objects_to_log,
    rubicon_project,
    artifact_schema,
):
    """Testing ``Project.log_with_schema`` can log experiment kwargs."""

    object_to_log, _ = objects_to_log

    rubicon_project.set_schema(artifact_schema)
    experiment = rubicon_project.log_with_schema(
        object_to_log,
        experiment_kwargs={"name": "name", "description": "description"},
    )

    assert experiment.name == "name"
    assert experiment.description == "description"


def test_log_with_schema_raises_error(objects_to_log, rubicon_project):
    """Testing ``Project.log_with_schema`` rasies an error when no schema is set."""

    object_to_log, _ = objects_to_log

    with pytest.raises(ValueError) as err:
        _ = rubicon_project.log_with_schema(object_to_log)

    assert "No schema set and no schema could be inferred" in str(err)


def test_set_schema(rubicon_project, artifact_schema):
    """Testing ``Project.set_schema``."""

    rubicon_project.set_schema(artifact_schema)

    assert rubicon_project.schema_ == artifact_schema
