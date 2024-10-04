"""Methods and a mixin to enable schema logging.

The functions available in the ``schema`` submodule are applied to
``rubicon_ml.Project`` via the ``SchemaMixin`` class. They can be
called directly as a method of an existing project.
"""

import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

from rubicon_ml.client.experiment import Experiment
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.schema import registry


def _get_value(obj, entity_schema):
    optional = entity_schema.get("optional", False)
    value = None

    if "value_attr" in entity_schema:
        value = _safe_getattr(obj, entity_schema["value_attr"], optional)
    if "value_env" in entity_schema:
        value = _safe_environ(entity_schema["value_env"], optional)
    if "value_func" in entity_schema:
        value = _safe_call_func(obj, entity_schema["value_func"], optional)

    return value


def _get_df(obj, entity_schema):
    optional = entity_schema.get("optional", False)
    df_value = None

    if "df_attr" in entity_schema:
        df_value = _safe_getattr(obj, entity_schema["df_attr"], optional)
    if "df_func" in entity_schema:
        df_value = _safe_call_func(obj, entity_schema["df_func"], optional)

    return df_value


def _get_data_object(obj, entity_schema):
    optional = entity_schema.get("optional", False)
    data_object = None

    if "data_object_func" in entity_schema:
        data_object = _safe_call_func(obj, entity_schema["data_object_func"], optional)
    elif "data_object_attr" in entity_schema:
        data_object = _safe_getattr(obj, entity_schema["data_object_attr"], optional)

    return data_object


def _safe_getattr(obj, attr, optional, default=None):
    try:
        value = getattr(obj, attr)
    except (TypeError, AttributeError) as err:
        if optional or (attr is None and isinstance(err, TypeError)):
            return default

        raise err

    return value


def _safe_environ(environ_var, optional, default=None):
    try:
        value = os.environ[environ_var]
    except KeyError as err:
        if optional:
            return default

        raise RubiconException(f"Environment variable '{environ_var}' not set.") from err

    return value


def _safe_call_func(obj, func, optional, default=None):
    method = _safe_getattr(obj, func, optional, default)
    value = None

    if method is not None:
        try:
            value = method()
        except Exception as err:
            if optional:
                return default

            raise err

    return value


@contextmanager
def _set_temporary_schema(project, schema_name):
    original_schema = project.schema_

    if schema_name == "infer":
        delattr(project, "schema_")
    else:
        project.set_schema(registry.get_schema(schema_name))

    yield

    project.set_schema(original_schema)


class SchemaMixin:
    """Adds schema logging support to a client object."""

    def log_with_schema(
        self,
        obj: Any,
        experiment: Experiment = None,
        experiment_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Log an experiment leveraging ``self.schema_``."""

        if not hasattr(self, "schema_"):
            try:
                schema_name = registry.get_schema_name(obj)
                self.schema_ = registry.get_schema(schema_name)
            except ValueError as err:
                raise ValueError(
                    f"No schema set and no schema could be inferred from object {obj}. "
                    f"Set a schema with `Project.set_schema(schema)`."
                ) from err

        if experiment_kwargs is None:
            experiment_kwargs = {}

        if experiment is None:
            experiment = self.log_experiment(**experiment_kwargs)

        base_schema_name = self.schema_.get("extends")
        if base_schema_name is not None:
            with _set_temporary_schema(self, base_schema_name):
                self.log_with_schema(obj, experiment=experiment)

        for feature in self.schema_.get("features", []):
            is_optional = feature.get("optional", False)

            if "names_attr" in feature:
                feature_names = _safe_getattr(obj, feature["names_attr"], is_optional)

                if feature_names is not None:
                    feature_importances = _safe_getattr(
                        obj,
                        feature.get("importances_attr"),
                        is_optional,
                        default=[None] * len(feature["names_attr"]),
                    )

                    for name, importance in zip(feature_names, feature_importances):
                        experiment.log_feature(name=name, importance=importance)

            elif "name_attr" in feature:
                feature_name = _safe_getattr(obj, feature["name_attr"], is_optional)

                if feature_name is not None:
                    feature_importance = _safe_getattr(
                        obj, feature.get("importance_attr"), is_optional
                    )

                    experiment.log_feature(name=feature_name, importance=feature_importance)

        for parameter in self.schema_.get("parameters", []):
            experiment.log_parameter(
                name=parameter["name"],
                value=_get_value(obj, parameter),
            )

        for metric in self.schema_.get("metrics", []):
            experiment.log_metric(
                name=metric["name"],
                value=_get_value(obj, metric),
            )
        for artifact in self.schema_.get("artifacts", []):
            if isinstance(artifact, str):
                if artifact == "self":
                    experiment.log_artifact(name=obj.__class__.__name__, data_object=obj)
            elif isinstance(artifact, dict):
                if "self" in artifact:
                    logging_func_name = artifact["self"]
                    logging_func = getattr(experiment, logging_func_name)

                    # Get remaining artifact logging function parameters and run with func
                    logging_func(
                        obj, **dict((k, v) for k, v in artifact.items() if k != "self")
                    )  # key-values in rest of dictionary are passed as arguments
                else:
                    data_object = _get_data_object(obj, artifact)

                    if data_object is not None:
                        experiment.log_artifact(name=artifact["name"], data_object=data_object)

        for dataframe in self.schema_.get("dataframes", []):
            df_value = _get_df(obj, dataframe)

            if df_value is not None:
                experiment.log_dataframe(df=df_value, name=dataframe["name"])

        for schema in self.schema_.get("schema", []):
            object_to_log = _safe_getattr(obj, schema["attr"], schema.get("optional", False))

            if object_to_log is not None:
                with _set_temporary_schema(self, schema["name"]):
                    self.log_with_schema(object_to_log, experiment=experiment)

        has_children = False

        for children in self.schema_.get("children", []):
            children_objects = _safe_getattr(
                obj, children["attr"], children.get("optional", False), default=[]
            )

            for child in children_objects:
                has_children = True

                child_experiment = self.log_experiment(**experiment_kwargs)
                child_experiment.add_tags(tags=["child", f"parent_id:{experiment.id}"])

                with _set_temporary_schema(self, children["name"]):
                    self.log_with_schema(child, experiment=child_experiment)

        if has_children:
            experiment.add_tags(tags=["parent"])

        return experiment

    def set_schema(self, schema: Dict[str, Any]) -> None:
        """Set the schema for this client object."""

        self.schema_ = schema
