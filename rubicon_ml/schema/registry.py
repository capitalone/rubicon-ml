"""Mehtods for interacting with the existing rubicon-ml ``schema``."""

import os
from typing import Any, List

import yaml

AVAILABLE_SCHEMA = [
    "h2o__ModelBase",
    "h2o__H2OGeneralizedLinearEstimator",
    "h2o__H2OGenericEstimator",
    "h2o__H2OGradientBoostingEstimator",
    "h2o__H2ORandomForestEstimator",
    "h2o__H2OTargetEncoderEstimator",
    "h2o__H2OXGBoostEstimator",
    "lightgbm__LGBMModel",
    "lightgbm__LGBMClassifier",
    "lightgbm__LGBMRegressor",
    "sklearn__RandomForestClassifier",
    "xgboost__XGBModel",
    "xgboost__XGBClassifier",
    "xgboost__XGBRegressor",
    "xgboost__DaskXGBClassifier",
    "xgboost__DaskXGBRegressor",
]
RUBICON_SCHEMA_REGISTRY = {
    schema: (lambda s=schema: _load_schema(os.path.join("schema", f"{s}.yaml")))
    for schema in AVAILABLE_SCHEMA
}


def _load_schema(path: str) -> Any:
    """Loads a schema YAML file from ``path`` relative to this file."""

    full_path = os.path.join(os.path.dirname(__file__), path)
    with open(full_path, "r") as file:
        schema = yaml.safe_load(file)

    return schema


def available_schema() -> List[str]:
    """Get the names of all available schema."""

    return list(RUBICON_SCHEMA_REGISTRY.keys())


def get_schema(name: str) -> Any:
    """Get the schema with name ``name``."""

    if name not in RUBICON_SCHEMA_REGISTRY:
        raise ValueError(
            f"'{name}' is not the name of an available rubicon schema. "
            "For a list of schema names, use `registry.available_schema()`."
        )

    return RUBICON_SCHEMA_REGISTRY[name]()


def get_schema_name(obj: Any) -> str:
    """Get the name of the schema that represents object ``obj``."""

    obj_cls = obj.__class__

    cls_name = obj_cls.__name__
    module_name = obj_cls.__module__.split(".")[0]

    return f"{module_name}__{cls_name}"


def register_schema(name: str, schema: dict):
    """Add a schema to the schema registry."""

    RUBICON_SCHEMA_REGISTRY[name] = lambda: schema
