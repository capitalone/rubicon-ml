"""Mehtods for interacting with the existing rubicon-ml ``schema``."""

import os
from typing import Any, List

import yaml

RUBICON_SCHEMA_REGISTRY = {
    "h2o__H2OGeneralizedLinearEstimator": lambda: _load_schema(
        os.path.join("schema", "h2o__H2OGeneralizedLinearEstimator.yaml")
    ),
    "h2o__H2OGradientBoostingEstimator": lambda: _load_schema(
        os.path.join("schema", "h2o__H2OGradientBoostingEstimator.yaml")
    ),
    "h2o__H2ORandomForestEstimator": lambda: _load_schema(
        os.path.join("schema", "h2o__H2ORandomForestEstimator.yaml")
    ),
    "h2o__H2OTargetEncoderEstimator": lambda: _load_schema(
        os.path.join("schema", "h2o__H2OTargetEncoderEstimator.yaml")
    ),
    "h2o__H2OXGBoostEstimator": lambda: _load_schema(
        os.path.join("schema", "h2o__H2OXGBoostEstimator.yaml")
    ),
    "lightgbm__LGBMModel": lambda: _load_schema(os.path.join("schema", "lightgbm__LGBMModel.yaml")),
    "lightgbm__LGBMClassifier": lambda: _load_schema(
        os.path.join("schema", "lightgbm__LGBMClassifier.yaml")
    ),
    "lightgbm__LGBMRegressor": lambda: _load_schema(
        os.path.join("schema", "lightgbm__LGBMRegressor.yaml")
    ),
    "sklearn__RandomForestClassifier": lambda: _load_schema(
        os.path.join("schema", "sklearn__RandomForestClassifier.yaml")
    ),
    "xgboost__XGBModel": lambda: _load_schema(os.path.join("schema", "xgboost__XGBModel.yaml")),
    "xgboost__XGBClassifier": lambda: _load_schema(
        os.path.join("schema", "xgboost__XGBClassifier.yaml")
    ),
    "xgboost__XGBRegressor": lambda: _load_schema(
        os.path.join("schema", "xgboost__XGBRegressor.yaml")
    ),
    "xgboost__DaskXGBClassifier": lambda: _load_schema(
        os.path.join("schema", "xgboost__DaskXGBClassifier.yaml")
    ),
    "xgboost__DaskXGBRegressor": lambda: _load_schema(
        os.path.join("schema", "xgboost__DaskXGBRegressor.yaml")
    ),
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
