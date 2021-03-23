from rubicon.sklearn.base_logger import BaseEstimatorLogger
from rubicon.sklearn.pipeline import RubiconPipeline

_logger_mappings = {}


def get_logger(sklearn_class_name):
    return _logger_mappings.get(sklearn_class_name, BaseEstimatorLogger)


__all__ = ["get_logger", "RubiconPipeline"]
