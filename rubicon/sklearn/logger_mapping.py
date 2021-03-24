from rubicon.sklearn.base_logger import BaseEstimatorLogger
from rubicon.sklearn.loggers import CountVectorizerLogger, StandardScalerLogger

_logger_mappings = {
    "CountVectorizerLogger": CountVectorizerLogger,
    "StandardScaler": StandardScalerLogger,
}


def get_logger(sklearn_class_name):
    return _logger_mappings.get(sklearn_class_name, BaseEstimatorLogger)
