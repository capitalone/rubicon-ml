from unittest.mock import patch

from rubicon_ml.sklearn import RubiconPipeline
from rubicon_ml.sklearn.estimator_logger import EstimatorLogger
from rubicon_ml.sklearn.filter_estimator_logger import FilterEstimatorLogger
from rubicon_ml.sklearn.pipeline import Pipeline


def test_get_default_estimator_logger(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    pipeline = RubiconPipeline(project, steps)

    logger = pipeline.get_estimator_logger()

    assert type(logger) == EstimatorLogger


def test_get_user_defined_estimator_logger(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    user_defined_logger = {"est": FilterEstimatorLogger(ignore_all=True)}
    pipeline = RubiconPipeline(project, steps, user_defined_logger)

    logger = pipeline.get_estimator_logger("est")

    assert type(logger) == FilterEstimatorLogger


def test_fit_logs_parameters(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    user_defined_logger = {"est": FilterEstimatorLogger(ignore_all=True)}
    pipeline = RubiconPipeline(project, steps, user_defined_logger)

    with patch.object(Pipeline, "fit", return_value=None):
        with patch.object(
            FilterEstimatorLogger, "log_parameters", return_value=None
        ) as mock_log_parameters:
            pipeline.fit(["fake data"])

    mock_log_parameters.assert_called_once()


def test_score_logs_metric(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    pipeline = RubiconPipeline(project, steps)

    with patch.object(Pipeline, "score", return_value=None):
        with patch.object(EstimatorLogger, "log_metric", return_value=None) as mock_log_metric:
            pipeline.score(["fake data"])

    mock_log_metric.assert_called_once()
