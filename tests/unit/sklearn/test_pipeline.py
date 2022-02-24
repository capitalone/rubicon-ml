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


def test_fit_logs_fit_parameters(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    user_defined_logger = {"est": FilterEstimatorLogger(ignore_all=True)}
    pipeline = RubiconPipeline(project, steps, user_defined_logger)

    with patch.object(Pipeline, "fit", return_value=None):
        with patch.object(FilterEstimatorLogger, "log_parameters", return_value=None):
            pipeline.fit(["fake data"], est__test_fit_param="test_value")

    parameters = pipeline.experiment.parameters()
    assert len(parameters) == 1

    parameter = parameters[0]
    assert parameter.name == "est__test_fit_param"
    assert parameter.value == "test_value"


def test_score_logs_metric(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    pipeline = RubiconPipeline(project, steps)

    with patch.object(Pipeline, "score", return_value=None):
        with patch.object(EstimatorLogger, "log_metric", return_value=None) as mock_log_metric:
            pipeline.score(["fake data"])

    mock_log_metric.assert_called_once()


def test_fit_multiple_scores(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    pipeline = RubiconPipeline(project, steps)

    with patch.object(Pipeline, "score", return_value=None):
        with patch.object(EstimatorLogger, "log_metric", return_value=None):
            pipeline.score(["fake data"])
            pipeline.score(["additional fake data"])
    with patch.object(Pipeline, "fit", return_value=None):
        with patch.object(FilterEstimatorLogger, "log_parameters", return_value=None):
            pipeline.fit(["fake data"])
            pipeline.fit("additional fake data")

    assert len(project.experiments()) == 4


def test_multiple_scores(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    pipeline = RubiconPipeline(project, steps)

    with patch.object(Pipeline, "score", return_value=None):
        with patch.object(EstimatorLogger, "log_metric", return_value=None):
            # first score gets its own explicitly declared experiment
            experiment = project.log_experiment(name="fake experiment")
            pipeline.score(["fake data"], experiment=experiment)
            pipeline.score(["additional fake data"])

    experiments = project.experiments()
    assert len(experiments) == 2
    assert experiments[0].name == "fake experiment"
    assert experiments[1].name == "RubiconPipeline experiment"


def test_multiple_fits(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    pipeline = RubiconPipeline(project, steps)

    with patch.object(Pipeline, "fit", return_value=None):
        with patch.object(FilterEstimatorLogger, "log_parameters", return_value=None):
            # first fit gets its own explicitly declared experiment
            experiment = project.log_experiment(name="fake experiment")
            pipeline.fit(["fake data"], experiment=experiment)
            pipeline.fit("additional fake data")

    experiments = project.experiments()
    assert len(experiments) == 2
    assert experiments[0].name == "fake experiment"
    assert experiments[1].name == "RubiconPipeline experiment"
