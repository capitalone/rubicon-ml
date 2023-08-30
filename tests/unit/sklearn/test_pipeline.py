from tempfile import mkdtemp
from unittest.mock import patch

from pytest import raises

from rubicon_ml.sklearn import RubiconPipeline
from rubicon_ml.sklearn.estimator_logger import EstimatorLogger
from rubicon_ml.sklearn.filter_estimator_logger import FilterEstimatorLogger
from rubicon_ml.sklearn.pipeline import Pipeline, make_pipeline


def test_get_default_estimator_logger(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    pipeline = RubiconPipeline(project, steps)

    logger = pipeline.get_estimator_logger()

    assert isinstance(logger, EstimatorLogger)


def test_get_user_defined_estimator_logger(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    user_defined_logger = {"est": FilterEstimatorLogger(ignore_all=True)}
    pipeline = RubiconPipeline(project, steps, user_defined_loggers=user_defined_logger)

    logger = pipeline.get_estimator_logger("est")

    assert isinstance(logger, FilterEstimatorLogger)


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


def test_make_pipeline(project_client, fake_estimator_cls):
    project = project_client
    clf = fake_estimator_cls()
    clf1 = fake_estimator_cls()
    pipe = make_pipeline(
        project, clf, clf1, experiment_kwargs={"name": "RubiconPipeline experiment"}
    )
    assert len(pipe.steps) == 2
    assert pipe.steps[0][1] == clf
    assert pipe.steps[1][1] == clf1
    assert len(pipe.user_defined_loggers) == 0


def test_make_pipeline_with_loggers(project_client, fake_estimator_cls):
    project = project_client
    clf = fake_estimator_cls()
    clf1 = fake_estimator_cls()
    user_defined_logger = FilterEstimatorLogger()
    user_defined_logger1 = FilterEstimatorLogger()
    pipe = make_pipeline(
        project,
        (clf, user_defined_logger),
        (clf1, user_defined_logger1),
        experiment_kwargs={"name": "RubiconPipeline experiment"},
    )
    assert len(pipe.steps) == 2
    assert pipe.steps[0][1] == clf
    assert pipe.steps[1][1] == clf1
    assert len(pipe.user_defined_loggers) == 2

    assert pipe.user_defined_loggers[pipe.steps[0][0]] == user_defined_logger
    assert pipe.user_defined_loggers[pipe.steps[1][0]] == user_defined_logger1
    pipe = make_pipeline(
        project,
        clf,
        (clf1, user_defined_logger1),
        experiment_kwargs={"name": "RubiconPipeline experiment"},
    )
    assert len(pipe.steps) == 2
    assert pipe.steps[0][1] == clf
    assert pipe.steps[1][1] == clf1
    assert len(pipe.user_defined_loggers) == 1
    assert pipe.user_defined_loggers[pipe.steps[1][0]] == user_defined_logger1


def test_make_pipeline_without_project(fake_estimator_cls):
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    with raises(ValueError) as e:
        make_pipeline(steps)
    assert "project" + str(steps) + " must be of type rubicon_ml.client.project.Project" == str(
        e.value
    )


def test_pipeline_memory_verbose(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    cachedir = mkdtemp()
    user_defined_logger = FilterEstimatorLogger()

    pipeline = RubiconPipeline(project, steps, {"est": user_defined_logger}, memory=cachedir)
    assert pipeline.memory == cachedir
    assert pipeline.verbose is False

    pipeline = RubiconPipeline(project, steps, {"est": user_defined_logger}, verbose=True)
    assert pipeline.memory is None
    assert pipeline.verbose is True

    pipeline = RubiconPipeline(project, steps, {"est": user_defined_logger})
    assert pipeline.memory is None
    assert pipeline.verbose is False


def test_multiple_fit_multiple_scores(project_client, fake_estimator_cls):
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
            pipeline.fit("additional fake data")

    assert mock_log_parameters._mock_call_count == 2
    assert len(project.experiments()) == 2

    with patch.object(Pipeline, "score", return_value=None):
        with patch.object(EstimatorLogger, "log_metric", return_value=None) as mock_log_metric:
            pipeline.score(["fake data"])
            assert pipeline.experiment is None
            pipeline.score(["additional fake data"])

    assert mock_log_metric._mock_call_count == 2
    assert len(project.experiments()) == 3


def test_multiple_scores(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    pipeline = RubiconPipeline(project, steps)

    with patch.object(Pipeline, "score", return_value=None) as mock_log_metric:
        with patch.object(EstimatorLogger, "log_metric", return_value=None):
            # first score gets its own explicitly declared experiment
            experiment = project.log_experiment(name="fake experiment")
            pipeline.score(["fake data"], experiment=experiment)
            pipeline.score(["additional fake data"])

    experiments = project.experiments()
    assert len(experiments) == 2
    assert mock_log_metric._mock_call_count == 2
    assert experiments[0].name == "fake experiment"
    assert experiments[1].name == "RubiconPipeline experiment"


def test_multiple_fits(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    user_defined_logger = {"est": FilterEstimatorLogger(ignore_all=True)}

    pipeline = RubiconPipeline(project, steps, user_defined_logger)

    with patch.object(Pipeline, "fit", return_value=None):
        with patch.object(
            FilterEstimatorLogger, "log_parameters", return_value=None
        ) as mock_log_parameters:
            # first fit gets its own explicitly declared experiment
            experiment = project.log_experiment(name="fake experiment")
            pipeline.fit(["fake data"], experiment=experiment)
            pipeline.fit("additional fake data")

    experiments = project.experiments()
    assert len(experiments) == 2
    assert mock_log_parameters._mock_call_count == 2

    assert experiments[0].name == "fake experiment"
    assert experiments[1].name == "RubiconPipeline experiment"


def test_score_samples(project_client, fake_estimator_cls):
    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    user_defined_logger = {"est": FilterEstimatorLogger(ignore_all=True)}

    pipeline = RubiconPipeline(project, steps, user_defined_logger)

    project = project_client
    estimator = fake_estimator_cls()
    steps = [("est", estimator)]
    user_defined_logger = {"est": FilterEstimatorLogger(ignore_all=True)}
    pipeline = RubiconPipeline(project, steps, user_defined_logger)

    with patch.object(Pipeline, "fit", return_value=None):
        with patch.object(FilterEstimatorLogger, "log_parameters", return_value=None):
            pipeline.fit(["fake data"])
    assert len(project.experiments()) == 1

    with patch.object(Pipeline, "score_samples", return_value=None):
        with patch.object(EstimatorLogger, "log_metric", return_value=None) as mock_log_metric:
            pipeline.score_samples(["fake data"])
            pipeline.score_samples(["additional fake data"])
            experiment = project.log_experiment(name="fake experiment")
            pipeline.score_samples(["additional fake data"], experiment=experiment)

    assert mock_log_metric._mock_call_count == 3
    assert len(project.experiments()) == 3
    assert project.experiments()[2].name == "fake experiment"


def test_pipeline_slices(project_client, fake_estimator_cls):
    project = project_client

    steps = [
        ("est", fake_estimator_cls()),
        ("est1", fake_estimator_cls()),
        ("est2", fake_estimator_cls()),
    ]
    cachedir = mkdtemp()
    est_logger = FilterEstimatorLogger()
    est1_logger = FilterEstimatorLogger
    user_defined_loggers = {"est": est_logger, "est1": est1_logger}

    pipeline = RubiconPipeline(project, steps, user_defined_loggers, memory=cachedir)
    assert pipeline[1:].steps == steps[1:]
    assert pipeline[1:].user_defined_loggers == {"est1": est1_logger}
    assert pipeline[:-1].steps == steps[:-1]

    with raises(ValueError) as e:
        pipeline[::-1]

    assert "Pipeline slicing only supports a step of 1" == str(e.value)


def test_sklearn_pipeline_invalid_step_count(project_client, fake_estimator_cls):
    project = project_client

    steps = [
        ("est", fake_estimator_cls()),
        ("est1", fake_estimator_cls()),
        ("est2", fake_estimator_cls()),
    ]
    cachedir = mkdtemp()
    est_logger = FilterEstimatorLogger()
    est1_logger = FilterEstimatorLogger
    user_defined_loggers = {"est": est_logger, "est1": est1_logger}

    pipeline = RubiconPipeline(project, steps, user_defined_loggers, memory=cachedir)
    with raises(ValueError) as e:
        pipeline[::-1]

    assert "Pipeline slicing only supports a step of 1" == str(e.value)
