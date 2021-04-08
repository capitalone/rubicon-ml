from rubicon.sklearn import RubiconPipeline
from rubicon.sklearn.estimator_logger import EstimatorLogger
from rubicon.sklearn.filter_estimator_logger import FilterEstimatorLogger

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
    
    logger = pipeline.get_estimator_logger('est')

    assert type(logger) == FilterEstimatorLogger