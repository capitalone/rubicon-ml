from unittest.mock import patch

import pytest

from rubicon import domain
from rubicon.client.experiment import Experiment
from rubicon.sklearn.estimator_logger import EstimatorLogger


def test_log_parameters_triggers_experiment_log_parameter(project_client, fake_estimator_cls):
    project = project_client
    experiment = Experiment(domain.Experiment(project_name=project.name), project)
    estimator = fake_estimator_cls()

    base_logger = EstimatorLogger(estimator=estimator, experiment=experiment, step_name="vect")

    with patch.object(Experiment, "log_parameter", return_value=None) as mock_log_parameter:
        base_logger.log_parameters()

    assert mock_log_parameter.call_count == 3

    # the step name gets prepended to each param
    mock_log_parameter.assert_called_with(name="vect__ngram_range", value=(1, 2))


def test_log_unserializable_param_triggers_exception(project_client, fake_estimator_cls):
    project = project_client
    experiment = Experiment(domain.Experiment(project_name=project.name), project)
    estimator = fake_estimator_cls(params={"unserializable": b"not serializable"})

    base_logger = EstimatorLogger(estimator=estimator, experiment=experiment, step_name="vect")

    with patch.object(Experiment, "log_parameter", side_effect=Exception("test")):
        with pytest.warns(Warning):
            base_logger.log_parameters()
