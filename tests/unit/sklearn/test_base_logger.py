from unittest.mock import patch

import pytest

from rubicon import domain
from rubicon.client.experiment import Experiment
from rubicon.sklearn.base_logger import BaseLogger

DEFAULT_PARAMS = {"max_df": 0.75, "lowercase": True, "ngram_range": (1, 2)}


class FakeEstimator:
    """A fake estimator that exposes the same API as a sklearn
    estimator so we can test without relying on sklearn.
    """

    def __init__(self, params=DEFAULT_PARAMS):
        self.params = params

    def get_params(self):
        return self.params


def test_log_parameters_triggers_experiment_log_parameter(project_client):
    project = project_client
    experiment = Experiment(domain.Experiment(project_name=project.name), project)
    estimator = FakeEstimator()

    base_logger = BaseLogger(estimator=estimator, experiment=experiment, step_name="vect")

    with patch.object(Experiment, "log_parameter", return_value=None) as mock_log_parameter:
        base_logger.log_parameters()

    assert mock_log_parameter.call_count == 3

    # the step name gets prepended to each param
    mock_log_parameter.assert_called_with(name="vect__ngram_range", value=(1, 2))

def test_log_unserializable_param_triggers_exception(project_client):
    project = project_client
    experiment = Experiment(domain.Experiment(project_name=project.name), project)
    estimator = FakeEstimator(params={"unserializable": b"not serializable"})

    base_logger = BaseLogger(estimator=estimator, experiment=experiment, step_name="vect")

    with patch.object(Experiment, "log_parameter", side_effect=Exception('test')) as mock_log_parameter:
        with pytest.warns(Warning):
            base_logger.log_parameters()
