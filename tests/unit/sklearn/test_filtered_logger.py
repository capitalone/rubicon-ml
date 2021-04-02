from unittest.mock import patch

import pytest

from rubicon import domain
from rubicon.client.experiment import Experiment
from rubicon.exceptions import RubiconException
from rubicon.sklearn.filter_logger import FilterLogger


def test_select_and_ignore_exception():
    with pytest.raises(RubiconException) as e:
        FilterLogger(select=["this", "should"], ignore=["fail"])

    assert "provide either `select` OR `ignore`" in str(e)


def test_ignore_all(fake_estimator_cls):
    logger = FilterLogger(estimator=fake_estimator_cls(), ignore_all=True)

    with patch.object(Experiment, "log_parameter", return_value=None) as mock_log_parameter:
        logger.log_parameters()

    mock_log_parameter.assert_not_called()


def test_ignore_parameters(project_client, fake_estimator_cls):
    project = project_client
    experiment = Experiment(domain.Experiment(project_name=project.name), project)
    estimator = fake_estimator_cls()

    logger = FilterLogger(
        estimator=estimator, experiment=experiment, step_name="vect", select=["max_df"]
    )

    with patch.object(Experiment, "log_parameter", return_value=None) as mock_log_parameter:
        logger.log_parameters()

    assert mock_log_parameter.call_count == 1

    # the step name gets prepended to each param
    mock_log_parameter.assert_called_with(name="vect__max_df", value=0.75)


def test_select_parameters(project_client, fake_estimator_cls):
    project = project_client
    experiment = Experiment(domain.Experiment(project_name=project.name), project)
    estimator = fake_estimator_cls()

    logger = FilterLogger(
        estimator=estimator, experiment=experiment, step_name="vect", ignore=["ngram_range"]
    )

    with patch.object(Experiment, "log_parameter", return_value=None) as mock_log_parameter:
        logger.log_parameters()

    assert mock_log_parameter.call_count == 2

    # the step name gets prepended to each param
    mock_log_parameter.assert_called_with(name="vect__lowercase", value=True)
