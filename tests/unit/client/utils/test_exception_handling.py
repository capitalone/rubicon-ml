from unittest.mock import patch

import pytest

from rubicon_ml.client.utils.exception_handling import FAILURE_MODES, set_failure_mode
from rubicon_ml.exceptions import RubiconException


@pytest.mark.parametrize("failure_mode", FAILURE_MODES)
def test_set_failure_mode(failure_mode):
    set_failure_mode(failure_mode=failure_mode)

    from rubicon_ml.client.utils.exception_handling import FAILURE_MODE

    assert FAILURE_MODE == failure_mode

    # cleanup: reset to default
    set_failure_mode("raise")


def test_set_failure_mode_traceback_options():
    traceback_chain = True
    traceback_limit = 1

    set_failure_mode("raise", traceback_chain=traceback_chain, traceback_limit=traceback_limit)

    from rubicon_ml.client.utils.exception_handling import (
        TRACEBACK_CHAIN,
        TRACEBACK_LIMIT,
    )

    assert TRACEBACK_CHAIN == traceback_chain
    assert TRACEBACK_LIMIT == traceback_limit


def test_set_failure_mode_error():
    with pytest.raises(ValueError) as e:
        set_failure_mode("invalid mode")

    assert f"`failure_mode` must be one of {FAILURE_MODES}" in repr(e)


def test_failure_mode_raise(rubicon_client):
    set_failure_mode("raise")

    with pytest.raises(RubiconException) as e:
        rubicon_client.get_project(name="does not exist")

    assert "No project with name 'does not exist' found." in repr(e)


@patch("warnings.warn")
def test_failure_mode_log(mock_warn, rubicon_client):
    set_failure_mode("warn")

    rubicon_client.get_project(name="does not exist")

    mock_warn.assert_called_once()

    # cleanup: reset to default
    set_failure_mode("raise")


@patch("logging.error")
def test_failure_mode_warn(mock_logger, rubicon_client):
    set_failure_mode("log")

    rubicon_client.get_project(name="does not exist")

    mock_logger.assert_called_once()

    # cleanup: reset to default
    set_failure_mode("raise")
