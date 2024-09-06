import importlib
import sys
from unittest.mock import patch

import pytest

from rubicon_ml import workflow
from rubicon_ml.workflow import prefect
from rubicon_ml.workflow.prefect import _check_for_prefect_extras, tasks


def test_deprecations():
    with pytest.deprecated_call():
        importlib.reload(workflow)

    with pytest.deprecated_call():
        importlib.reload(prefect)

    with pytest.deprecated_call():
        importlib.reload(tasks)


def test_missing_prefect_extra_raises_error():
    install_prefect_message = "Install `prefect` with `pip install rubicon[prefect]`."

    with patch.dict(sys.modules, {"prefect": None}):
        with pytest.raises(ImportError) as e:
            _check_for_prefect_extras()

        assert str(e.value) in install_prefect_message
