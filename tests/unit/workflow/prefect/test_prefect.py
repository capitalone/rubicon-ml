import sys
from unittest.mock import patch

import pytest

from rubicon_ml.workflow.prefect import _check_for_prefect_extras


def test_missing_prefect_extra_raises_error():
    install_prefect_message = "Install `prefect` with `pip install rubicon[prefect]`."

    with patch.dict(sys.modules, {"prefect": None}):
        with pytest.raises(ImportError) as e:
            _check_for_prefect_extras()

        assert str(e.value) in install_prefect_message
