import sys
from unittest.mock import patch

import pytest

from rubicon_ml.ui import _check_for_ui_extras


def test_missing_ui_extra_raises_error():
    install_ui_message = "Install the packages required for the UI with `pip install rubicon[ui]`."

    with patch.dict(sys.modules, {"dash": None, "dash-html-components": None}):
        with pytest.raises(ImportError) as e:
            _check_for_ui_extras()

        assert install_ui_message in str(e.value)
