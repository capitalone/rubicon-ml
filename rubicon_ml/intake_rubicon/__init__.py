import importlib
import logging
import warnings

LOGGER = logging.getLogger(__name__)

_DEPRECATION_MSG = "`intake_rubicon` is deprecated and will be removed in an upcoming release."
_INSTALL_MSG = (
    "`intake` is required to use `intake_rubicon`. Please install it with "
    "`pip install intake` or `pip install rubicon-ml[intake]`."
)


def _check_intake():
    """Check that intake is installed and emit a deprecation warning."""
    warnings.warn(_DEPRECATION_MSG, category=DeprecationWarning, stacklevel=3)
    LOGGER.warning(_DEPRECATION_MSG)

    if not importlib.util.find_spec("intake"):
        raise ImportError(_INSTALL_MSG)


if importlib.util.find_spec("intake"):
    import intake  # noqa F401

    from rubicon_ml.intake_rubicon.experiment import ExperimentSource
    from rubicon_ml.intake_rubicon.viz import (
        DataframePlotDataSource,
        ExperimentsTableDataSource,
        MetricCorrelationPlotDataSource,
        MetricListComparisonDataSource,
    )

    __all__ = [
        "ExperimentSource",
        "ExperimentsTableDataSource",
        "MetricCorrelationPlotDataSource",
        "DataframePlotDataSource",
        "MetricListComparisonDataSource",
    ]
else:
    __all__ = []
