import warnings

from rubicon_ml import __version__  # noqa: F401
from rubicon_ml import (
    Artifact,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
    Rubicon,
)

rename_warning_message = (
    "`rubicon` will be renamed to `rubicon_ml` in version 0.3.0. You "
    "can switch your imports now: `from rubicon_ml import Rubicon`."
)
warnings.warn(rename_warning_message)

__all__ = [
    "Artifact",
    "Dataframe",
    "Experiment",
    "Feature",
    "Metric",
    "Parameter",
    "Project",
    "Rubicon",
]
