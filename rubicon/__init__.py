import warnings

from rubicon.client import (
    Artifact,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
    Rubicon,
)

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

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
