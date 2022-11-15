from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from rubicon_ml.client import (  # noqa: E402
    Artifact,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
    Rubicon,
)
from rubicon_ml.client.utils.exception_handling import set_failure_mode  # noqa: E402
from rubicon_ml.intake_rubicon.publish import publish  # noqa: E402

__all__ = [
    "Artifact",
    "Dataframe",
    "Experiment",
    "Feature",
    "Metric",
    "Parameter",
    "Project",
    "publish",
    "Rubicon",
    "set_failure_mode",
]
