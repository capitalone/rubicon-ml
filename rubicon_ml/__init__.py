__version__ = "0.15.2"

import importlib

from rubicon_ml.client import (  # noqa: E402
    Artifact,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
    Rubicon,
    RubiconJSON,
)
from rubicon_ml.client.utils.exception_handling import set_failure_mode  # noqa: E402

__all__ = [
    "Artifact",
    "Dataframe",
    "Experiment",
    "Feature",
    "Metric",
    "Parameter",
    "Project",
    "Rubicon",
    "RubiconJSON",
    "set_failure_mode",
]

if importlib.util.find_spec("intake"):
    from rubicon_ml.intake_rubicon.publish import publish  # noqa: E402, F401

    __all__.append("publish")
