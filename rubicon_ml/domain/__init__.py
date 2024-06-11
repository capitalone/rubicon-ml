from __future__ import annotations

from typing import TypeVar, Union

from rubicon_ml.domain import utils
from rubicon_ml.domain.artifact import Artifact
from rubicon_ml.domain.dataframe import Dataframe
from rubicon_ml.domain.experiment import Experiment
from rubicon_ml.domain.feature import Feature
from rubicon_ml.domain.metric import Metric
from rubicon_ml.domain.parameter import Parameter
from rubicon_ml.domain.project import Project

DOMAIN_TYPES = Union[Artifact, Dataframe, Experiment, Feature, Metric, Parameter, Project]

DomainsVar = TypeVar(
    "DomainsVar", Artifact, Dataframe, Experiment, Feature, Metric, Parameter, Project
)


__all__ = [
    "Artifact",
    "Dataframe",
    "Experiment",
    "Feature",
    "Metric",
    "Parameter",
    "Project",
    "utils",
]
