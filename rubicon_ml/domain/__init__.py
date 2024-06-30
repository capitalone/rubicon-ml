from __future__ import annotations

from typing import TypeVar, Union

from rubicon_ml.domain import utils
from rubicon_ml.domain.artifact import Artifact
from rubicon_ml.domain.dataframe import Dataframe
from rubicon_ml.domain.experiment import Experiment
from rubicon_ml.domain.feature import Feature
from rubicon_ml.domain.metric import Metric
from rubicon_ml.domain.mixin import CommentUpdate, TagUpdate
from rubicon_ml.domain.parameter import Parameter
from rubicon_ml.domain.project import Project

DOMAIN_TYPES = Union[
    Artifact, CommentUpdate, Dataframe, Experiment, Feature, Metric, Parameter, Project, TagUpdate
]

DomainsVar = TypeVar(
    "DomainsVar",
    Artifact,
    CommentUpdate,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
    TagUpdate,
)


__all__ = [
    "Artifact",
    "CommentUpdate",
    "Dataframe",
    "Experiment",
    "Feature",
    "Metric",
    "Parameter",
    "Project",
    "TagUpdate",
    "utils",
]
