from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from rubicon_ml.client import Base, CommentMixin, TagMixin

if TYPE_CHECKING:
    from rubicon_ml.client import Experiment
    from rubicon_ml.domain import Metric as MetricDomain


class Metric(Base, TagMixin, CommentMixin):
    """A client metric.

    A `metric` is a single-value output of an `experiment` that
    helps evaluate the quality of the model's predictions.

    It can be either a 'score' (value to maximize) or
    a 'loss' (value to minimize).

    A `metric` is logged to an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Metric
        The metric domain model.
    parent : rubicon.client.Experiment
        The experiment that the metric is
        logged to.
    """

    def __init__(self, domain: MetricDomain, parent: Experiment):
        super().__init__(domain, parent._config)

        self._domain: MetricDomain

        self._data = None
        self._parent = parent

    @property
    def id(self) -> str:
        """Get the metric's id."""
        return self._domain.id

    @property
    def name(self) -> Optional[str]:
        """Get the metric's name."""
        return self._domain.name

    @property
    def value(self):
        """Get the metric's value."""
        return self._domain.value

    @property
    def directionality(self) -> str:
        """Get the metric's directionality."""
        return self._domain.directionality

    @property
    def description(self) -> Optional[str]:
        """Get the metric's description."""
        return self._domain.description

    @property
    def created_at(self) -> datetime:
        """Get the metric's created_at."""
        return self._domain.created_at

    @property
    def parent(self) -> Experiment:
        """Get the metric's parent client object."""
        return self._parent
