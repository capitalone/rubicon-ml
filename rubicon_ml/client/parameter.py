from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional, Union

from rubicon_ml.client import Base, CommentMixin, TagMixin

if TYPE_CHECKING:
    from rubicon_ml.client import Experiment
    from rubicon_ml.domain import Parameter as ParameterDomain


class Parameter(Base, TagMixin, CommentMixin):
    """A client parameter.

    A `parameter` is an input to an `experiment` (model run)
    that depends on the type of model being used. It affects
    the model's predictions.

    For example, if you were using a random forest classifier,
    'n_estimators' (the number of trees in the forest) could
    be a parameter.

    A `parameter` is logged to an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Parameter
        The parameter domain model.
    parent : rubicon.client.Experiment
        The experiment that the parameter is logged to.
    """

    def __init__(self, domain: ParameterDomain, parent: Experiment):
        super().__init__(domain, parent._config)
        self._parent = parent

        self._domain: ParameterDomain

    @property
    def id(self) -> str:
        """Get the parameter's id."""
        return self._domain.id

    @property
    def name(self) -> Optional[str]:
        """Get the parameter's name."""
        return self._domain.name

    @property
    def value(self) -> Optional[Union[object, float]]:
        """Get the parameter's value."""
        return self._domain.value

    @property
    def description(self) -> Optional[str]:
        """Get the parameter's description."""
        return self._domain.description

    @property
    def created_at(self) -> datetime:
        """Get the time the parameter was created."""
        return self._domain.created_at

    @property
    def parent(self) -> Experiment:
        """Get the parameter's parent client object."""
        return self._parent
