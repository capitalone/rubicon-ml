from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Literal, Optional

from rubicon_ml.domain.mixin import CommentMixin, InitMixin, TagMixin

if TYPE_CHECKING:
    from datetime import datetime

DIRECTIONALITY_VALUES = ["score", "loss"]


@dataclass(init=False)
class Metric(CommentMixin, InitMixin, TagMixin):
    """A domain-level metric.

    name : str
        The metric's name.
    value : any
        The metric's value.
    comments : list of str, optional
        Additional text information and observations about the metric. Defaults to
        `None`.
    created_at : datetime, optional
        The date and time the metric was created. Defaults to `None` and uses
        `datetime.datetime.now` to generate a UTC timestamp. `created_at` should be
        left as `None` to allow for automatic generation.
    description : str, optional
        A description of the metric. Defaults to `None`.
    directionality : "score" or "loss", optional
        "score" to indicate greater values are better, "loss" to indiciate smaller
        values are better. Defaults to "score".
    id : str, optional
        The metric's unique identifier. Defaults to `None` and uses `uuid.uuid4`
        to generate a unique ID. `id` should be left as `None` to allow for automatic
        generation.
    tags : list of str, optional
        The values this metric is tagged with. Defaults to `None`.
    """

    name: str
    value: Any
    comments: Optional[List[str]] = None
    created_at: Optional["datetime"] = None
    description: Optional[str] = None
    directionality: Literal["score", "loss"] = "score"
    id: Optional[str] = None
    tags: Optional[List[str]] = None

    def __init__(
        self,
        name: str,
        value: Any,
        comments: Optional[List[str]] = None,
        created_at: Optional["datetime"] = None,
        description: Optional[str] = None,
        directionality: Literal["score", "loss"] = "score",
        id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        **kwargs,
    ):
        """Initialize this domain metric."""

        if directionality not in DIRECTIONALITY_VALUES:
            raise ValueError(f"`directionality` must be one of {DIRECTIONALITY_VALUES}")

        self._check_extra_kwargs(kwargs)

        self.name = name
        self.value = value
        self.comments = comments or []
        self.created_at = self._init_created_at(created_at)
        self.description = description
        self.directionality = directionality
        self.id = self._init_id(id)
        self.tags = tags or []
