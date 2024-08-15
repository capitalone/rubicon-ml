from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from rubicon_ml.domain.mixin import CommentMixin, InitMixin, TagMixin

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(init=False)
class Feature(CommentMixin, InitMixin, TagMixin):
    """A domain-level feature.

    name : str
        The feature's name.
    comments : list of str, optional
        Additional text information and observations about the feature. Defaults to
        `None`.
    created_at : datetime, optional
        The date and time the feature was created. Defaults to `None` and uses
        `datetime.datetime.now` to generate a UTC timestamp. `created_at` should be
        left as `None` to allow for automatic generation.
    description : str, optional
        A description of the feature. Defaults to `None`.
    id : str, optional
        The feature's unique identifier. Defaults to `None` and uses `uuid.uuid4`
        to generate a unique ID. `id` should be left as `None` to allow for automatic
        generation.
    importance : float, optional
        The feature's calculated importance value. Defaults to `None`.
    tags : list of str, optional
        The values this feature is tagged with. Defaults to `None`.
    """

    name: str
    comments: Optional[List[str]] = None
    created_at: Optional["datetime"] = None
    description: Optional[str] = None
    id: Optional[str] = None
    importance: Optional[float] = None
    tags: Optional[List[str]] = None

    def __init__(
        self,
        name: str,
        comments: Optional[List[str]] = None,
        created_at: Optional["datetime"] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        importance: Optional[float] = None,
        tags: Optional[List[str]] = None,
        **kwargs,
    ):
        """Initialize this domain feature."""

        self._check_extra_kwargs(kwargs)

        self.name = name
        self.comments = comments or []
        self.created_at = self._init_created_at(created_at)
        self.description = description
        self.id = self._init_id(id)
        self.importance = importance
        self.tags = tags or []
