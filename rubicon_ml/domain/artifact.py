from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from rubicon_ml.domain.mixin import CommentMixin, InitMixin, TagMixin

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(init=False)
class Artifact(CommentMixin, InitMixin, TagMixin):
    """A domain-level artifact.

    name : str
        The artifact's name.
    comments : list of str, optional
        Additional text information and observations about the artifact. Defaults to
        `None`.
    created_at : datetime, optional
        The date and time the artifact was created. Defaults to `None` and uses
        `datetime.datetime.now` to generate a UTC timestamp. `created_at` should be
        left as `None` to allow for automatic generation.
    description : str, optional
        A description of the artifact. Defaults to `None`.
    id : str, optional
        The artifact's unique identifier. Defaults to `None` and uses `uuid.uuid4`
        to generate a unique ID. `id` should be left as `None` to allow for automatic
        generation.
    parent_id : str, optional
        The unique identifier of the project or experiment the artifact belongs to.
        Defaults to `None`.
    tags : list of str, optional
        The values this artifact is tagged with. Defaults to `None`.
    """

    name: str
    comments: Optional[List[str]] = None
    created_at: Optional["datetime"] = None
    description: Optional[str] = None
    id: Optional[str] = None
    parent_id: Optional[str] = None
    tags: Optional[List[str]] = None

    def __init__(
        self,
        name: str,
        comments: Optional[List[str]] = None,
        created_at: Optional["datetime"] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        parent_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        **kwargs,
    ):
        """Initialize this domain artifact."""

        self._check_extra_kwargs(kwargs)

        self.name = name
        self.comments = comments or []
        self.created_at = self._init_created_at(created_at)
        self.description = description
        self.id = self._init_id(id)
        self.parent_id = parent_id
        self.tags = tags or []
