from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from rubicon_ml.domain.mixin import CommentMixin, InitMixin, TagMixin

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(init=False)
class Dataframe(CommentMixin, InitMixin, TagMixin):
    """A domain-level dataframe.

    comments : list of str, optional
        Additional text information and observations about the dataframe. Defaults to
        `None`.
    created_at : datetime, optional
        The date and time the dataframe was created. Defaults to `None` and uses
        `datetime.datetime.now` to generate a UTC timestamp. `created_at` should be
        left as `None` to allow for automatic generation.
    description : str, optional
        A description of the dataframe. Defaults to `None`.
    id : str, optional
        The dataframe's unique identifier. Defaults to `None` and uses `uuid.uuid4`
        to generate a unique ID. `id` should be left as `None` to allow for automatic
        generation.
    name : str, optional
        The dataframe's name. Defaults to `None`.
    parent_id : str, optional
        The unique identifier of the project or experiment the dataframe belongs to.
        Defaults to `None`.
    tags : list of str, optional
        The values this dataframe is tagged with. Defaults to `None`.
    """

    comments: Optional[List[str]] = None
    created_at: Optional["datetime"] = None
    description: Optional[str] = None
    id: Optional[str] = None
    name: Optional[str] = None
    parent_id: Optional[str] = None
    tags: Optional[List[str]] = None

    def __init__(
        self,
        comments: Optional[List[str]] = None,
        created_at: Optional["datetime"] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        name: Optional[str] = None,
        parent_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        **kwargs,
    ):
        """Initialize this doimain dataframe."""

        self._check_extra_kwargs(kwargs)

        self.comments = comments or []
        self.created_at = self._init_created_at(created_at)
        self.description = description
        self.id = self._init_id(id)
        self.name = name
        self.parent_id = parent_id
        self.tags = tags or []
