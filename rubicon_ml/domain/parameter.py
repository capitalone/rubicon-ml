from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional

from rubicon_ml.domain.mixin import CommentMixin, InitMixin, TagMixin

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(init=False)
class Parameter(CommentMixin, InitMixin, TagMixin):
    """A domain-level parameter.

    name : str
        The parameter's name.
    value : any
        The parameter's value.
    comments : list of str, optional
        Additional text information and observations about the parameter. Defaults to
        `None`.
    created_at : datetime, optional
        The date and time the parameter was created. Defaults to `None` and uses
        `datetime.datetime.now` to generate a UTC timestamp. `created_at` should be
        left as `None` to allow for automatic generation.
    description : str, optional
        A description of the parameter. Defaults to `None`.
    id : str, optional
        The parameter's unique identifier. Defaults to `None` and uses `uuid.uuid4`
        to generate a unique ID. `id` should be left as `None` to allow for automatic
        generation.
    tags : list of str, optional
        The values this parameter is tagged with. Defaults to `None`.
    """

    name: str
    value: Any
    comments: Optional[List[str]] = None
    created_at: Optional["datetime"] = None
    description: Optional[str] = None
    id: Optional[str] = None
    tags: Optional[List[str]] = None

    def __init__(
        self,
        name: str,
        value: Any,
        comments: Optional[List[str]] = None,
        created_at: Optional["datetime"] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        **kwargs,
    ):
        """Initialize this domain parameter."""

        self._check_extra_kwargs(kwargs)

        self.name = name
        self.value = value
        self.comments = comments or []
        self.created_at = self._init_created_at(created_at)
        self.description = description
        self.id = self._init_id(id)
        self.tags = tags or []
