from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from rubicon_ml.domain.mixin import InitMixin

if TYPE_CHECKING:
    from datetime import datetime

    from rubicon_ml.domain.utils import TrainingMetadata


@dataclass(init=False)
class Project(InitMixin):
    """A domain-level project.

    Parameters
    ----------
    name : str
        The project's name.
    created_at : datetime, optional
        The date and time the project was created. Defaults to `None` and uses
        `datetime.datetime.now` to generate a UTC timestamp. `created_at` should be
        left as `None` to allow for automatic generation.
    description : str, optional
        A description of the project. Defaults to `None`.
    github_url : str, optional
        The URL of the GitHub repository associated with this project. Defaults to
        `None`.
    id : str, optional
        The project's unique identifier. Defaults to `None` and uses `uuid.uuid4`
        to generate a unique ID. `id` should be left as `None` to allow for automatic
        generation.
    training_metadata : rubicon_ml.domain.utils.TrainingMetadata, optional
        Additional metadata pertaining to any data this project was trained on.
        Defaults to `None`.
    """

    name: str
    created_at: Optional["datetime"] = None
    description: Optional[str] = None
    github_url: Optional[str] = None
    id: Optional[str] = None
    training_metadata: Optional["TrainingMetadata"] = None

    def __init__(
        self,
        name: str,
        created_at: Optional["datetime"] = None,
        description: Optional[str] = None,
        github_url: Optional[str] = None,
        id: Optional[str] = None,
        training_metadata: Optional["TrainingMetadata"] = None,
        **kwargs,
    ):
        """Initialize this domain project."""

        self._check_extra_kwargs(kwargs)

        self.name = name
        self.created_at = self._init_created_at(created_at)
        self.description = description
        self.github_url = github_url
        self.id = self._init_id(id)
        self.training_metadata = training_metadata
