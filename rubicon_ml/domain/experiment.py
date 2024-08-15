from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from rubicon_ml.domain.mixin import CommentMixin, InitMixin, TagMixin

if TYPE_CHECKING:
    from datetime import datetime

    from rubicon_ml.domain.utils import TrainingMetadata


@dataclass(init=False)
class Experiment(CommentMixin, InitMixin, TagMixin):
    """A domain-level experiment.

    project_name : str
        The name of the project the experiment belongs to.
    branch_name : str, optional
        The name of the git branch associated with the experiment. Defaults to `None`.
    comments : list of str, optional
        Additional text information and observations about the experiment. Defaults to
        `None`.
    commit_hash : str, optional
        The hash of the git commit associated with the experiment. Defaults to `None`.
    created_at : datetime, optional
        The date and time the experiment was created. Defaults to `None` and uses
        `datetime.datetime.now` to generate a UTC timestamp. `created_at` should be
        left as `None` to allow for automatic generation.
    description : str, optional
        A description of the experiment. Defaults to `None`.
    id : str, optional
        The experiment's unique identifier. Defaults to `None` and uses `uuid.uuid4`
        to generate a unique ID. `id` should be left as `None` to allow for automatic
        generation.
    model_name : str, optional
        The name of the model associated with the experiment. Defaults to `None`.
    name : str, optional
        The experiment's name. Defaults to `None`.
    tags : list of str, optional
        The values this experiment is tagged with. Defaults to `None`.
    training_metadata : rubicon_ml.domain.utils.TrainingMetadata, optional
        Additional metadata pertaining to any data this experiment was trained on.
        Defaults to `None`.
    """

    project_name: str
    branch_name: Optional[str] = None
    comments: Optional[List[str]] = None
    commit_hash: Optional[str] = None
    created_at: Optional["datetime"] = None
    description: Optional[str] = None
    id: Optional[str] = None
    model_name: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[List[str]] = None
    training_metadata: Optional["TrainingMetadata"] = None

    def __init__(
        self,
        project_name: str,
        branch_name: Optional[str] = None,
        comments: Optional[List[str]] = None,
        commit_hash: Optional[str] = None,
        created_at: Optional["datetime"] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        model_name: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        training_metadata: Optional["TrainingMetadata"] = None,
        **kwargs,
    ):
        """Initialize this domain experiment."""

        self._check_extra_kwargs(kwargs)

        self.project_name = project_name
        self.branch_name = branch_name
        self.comments = comments or []
        self.commit_hash = commit_hash
        self.created_at = self._init_created_at(created_at)
        self.description = description
        self.id = self._init_id(id)
        self.model_name = model_name
        self.name = name
        self.tags = tags or []
        self.training_metadata = training_metadata
