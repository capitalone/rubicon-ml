import datetime
import logging
import uuid
from typing import Dict, List, Optional

LOGGER = logging.getLogger()


class CommentMixin:
    """Adds comment support to a domain model."""

    def add_comments(self, comments: List[str]):
        """
        Add new comments to this model.

        Parameters
        ----------
        comments : List[str]
            A list of string comments to add to the domain model.
        """
        self.comments.extend(comments)

    def remove_comments(self, comments: List[str]):
        """
        Remove comments from this model.

        Parameters
        ----------
        comments : List[str]
            A list of string comments to remove from this domain model.
        """
        self.comments = list(set(self.comments).difference(set(comments)))


class InitMixin:
    """Adds helpers for initializing default values."""

    def _check_extra_kwargs(self, kwargs: Dict):
        """Warn if extra kwargs are given when initialized.

        Replaces default `dataclass` behavior of erroring on unexpected kwargs.
        """
        if kwargs:
            LOGGER.warning(
                f"{self.__class__.__name__}.__init__() got an unexpected keyword "
                f"argument(s): `{'`, `'.join([key for key in kwargs])}`"
            )

    def _init_created_at(self, created_at: Optional[datetime.datetime]) -> datetime.datetime:
        """Initialize the `created_at` attribute to the current time if necessary.

        `datetime.UTC` was added and `datetime.utcnow` was deprecated in Python 3.11.
        """
        if created_at is None:
            try:
                return datetime.datetime.now(datetime.UTC)
            except AttributeError:
                return datetime.datetime.utcnow()

        return created_at

    def _init_id(self, id: Optional[str]) -> str:
        """Initialize the `id` attribute to a new UUID if necessary."""
        if id is None:
            return str(uuid.uuid4())

        return id


class TagMixin:
    """Adds tagging support to a domain model."""

    def add_tags(self, tags: List[str]):
        """
        Add new tags to this model.

        Parameters
        ----------
        tags : List[str]
            A list of string tags to add to the domain model.
        """
        self.tags = list(set(self.tags).union(set(tags)))

    def remove_tags(self, tags: List[str]):
        """
        Remove tags from this model.

        Parameters
        ----------
        tags : List[str]
            A list of string tags to remove from this domain model.
        """
        self.tags = list(set(self.tags).difference(set(tags)))
