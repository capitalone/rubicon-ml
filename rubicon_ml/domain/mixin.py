from typing import List


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
