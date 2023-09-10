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
