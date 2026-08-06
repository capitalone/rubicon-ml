from dataclasses import dataclass, field
from typing import List


@dataclass
class CommentUpdate:
    """A domain-level comment update event.

    Represents an append-only record of comments being added to or removed
    from a rubicon-ml entity. Used by the repository layer to persist
    comment operations without mutating entity metadata in place.

    added_comments : list of str, optional
        Comment values that were added. Defaults to an empty list.
    removed_comments : list of str, optional
        Comment values that were removed. Defaults to an empty list.
    """

    added_comments: List[str] = field(default_factory=list)
    removed_comments: List[str] = field(default_factory=list)
