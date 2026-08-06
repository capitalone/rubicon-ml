from dataclasses import dataclass, field
from typing import List


@dataclass
class TagUpdate:
    """A domain-level tag update event.

    Represents an append-only record of tags being added to or removed
    from a rubicon-ml entity. Used by the repository layer to persist
    tag operations without mutating entity metadata in place.

    added_tags : list of str, optional
        Tag values that were added. Defaults to an empty list.
    removed_tags : list of str, optional
        Tag values that were removed. Defaults to an empty list.
    """

    added_tags: List[str] = field(default_factory=list)
    removed_tags: List[str] = field(default_factory=list)
