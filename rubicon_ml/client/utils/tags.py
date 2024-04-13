import re
from typing import List, Optional, Union


class TagContainer(list):
    """List-based container for tags.

    Allows indexing into tags with colons in them by string, like a dictionary.
    """

    def __getitem__(self, index_or_key: Union[int, str]):
        if isinstance(index_or_key, str):
            values = []

            for tag in self:
                key_value = tag.split(":", 1)

                if len(key_value) > 1 and key_value[0] == index_or_key:
                    values.append(key_value[1])

            if len(values) == 0:
                raise KeyError(index_or_key)
            elif len(values) == 1:
                return values[0]
            else:
                return values
        else:
            item = super().__getitem__(index_or_key)

            return TagContainer(item) if isinstance(item, list) else item


def has_tag_requirements(tags: List[str], required_tags: List[str], qtype: str) -> bool:
    """Determines if the `required_tags` are in `tags`.

    Returns True if all `required_tags` are in `tags` and `qtype` is "and" or if
    any `required_tags` are in `tags` and `qtype` is "or". The tags in
    `required_tags` may contain contain wildcard (*) characters.
    """
    qtype_func = any if qtype == "or" else all

    if any(["*" in tag for tag in required_tags]):

        def _wildcard_match(pattern, tag):
            return re.match(f"^{pattern.replace('*', '.*')}$", tag) is not None

        return qtype_func(
            [
                any([_wildcard_match(required_tag, tag) for tag in tags])
                for required_tag in required_tags
            ]
        )
    else:
        return qtype_func(tag in tags for tag in required_tags)


def filter_children(children, tags: List[str], qtype: str, name: Optional[str]):
    """Return the children in `children` with the given tags or name.

    If both are provided, children are first filtered by tags and then by names.
    """
    if len(tags) > 0:
        children = [c for c in children if has_tag_requirements(c.tags, tags, qtype)]

    if name is not None:
        children = [c for c in children if c.name == name]

    return children
