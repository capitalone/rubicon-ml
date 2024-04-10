from typing import List


class TagContainer(list):
    """List-based container for tags that allows indexing into tags
    with colons in them by string, like a dictionary.
    """

    def __getitem__(self, index_or_key):
        if isinstance(index_or_key, str):
            for tag in self:
                key_value = tag.split(":", 1)

                if len(key_value) > 1 and key_value[0] == index_or_key:
                    return key_value[1]

            raise KeyError(index_or_key)
        else:
            item = super().__getitem__(index_or_key)
            item = TagContainer(item) if isinstance(item, list) else item

            return item


def has_tag_requirements(tags: List[str], required_tags: List[str], qtype: str) -> bool:
    """Returns True if `tags` meets the requirements based on
    the values of `required_tags` and `qtype`. False otherwise.
    """
    has_tag_requirements = False

    tag_intersection = set(required_tags).intersection(set(tags))
    if qtype == "or":
        if len(tag_intersection) > 0:
            has_tag_requirements = True
    if qtype == "and":
        if len(tag_intersection) == len(required_tags):
            has_tag_requirements = True

    return has_tag_requirements


def filter_children(children, tags, qtype, name):
    """Filters the provided rubicon objects by `tags` using
    query type `qtype` and by `name`.
    """
    filtered_children = children

    if len(tags) > 0:
        filtered_children = [
            c for c in filtered_children if has_tag_requirements(c.tags, tags, qtype)
        ]
    if name is not None:
        filtered_children = [c for c in filtered_children if c.name == name]

    return filtered_children
