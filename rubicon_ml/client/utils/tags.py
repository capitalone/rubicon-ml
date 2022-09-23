def has_tag_requirements(tags, required_tags, qtype):
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
