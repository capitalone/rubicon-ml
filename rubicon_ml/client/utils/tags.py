def has_tag_requirements(tags, required_tags, qtype):
    """
    Returns True if `tags` meets the requirements based on
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
