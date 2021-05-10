import uuid


def uuid4():
    """Generate a UUID as a string in a single function.

    To be used as a default factory within `dataclasses.field`.
    """
    return str(uuid.uuid4())
