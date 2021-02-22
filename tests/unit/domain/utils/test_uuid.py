from uuid import UUID

from rubicon.domain.utils import uuid


def test_returns_valid_uuid():
    uuid_str = uuid.uuid4()

    try:
        UUID(uuid_str)
    except ValueError as e:
        assert f"`{uuid_str}` is not a valid UUID" in e
