from abc import ABC


class BaseRepository(ABC):
    """Abstract base for rubicon-ml backend repositories."""

    def __init__(*args, **kwargs):
        pass


class BaseRepositoryV2(BaseRepository):
    """`BaseRepository` alias for testing and integration."""

    pass
