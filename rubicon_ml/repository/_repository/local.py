from rubicon_ml.repository._repository.fsspec import FSSpecRepositoryABC


class LocalRepository(FSSpecRepositoryABC):
    """rubicon-ml backend repository for local filesystems.

    Leverages `fsspec` and its 'file' protocol.
    """

    def _get_protocol(self) -> str:
        return "file"
