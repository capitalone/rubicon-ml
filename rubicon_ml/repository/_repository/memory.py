from rubicon_ml.repository._repository.fsspec import FSSpecRepositoryABC


class MemoryRepository(FSSpecRepositoryABC):
    """rubicon-ml backend repository for in-memory filesystems.

    Leverages `fsspec` and its 'memory' protocol.
    """

    def __init__(self, root_dir: str, **storage_options):
        """"""
        super().__init__(root_dir, **storage_options)

        self._create_filesystem()

    def _create_filesystem(self):
        """"""
        if not self.filesystem.exists(self.root_dir):
            self.filesystem.mkdir(self.root_dir)

    def _get_protocol(self) -> str:
        """"""
        return "memory"
