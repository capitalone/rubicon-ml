import os

from rubicon_ml.repository import BaseRepository
from rubicon_ml.repository.utils import json


class LocalRepository(BaseRepository):
    """The local repository persists Rubicon data to a local
    filesystem.

    Parameters
    ----------
    root_dir : str
        Absolute path to the root directory to persist Rubicon
        data to.
    """

    PROTOCOL = "file"

    def _persist_bytes(self, bytes_data, path):
        """Persists the raw bytes `bytes_data` to the local
        path defined by `path`.
        """
        self.filesystem.mkdirs(os.path.dirname(path), exist_ok=True)

        with self.filesystem.open(path, "wb") as f:
            f.write(bytes_data)

    def _persist_domain(self, domain, path):
        """Persists the Rubicon object `domain` to the local
        path defined by `path`.
        """
        json_domain = json.dumps(domain)

        self.filesystem.mkdirs(os.path.dirname(path), exist_ok=True)

        with self.filesystem.open(path, "w") as f:
            f.write(json_domain)
