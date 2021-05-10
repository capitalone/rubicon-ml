from rubicon_ml.repository import BaseRepository
from rubicon_ml.repository.utils import json


class S3Repository(BaseRepository):
    """The S3 repository persists Rubicon data to a remote
    S3 bucket.

    S3 credentials can be specified via environment variables
    or the credentials file in '~/.aws'.

    Parameters
    ----------
    root_dir : str
        The full S3 path (including 's3://') to persist
        Rubicon data to.
    """

    PROTOCOL = "s3"

    def _persist_bytes(self, bytes_data, path):
        """Persists the raw bytes `bytes_data` to the S3
        bucket defined by `path`.
        """
        with self.filesystem.open(path, "wb") as f:
            f.write(bytes_data)

    def _persist_domain(self, domain, path):
        """Persists the Rubicon object `domain` to the S3
        bucket defined by `path`.
        """
        json_domain = json.dumps(domain)

        with self.filesystem.open(path, "w") as f:
            f.write(json_domain)
