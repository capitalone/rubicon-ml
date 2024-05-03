from rubicon_ml.repository._repository.fsspec import FSSpecRepositoryABC
from rubicon_ml.repository.utils import json


class S3Repository(FSSpecRepositoryABC):
    """rubicon-ml backend repository for Amazon S3 filesystems.

    Leverages `fsspec` and its 's3' protocol (`s3fs`).
    """

    def _get_path(self) -> str:
        """"""
        return "s3://"

    def _get_protocol(self) -> str:
        """"""
        return "s3"

    def write_bytes(self, data, *args):
        """"""
        path = self._get_path(*args)

        self._write(data, path, "wb", *args, make_dir=False)

    def write_json(self, data, *args):
        """"""
        path = self._get_path(*args)

        self._write(json.dumps(data), path, *args, make_dir=False)
