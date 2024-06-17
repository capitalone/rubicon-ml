from typing import TYPE_CHECKING, Dict, Union

from rubicon_ml.repository._repository.fsspec import FSSpecRepositoryABC
from rubicon_ml.repository.utils import json

if TYPE_CHECKING:
    from rubicon_ml.types import DOMAIN_TYPES


class S3Repository(FSSpecRepositoryABC):
    """rubicon-ml backend repository for Amazon S3 filesystems.

    Leverages `fsspec` and its 's3' protocol (`s3fs`).
    """

    def __init__(self, root_dir: str, **storage_options):
        """"""
        if not root_dir.startswith("s3://"):
            raise ValueError(f"`{self.__class__.__name__}` `root_dir` must start with 's3://'")

        super().__init__(root_dir, **storage_options)

    def _get_protocol(self) -> str:
        """"""
        return "s3"

    def _write_bytes(self, data: bytes, location: str, *args):
        """"""
        with self.filesystem.open(location, "wb") as file:
            file.write(data)

    def _write_json(self, data: Union[Dict, "DOMAIN_TYPES"], location: str, *args):
        """"""
        with self.filesystem.open(location, "w") as file:
            file.write(json.dumps(data))
