from rubicon_ml.repository._repository.fsspec import FSSpecRepositoryABC


class S3Repository(FSSpecRepositoryABC):
    """rubicon-ml backend repository for Amazon S3 filesystems.

    Leverages `fsspec` and its 's3' protocol (`s3fs`).
    """

    def _get_protocol(self) -> str:
        return "s3"
