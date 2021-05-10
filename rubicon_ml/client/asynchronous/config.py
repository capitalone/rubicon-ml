from rubicon_ml.client import Config as SyncConfig
from rubicon_ml.repository.asynchronous import S3Repository


class Config(SyncConfig):
    """Used to configure asynchronous `rubicon` client objects.

    Configuration can be specified (in order of precedence) by:
        1. environment variables 'PERSISTENCE' and 'ROOT_DIR'
        2. arguments to `__init__`

    Parameters
    ----------
    persistence : str, optional
        The persistence type. Can be one of ["filesystem"].
    root_dir : str, optional
        Absolute or relative filepath. Currently, only s3
        paths are supported asynchronously.
    """

    PERSISTENCE_TYPES = ["filesystem"]
    REPOSITORIES = {
        "filesystem-s3": S3Repository,
    }
