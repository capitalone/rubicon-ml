from rubicon_ml.repository.asynchronous import AsynchronousBaseRepository
from rubicon_ml.repository.utils import json


class S3Repository(AsynchronousBaseRepository):
    """The asynchronous S3 repository uses `asyncio` to
    persist Rubicon data to a remote S3 bucket.

    S3 credentials can be specified via environment variables
    or the credentials file in '~/.aws'.

    Parameters
    ----------
    root_dir : str
        The full S3 path (including 's3://') to persist
        Rubicon data to.
    loop : asyncio.unix_events._UnixSelectorEventLoop, optional
        The event loop the asynchronous calling program is running on.
        It should not be necessary to provide this parameter in
        standard asynchronous operating cases.
    """

    PROTOCOL = "s3"

    async def _connect(self):
        """Asynchronously connect to the underlying S3 persistence layer.

        Note
        ----
        This function must be run before any other that reaches
        out to S3. It is implicitly called by such functions.
        """
        await self.filesystem._connect()

    async def _persist_bytes(self, bytes_data, path):
        """Asynchronously persists the raw bytes `bytes_data`
        to the S3 bucket defined by `path`.
        """
        await self.filesystem._pipe_file(path, bytes_data)

    async def _persist_domain(self, domain, path):
        """Asynchronously persists the Rubicon object `domain`
        to the S3 bucket defined by `path`.
        """
        await self.filesystem._pipe_file(path, json.dumps(domain))
