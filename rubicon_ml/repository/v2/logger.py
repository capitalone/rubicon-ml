import json
import logging
from typing import TYPE_CHECKING, Any, Optional

from rubcion_ml.exceptions import RubiconNotImplementedError

from rubicon_ml.repository.v2.base import BaseRepository

if TYPE_CHECKING:
    from rubicon_ml.domain import DOMAIN_TYPES


class LoggerRepository(BaseRepository):
    """Builtin `logging.Logger`-based backend."""

    def __init__(self, logger: Optional[logging.Logger] = None, **kwargs):
        self.logger = logger if logger else self._make_default_logger()

    def _make_default_logger(self):
        return logging.Logger(name="rubicon-ml")

    # core read/writes

    def read_domain(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    def read_domains(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    def write_domain(self, domain: "DOMAIN_TYPES"):
        self.logger.info(json.dumps(domain))

    # binary read/writes

    def read_artifact_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    def write_artifact_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(
            f"{self.__class__.__name__} does not support logging of binary artifact data."
        )

    def stream_artifact_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(
            f"{self.__class__.__name__} does not support logging of binary artifact data."
        )

    def read_dataframe_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    def write_dataframe_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(
            f"{self.__class__.__name__} does not support logging of dataframe data."
        )
