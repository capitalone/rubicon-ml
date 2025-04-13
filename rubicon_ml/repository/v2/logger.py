import logging
from typing import TYPE_CHECKING, Any, Optional

from rubicon_ml.exceptions import RubiconNotImplementedError
from rubicon_ml.repository.utils import json
from rubicon_ml.repository.v2.base import BaseRepository

if TYPE_CHECKING:
    from rubicon_ml.domain import DOMAIN_TYPES


class LoggerRepository(BaseRepository):
    """Builtin `logging.Logger`-based backend."""

    def __init__(
        self,
        log_template: str = "rubicon-ml::{name}::{metadata}",
        logger: Optional[logging.Logger] = None,
        **kwargs,
    ):
        if log_template == "":
            log_template = "{metadata}"

        if "{metadata}" not in log_template:
            raise ValueError("`log_template` must contain '{metadata}' placeholder.")

        self.log_template = log_template
        self.logger = logger if logger else self._make_default_logger()

    def _make_default_logger(self):
        logger = logging.Logger(name="rubicon-ml")
        logger.setLevel(logging.INFO)

        return logger

    # core read/writes

    def read_domain(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    def read_domains(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    def write_domain(self, domain: "DOMAIN_TYPES"):
        self.logger.log(
            self.logger.getEffectiveLevel(),
            self.log_template.format(
                name=domain.__class__.__name__.lower(),
                metadata=json.dumps(domain),
            ),
        )

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
            f"{self.__class__.__name__} does not support logging of binary dataframe data."
        )
