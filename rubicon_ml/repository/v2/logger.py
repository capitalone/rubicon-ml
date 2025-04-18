import logging
from typing import TYPE_CHECKING, Optional

from rubicon_ml.repository.utils import json
from rubicon_ml.repository.v2.base import BaseRepository
from rubicon_ml.repository.v2.exceptions import DomainOnlyMixin, WriteOnlyMixin

if TYPE_CHECKING:
    from rubicon_ml.domain import DOMAIN_TYPES

LOGGER = logging.Logger(__name__)


class LoggerRepository(DomainOnlyMixin, WriteOnlyMixin, BaseRepository):
    """`logging.Logger`-based backend."""

    def __init__(
        self,
        log_template: str = "{domain_name}::{metadata}",
        logger: Optional[logging.Logger] = None,
        **kwargs,
    ):
        if log_template == "":
            log_template = "{metadata}"

        if "{metadata}" not in log_template:
            raise ValueError("`log_template` must contain '{metadata}' placeholder.")

        self.log_template = log_template
        self.logger = logger if logger else self._make_default_logger()

    def _make_default_logger(self) -> logging.Logger:
        default_log_level = logging.WARN
        default_log_name = "rubicon-ml"

        LOGGER.info(
            f"setting default logger with name '{default_log_name}' and level "
            f"`{default_log_level}`."
        )

        logger = logging.Logger(name=default_log_name)
        logger.setLevel(default_log_level)

        formatter = logging.Formatter("%(name)s::%(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    # core read/writes

    def write_domain(
        self,
        domain: "DOMAIN_TYPES",
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ):
        self.logger.log(
            self.logger.getEffectiveLevel(),
            self.log_template.format(
                domain_name=domain.__class__.__name__.lower(),
                metadata=json.dumps(domain),
            ),
        )
