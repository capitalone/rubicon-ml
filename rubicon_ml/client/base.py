from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from rubicon_ml.exceptions import RubiconException

if TYPE_CHECKING:
    from rubicon_ml.client import Config
    from rubicon_ml.domain import DOMAIN_TYPES
    from rubicon_ml.repository import BaseRepository


class Base:
    """The base object for all top-level client objects.

    Parameters
    ----------
    domain : one of rubicon.domain.*
        The top-level object's domain instance.
    config : rubicon.client.Config, optional
        The config, which injects the repository to use.
    """

    def __init__(self, domain: DOMAIN_TYPES, config: Optional[Config] = None):
        self._config = config
        self._domain = domain

    def __str__(self) -> str:
        return self._domain.__str__()

    def _raise_rubicon_exception(self, exception: Exception):
        if len(self.repositories) > 1:
            raise RubiconException("all configured storage backends failed") from exception
        else:
            raise exception

    @property
    def repository(self) -> Optional[BaseRepository]:
        return self._config.repository if self._config is not None else None

    @property
    def repositories(self) -> Optional[List[BaseRepository]]:
        if self._config is None:
            return None

        if hasattr(self._config, "repositories"):
            return self._config.repositories
        else:
            return [self._config.repository]
