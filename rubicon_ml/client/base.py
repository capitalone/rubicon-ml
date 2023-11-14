from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Union

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

    def __init__(self, domain: DOMAIN_TYPES, config: Optional[Union[Config, List[Config]]] = None):
        self._config = config
        self._domain = domain

    def __str__(self) -> str:
        return self._domain.__str__()

    def is_auto_git_enabled(self) -> bool:
        """Is git enabled for any of the configs."""
        if isinstance(self._config, list):
            return any(_config.is_auto_git_enabled for _config in self._config)

        if self._config is None:
            return False

        return self._config.is_auto_git_enabled

    def _raise_rubicon_exception(self, exception: Exception):
        if self.repositories is None or len(self.repositories) > 1:
            raise RubiconException("all configured storage backends failed") from exception
        else:
            raise exception

    @property
    def repository(self) -> Optional[BaseRepository]:
        """Get the repository."""
        if self._config is None:
            return None

        if isinstance(self._config, list):
            if len(self._config) > 1:
                raise ValueError("More than one repository available. Use `.repositories` instead.")
            return self._config[0].repository

        return self._config.repository

    @property
    def repositories(self) -> Optional[List[BaseRepository]]:
        """Get all repositories."""
        if self._config is None:
            return None

        if isinstance(self._config, list):
            return [_config.repository for _config in self._config]

        return [self._config.repository]
