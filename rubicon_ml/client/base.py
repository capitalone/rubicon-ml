from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from rubicon_ml.client import Config
    from rubicon_ml.domain import DOMAIN_TYPES
    from rubicon_ml.repository import RepositoryBase


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

    def is_auto_git_enabled(self) -> bool:
        """Is git enabled for the config."""
        if self._config is None:
            return False

        return self._config.is_auto_git_enabled

    @property
    def repository(self) -> Optional[RepositoryBase]:
        """Get the repository."""
        if self._config is None:
            return None

        return self._config.repository

    @property
    def repositories(self) -> Optional[List[RepositoryBase]]:
        """Deprecated. Use ``.repository`` instead."""
        warnings.warn(
            "`.repositories` is deprecated. Use `.repository` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._config is None:
            return None

        return [self._config.repository]
