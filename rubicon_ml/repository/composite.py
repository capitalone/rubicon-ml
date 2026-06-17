import copy
import logging
from typing import List

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.base import RepositoryBase

LOGGER = logging.getLogger(__name__)


class CompositeRepository(RepositoryBase):
    """Repository that fans out operations to multiple backends.

    Writes are broadcast to all backends with primary-first semantics:
    the first (primary) backend's errors propagate immediately, while
    secondary backend errors are logged as warnings.

    Reads try each backend in order, returning the first success.

    Parameters
    ----------
    repositories : list of RepositoryBase
        The child repositories to delegate to, in priority order.
        The first repository is the "primary" backend.
    """

    PROTOCOL = "composite"

    def __init__(self, repositories: List[RepositoryBase]):
        if not repositories:
            raise ValueError("CompositeRepository requires at least one child repository.")
        self._repositories = list(repositories)

    @property
    def repositories(self) -> List[RepositoryBase]:
        """The child repositories, in priority order."""
        return list(self._repositories)

    @property
    def root_dir(self):
        """The primary backend's root directory."""
        return self._repositories[0].root_dir

    # -------- Helpers --------

    def _broadcast(self, method_name, *args, **kwargs):
        """Call a method on all backends.

        The primary (first) backend's errors propagate immediately.
        Secondary backend errors are logged as warnings.
        """
        primary, *secondaries = self._repositories

        # Primary — errors propagate
        getattr(primary, method_name)(*args, **kwargs)

        # Secondaries — best-effort
        for repo in secondaries:
            try:
                getattr(repo, method_name)(*args, **kwargs)
            except Exception as e:
                LOGGER.warning(f"{repo.__class__.__name__} failed for '{method_name}': {e}")

    def _broadcast_with_copy(self, method_name, domain_obj, *args, **kwargs):
        """Broadcast a method that receives a domain object as its first argument.

        Deep-copies the domain object for each child to prevent mutation
        side-effects (e.g., WandBRepository mutating experiment.id).
        After broadcast, applies the primary child's mutations back to
        the caller's domain object.
        """
        primary, *secondaries = self._repositories

        # Primary — use a copy, then apply mutations back to the original
        primary_copy = copy.deepcopy(domain_obj)
        getattr(primary, method_name)(primary_copy, *args, **kwargs)

        # Apply primary's mutations back to the caller's object
        domain_obj.__dict__.update(primary_copy.__dict__)

        # Secondaries — best-effort with independent copies
        for repo in secondaries:
            repo_copy = copy.deepcopy(domain_obj)
            try:
                getattr(repo, method_name)(repo_copy, *args, **kwargs)
            except Exception as e:
                LOGGER.warning(f"{repo.__class__.__name__} failed for '{method_name}': {e}")

    def _failover(self, method_name, *args, **kwargs):
        """Try each backend in order, return the first success.

        If all backends fail, raise a ``RubiconException`` chained from
        the first error with details of all failures.
        """
        errors = []

        for repo in self._repositories:
            try:
                return getattr(repo, method_name)(*args, **kwargs)
            except Exception as e:
                errors.append((repo, e))

        # All failed
        details = "; ".join(f"{repo.__class__.__name__}: {e}" for repo, e in errors)
        raise RubiconException(
            f"All {len(errors)} backends failed for '{method_name}': {details}"
        ) from errors[0][1]

    # -------- Abstract Method Implementations --------

    def write_domain(self, domain_obj, project_name, **kwargs):
        self._broadcast("write_domain", domain_obj, project_name, **kwargs)

    def read_domain(self, domain_cls, project_name, **kwargs):
        return self._failover("read_domain", domain_cls, project_name, **kwargs)

    def read_domains(self, domain_cls, project_name=None, **kwargs):
        return self._failover("read_domains", domain_cls, project_name, **kwargs)

    def remove_domain(self, domain_cls, project_name, **kwargs):
        self._broadcast("remove_domain", domain_cls, project_name, **kwargs)

    def write_artifact_data(self, data, project_name, artifact_id, **kwargs):
        self._broadcast("write_artifact_data", data, project_name, artifact_id, **kwargs)

    def read_artifact_data(self, project_name, artifact_id, **kwargs):
        return self._failover("read_artifact_data", project_name, artifact_id, **kwargs)

    def write_dataframe_data(self, df, project_name, dataframe_id, **kwargs):
        self._broadcast("write_dataframe_data", df, project_name, dataframe_id, **kwargs)

    def read_dataframe_data(self, project_name, dataframe_id, **kwargs):
        return self._failover("read_dataframe_data", project_name, dataframe_id, **kwargs)

    # -------- Convenience Overrides --------
    # Only methods that have genuinely different per-backend semantics
    # need explicit overrides here. Everything else flows through the
    # 8 abstract method implementations above.

    def create_project(self, project):
        self._broadcast("create_project", project)

    def create_artifact(self, artifact, data, project_name, experiment_id=None):
        self._broadcast_with_copy(
            "create_artifact", artifact, data, project_name, experiment_id=experiment_id
        )

    def create_dataframe(self, dataframe, data, project_name, experiment_id=None):
        self._broadcast(
            "create_dataframe", dataframe, data, project_name, experiment_id=experiment_id
        )

    def get_projects(self):
        return self._failover("get_projects")

    def _get_artifact_data_path(self, project_name, experiment_id, artifact_id):
        """Delegate to primary backend for artifact data path (used by xgboost/h2o deserialization)."""
        return self._repositories[0]._get_artifact_data_path(
            project_name, experiment_id, artifact_id
        )

    # -------- Special --------

    def finish(self):
        """Call ``finish()`` on all backends that support it."""
        for repo in self._repositories:
            if hasattr(repo, "finish"):
                try:
                    repo.finish()
                except Exception as e:
                    LOGGER.warning(f"{repo.__class__.__name__} failed during finish(): {e}")
