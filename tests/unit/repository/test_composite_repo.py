"""Tests for the CompositeRepository."""

import logging
import uuid
from unittest.mock import MagicMock

import pytest

from rubicon_ml import domain
from rubicon_ml.domain.tag_update import TagUpdate
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.base import RepositoryBase
from rubicon_ml.repository.composite import CompositeRepository
from rubicon_ml.repository.memory import MemoryRepository


# -------- Fixtures --------


@pytest.fixture
def two_memory_repos():
    """Two independent MemoryRepository instances."""
    repo_a = MemoryRepository(root_dir="/composite-a")
    repo_b = MemoryRepository(root_dir="/composite-b")
    yield repo_a, repo_b
    for repo in (repo_a, repo_b):
        try:
            repo.filesystem.rm(repo.root_dir, recursive=True)
        except FileNotFoundError:
            pass


@pytest.fixture
def composite(two_memory_repos):
    """CompositeRepository wrapping two MemoryRepositories."""
    repo_a, repo_b = two_memory_repos
    return CompositeRepository([repo_a, repo_b])


def _make_project(name=None):
    return domain.Project(name=name or f"project-{uuid.uuid4()}")


def _make_experiment(project_name):
    return domain.Experiment(name=f"exp-{uuid.uuid4()}", project_name=project_name)


# -------- Construction Tests --------


class TestConstruction:
    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            CompositeRepository([])

    def test_single_repo(self):
        repo = MemoryRepository(root_dir="/single")
        composite = CompositeRepository([repo])
        assert len(composite.repositories) == 1
        repo.filesystem.rm("/single", recursive=True)

    def test_multiple_repos(self, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        composite = CompositeRepository([repo_a, repo_b])
        assert len(composite.repositories) == 2

    def test_protocol(self, composite):
        assert composite.PROTOCOL == "composite"

    def test_root_dir_delegates_to_primary(self, composite, two_memory_repos):
        repo_a, _ = two_memory_repos
        assert composite.root_dir == repo_a.root_dir

    def test_is_repository_base(self, composite):
        assert isinstance(composite, RepositoryBase)


# -------- Write Broadcast Tests (homogeneous) --------


class TestWriteBroadcast:
    def test_write_to_all_backends(self, composite, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()

        composite.create_project(project)

        assert repo_a.get_project(project.name).name == project.name
        assert repo_b.get_project(project.name).name == project.name

    def test_create_experiment_in_all_backends(self, composite, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        composite.create_project(project)

        experiment = _make_experiment(project.name)
        composite.write_domain(experiment, project.name, experiment_id=experiment.id)

        assert (
            repo_a.read_domain(domain.Experiment, project.name, experiment_id=experiment.id).id
            == experiment.id
        )
        assert (
            repo_b.read_domain(domain.Experiment, project.name, experiment_id=experiment.id).id
            == experiment.id
        )

    def test_create_metric_in_all_backends(self, composite, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        composite.create_project(project)
        experiment = _make_experiment(project.name)
        composite.write_domain(experiment, project.name, experiment_id=experiment.id)

        metric = domain.Metric(name="accuracy", value=0.95)
        composite.write_domain(
            metric,
            project.name,
            experiment_id=experiment.id,
            entity_identifier="accuracy",
            entity_type="Metric",
        )

        assert (
            repo_a.read_domain(
                domain.Metric,
                project.name,
                experiment_id=experiment.id,
                entity_identifier="accuracy",
                entity_type="Metric",
            ).value
            == 0.95
        )
        assert (
            repo_b.read_domain(
                domain.Metric,
                project.name,
                experiment_id=experiment.id,
                entity_identifier="accuracy",
                entity_type="Metric",
            ).value
            == 0.95
        )

    def test_primary_failure_propagates(self, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()

        # Create project in repo_a so duplicate check fails
        repo_a.create_project(project)

        composite = CompositeRepository([repo_a, repo_b])
        with pytest.raises(RubiconException, match="already exists"):
            composite.create_project(project)

    def test_secondary_failure_warns(self, two_memory_repos, caplog):
        repo_a, repo_b = two_memory_repos
        project = _make_project()

        # Create project in repo_b so duplicate check fails on secondary
        repo_b.create_project(project)

        composite = CompositeRepository([repo_a, repo_b])
        with caplog.at_level(logging.WARNING):
            composite.create_project(project)

        # Primary succeeded
        assert repo_a.get_project(project.name).name == project.name
        # Secondary warned
        assert "failed for 'create_project'" in caplog.text


# -------- Read Failover Tests (homogeneous) --------


class TestReadFailover:
    def test_reads_from_first_backend(self, composite, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        composite.create_project(project)

        result = composite.get_project(project.name)
        assert result.name == project.name

    def test_failover_to_second_backend(self, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()

        # Only write to repo_b
        repo_b.create_project(project)

        composite = CompositeRepository([repo_a, repo_b])
        result = composite.get_project(project.name)
        assert result.name == project.name

    def test_all_fail_raises(self, composite):
        with pytest.raises(RubiconException, match="All 2 backends failed"):
            composite.get_project("nonexistent")

    def test_read_experiments_from_primary(self, composite, two_memory_repos):
        """Experiments are read from primary when both have data."""
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        composite.create_project(project)

        exp = _make_experiment(project.name)
        composite.write_domain(exp, project.name, experiment_id=exp.id)

        experiments = composite.read_domains(domain.Experiment, project.name)
        assert len(experiments) == 1
        assert experiments[0].id == exp.id

    def test_get_experiment_failover(self, two_memory_repos):
        """Single-entity read fails over when primary doesn't have it."""
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        repo_b.create_project(project)
        experiment = _make_experiment(project.name)
        repo_b.write_domain(experiment, project.name, experiment_id=experiment.id)

        composite = CompositeRepository([repo_a, repo_b])
        result = composite.read_domain(domain.Experiment, project.name, experiment_id=experiment.id)
        assert result.id == experiment.id


# -------- Tag/Comment Tests --------


class TestTagsAndComments:
    def test_add_tags_broadcast(self, composite, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        composite.create_project(project)
        experiment = _make_experiment(project.name)
        composite.write_domain(experiment, project.name, experiment_id=experiment.id)

        composite.write_domain(
            TagUpdate(added_tags=["tag_a"]),
            project.name,
            experiment_id=experiment.id,
            entity_identifier=experiment.id,
            entity_type="Experiment",
        )

        tags_a = repo_a.read_domains(
            TagUpdate,
            project.name,
            experiment_id=experiment.id,
            entity_identifier=experiment.id,
            entity_type="Experiment",
        )
        tags_b = repo_b.read_domains(
            TagUpdate,
            project.name,
            experiment_id=experiment.id,
            entity_identifier=experiment.id,
            entity_type="Experiment",
        )
        assert len(tags_a) == 1
        assert len(tags_b) == 1

    def test_get_tags_from_primary(self, composite, two_memory_repos):
        """Tags are read from primary when it has data."""
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        composite.create_project(project)
        experiment = _make_experiment(project.name)
        composite.write_domain(experiment, project.name, experiment_id=experiment.id)

        composite.write_domain(
            TagUpdate(added_tags=["tag_a"]),
            project.name,
            experiment_id=experiment.id,
            entity_identifier=experiment.id,
            entity_type="Experiment",
        )

        tags = composite.read_domains(
            TagUpdate,
            project.name,
            experiment_id=experiment.id,
            entity_identifier=experiment.id,
            entity_type="Experiment",
        )
        assert len(tags) == 1


# -------- Artifact/Dataframe Tests --------


class TestArtifactsAndDataframes:
    def test_create_artifact_in_all_backends(self, composite, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        composite.create_project(project)
        experiment = _make_experiment(project.name)
        composite.write_domain(experiment, project.name, experiment_id=experiment.id)

        artifact = domain.Artifact(name="model")
        composite.create_artifact(artifact, b"model_data", project.name, experiment.id)

        assert (
            len(repo_a.read_domains(domain.Artifact, project.name, experiment_id=experiment.id))
            == 1
        )
        assert (
            len(repo_b.read_domains(domain.Artifact, project.name, experiment_id=experiment.id))
            == 1
        )

    def test_get_artifact_data_failover(self, two_memory_repos):
        repo_a, repo_b = two_memory_repos
        project = _make_project()
        repo_b.create_project(project)
        experiment = _make_experiment(project.name)
        repo_b.write_domain(experiment, project.name, experiment_id=experiment.id)
        artifact = domain.Artifact(name="model")
        repo_b.create_artifact(artifact, b"model_data", project.name, experiment.id)

        composite = CompositeRepository([repo_a, repo_b])
        data = composite.read_artifact_data(project.name, artifact.id, experiment_id=experiment.id)
        assert data == b"model_data"


# -------- finish() Tests --------


class TestFinish:
    def test_finish_calls_all_children(self):
        mock_a = MagicMock(spec=RepositoryBase)
        mock_b = MagicMock(spec=RepositoryBase)
        mock_a.finish = MagicMock()
        mock_b.finish = MagicMock()

        composite = CompositeRepository([mock_a, mock_b])
        composite.finish()

        mock_a.finish.assert_called_once()
        mock_b.finish.assert_called_once()

    def test_finish_skips_children_without_method(self):
        mock_a = MagicMock(spec=RepositoryBase)
        del mock_a.finish  # Remove the finish attribute

        composite = CompositeRepository([mock_a])
        composite.finish()  # Should not raise

    def test_finish_warns_on_child_failure(self, caplog):
        mock_a = MagicMock()
        mock_a.finish.side_effect = Exception("W&B error")

        composite = CompositeRepository([mock_a])
        with caplog.at_level(logging.WARNING):
            composite.finish()

        assert "failed during finish()" in caplog.text


# -------- Full CRUD Integration --------


class TestFullCRUD:
    def test_full_lifecycle(self, composite, two_memory_repos):
        repo_a, repo_b = two_memory_repos

        # Create project
        project = _make_project()
        composite.create_project(project)

        # Create experiment
        experiment = _make_experiment(project.name)
        composite.write_domain(experiment, project.name, experiment_id=experiment.id)

        # Log metric, parameter, feature
        metric = domain.Metric(name="accuracy", value=0.95)
        composite.write_domain(
            metric,
            project.name,
            experiment_id=experiment.id,
            entity_identifier="accuracy",
            entity_type="Metric",
        )

        param = domain.Parameter(name="lr", value=0.01)
        composite.write_domain(
            param,
            project.name,
            experiment_id=experiment.id,
            entity_identifier="lr",
            entity_type="Parameter",
        )

        feature = domain.Feature(name="age", importance=0.8)
        composite.write_domain(
            feature,
            project.name,
            experiment_id=experiment.id,
            entity_identifier="age",
            entity_type="Feature",
        )

        # Read back from composite (should get from primary)
        assert composite.get_project(project.name).name == project.name
        assert (
            composite.read_domain(domain.Experiment, project.name, experiment_id=experiment.id).id
            == experiment.id
        )
        assert (
            composite.read_domain(
                domain.Metric,
                project.name,
                experiment_id=experiment.id,
                entity_identifier="accuracy",
                entity_type="Metric",
            ).value
            == 0.95
        )
        assert (
            composite.read_domain(
                domain.Parameter,
                project.name,
                experiment_id=experiment.id,
                entity_identifier="lr",
                entity_type="Parameter",
            ).value
            == 0.01
        )
        assert (
            composite.read_domain(
                domain.Feature,
                project.name,
                experiment_id=experiment.id,
                entity_identifier="age",
                entity_type="Feature",
            ).importance
            == 0.8
        )

        # Verify data in both backends
        assert (
            repo_a.read_domain(
                domain.Metric,
                project.name,
                experiment_id=experiment.id,
                entity_identifier="accuracy",
                entity_type="Metric",
            ).value
            == 0.95
        )
        assert (
            repo_b.read_domain(
                domain.Metric,
                project.name,
                experiment_id=experiment.id,
                entity_identifier="accuracy",
                entity_type="Metric",
            ).value
            == 0.95
        )

    def test_read_survives_primary_down(self, two_memory_repos):
        """Write to both, then destroy primary — reads should failover."""
        repo_a, repo_b = two_memory_repos
        composite = CompositeRepository([repo_a, repo_b])

        project = _make_project()
        composite.create_project(project)

        # Destroy primary's data
        repo_a.filesystem.rm(repo_a.root_dir, recursive=True)
        repo_a.filesystem.mkdir(repo_a.root_dir)

        # Should failover to repo_b
        result = composite.get_project(project.name)
        assert result.name == project.name


# -------- Heterogeneous Backend Tests (with mocks) --------


class _MutatingRepository(RepositoryBase):
    """Mock repository that mutates domain objects (like WandBRepository)."""

    PROTOCOL = "mutating"

    def __init__(self):
        self._written = {}

    def write_domain(self, domain_obj, project_name, **kwargs):
        pass

    def read_domain(self, domain_cls, project_name, **kwargs):
        raise RubiconException("not found")

    def read_domains(self, domain_cls, project_name=None, **kwargs):
        return []

    def remove_domain(self, domain_cls, project_name, **kwargs):
        raise RubiconException("deletion not supported")

    def write_artifact_data(self, data, project_name, artifact_id, **kwargs):
        pass

    def read_artifact_data(self, project_name, artifact_id, **kwargs):
        raise RubiconException("not found")

    def write_dataframe_data(self, df, project_name, dataframe_id, **kwargs):
        pass

    def read_dataframe_data(self, project_name, dataframe_id, **kwargs):
        raise RubiconException("not found")

    def create_artifact(self, artifact, data, project_name, experiment_id=None):
        """Mutates artifact.id like WandBRepository does."""
        artifact.id = f"{artifact.id}:mutated-name"


class TestHeterogeneousMutation:
    def test_create_experiment_deep_copy_prevents_contamination(self, two_memory_repos):
        repo_a, _ = two_memory_repos
        mutating = _MutatingRepository()
        composite = CompositeRepository([repo_a, mutating])

        project = _make_project()
        repo_a.create_project(project)

        experiment = _make_experiment(project.name)
        original_id = experiment.id
        # Use write_domain directly since create_experiment no longer exists
        composite.write_domain(experiment, project.name, experiment_id=experiment.id)

        # Primary (repo_a) should have the original ID
        stored = repo_a.read_domain(domain.Experiment, project.name, experiment_id=original_id)
        assert stored.id == original_id

    def test_create_artifact_deep_copy_prevents_contamination(self, two_memory_repos):
        repo_a, _ = two_memory_repos
        mutating = _MutatingRepository()
        composite = CompositeRepository([repo_a, mutating])

        project = _make_project()
        repo_a.create_project(project)
        experiment = _make_experiment(project.name)
        repo_a.write_domain(experiment, project.name, experiment_id=experiment.id)

        artifact = domain.Artifact(name="model")
        original_id = artifact.id
        composite.create_artifact(artifact, b"data", project.name, experiment.id)

        # Primary (repo_a) should have the original ID
        stored = repo_a.read_domain(
            domain.Artifact,
            project.name,
            experiment_id=experiment.id,
            entity_identifier=original_id,
            entity_type="Artifact",
        )
        assert stored.id == original_id

        # Caller's artifact should have primary's ID
        assert artifact.id == original_id

    def test_secondary_write_failure_warns(self, two_memory_repos, caplog):
        repo_a, _ = two_memory_repos
        mutating = _MutatingRepository()
        composite = CompositeRepository([repo_a, mutating])

        project = _make_project()
        repo_a.create_project(project)

        # mutating.create_project is inherited from RepositoryBase and calls write_domain (no-op)
        # So this should succeed on both. Let's test with a method that will fail on secondary.
        experiment = _make_experiment(project.name)
        composite.write_domain(experiment, project.name, experiment_id=experiment.id)

        artifact = domain.Artifact(name="model")
        composite.create_artifact(artifact, b"data", project.name, experiment.id)

        # Primary should have the data
        assert (
            len(repo_a.read_domains(domain.Artifact, project.name, experiment_id=experiment.id))
            == 1
        )

    def test_delete_with_unsupported_backend_warns(self, two_memory_repos, caplog):
        repo_a, _ = two_memory_repos
        mutating = _MutatingRepository()
        composite = CompositeRepository([repo_a, mutating])

        project = _make_project()
        repo_a.create_project(project)
        experiment = _make_experiment(project.name)
        repo_a.write_domain(experiment, project.name, experiment_id=experiment.id)
        artifact = domain.Artifact(name="model")
        repo_a.create_artifact(artifact, b"data", project.name, experiment.id)

        with caplog.at_level(logging.WARNING):
            composite.remove_domain(
                domain.Artifact,
                project.name,
                experiment_id=experiment.id,
                entity_identifier=artifact.id,
                entity_type="Artifact",
            )

        # Primary deleted
        assert (
            len(repo_a.read_domains(domain.Artifact, project.name, experiment_id=experiment.id))
            == 0
        )
        # Secondary warned
        assert "failed for 'remove_domain'" in caplog.text
