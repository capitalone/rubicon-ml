from unittest.mock import MagicMock, patch

import pytest

from rubicon_ml import domain
from rubicon_ml.domain.comment_update import CommentUpdate
from rubicon_ml.domain.tag_update import TagUpdate
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import WandBRepository
from rubicon_ml.repository.base import RepositoryBase


def _make_repo(**kwargs):
    return WandBRepository(warn=False, **kwargs)


def _mock_wandb():
    """Create a MagicMock that stands in for the wandb module."""
    mock = MagicMock()
    mock_run = MagicMock()
    mock_run.id = "run-123"
    mock_run.name = "test-run"
    mock_run.project = "test-project"
    mock_run.tags = []
    mock_run.config = {}
    mock.init.return_value = mock_run
    return mock, mock_run


def _patch_wandb(repo, mock_wandb):
    """Return a context manager that patches the wandb property on a repo instance."""
    return patch.object(type(repo), "wandb", new_callable=lambda: property(lambda self: mock_wandb))


# -------- Structural Tests --------


class TestWandBStructure:
    def test_inherits_repository_base(self):
        assert issubclass(WandBRepository, RepositoryBase)

    def test_does_not_inherit_fsspec(self):
        from rubicon_ml.repository.fsspec import FsspecRepository

        assert not issubclass(WandBRepository, FsspecRepository)

    def test_no_fsspec_import(self):
        import inspect

        source = inspect.getsource(WandBRepository)
        assert "import fsspec" not in source
        assert "self.filesystem" not in source

    def test_protocol_is_wandb(self):
        repo = _make_repo()
        assert repo.PROTOCOL == "wandb"

    def test_no_filesystem_attribute(self):
        repo = _make_repo()
        assert not hasattr(repo, "filesystem")


# -------- Init Tests --------


class TestWandBInit:
    def test_default_init(self):
        repo = _make_repo()
        assert repo.entity is None
        assert repo.root_dir == "WANDB"
        assert repo.wandb_init_kwargs == {}
        assert repo._active_run is None
        assert repo._current_project is None

    def test_init_with_entity(self):
        repo = _make_repo(entity="my-team")
        assert repo.entity == "my-team"

    def test_init_with_wandb_kwargs(self):
        repo = _make_repo(wandb_init_kwargs={"mode": "offline"})
        assert repo.wandb_init_kwargs == {"mode": "offline"}


# -------- Helper Tests --------


class TestWandBHelpers:
    def test_get_active_run_passes_entity(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo(entity="my-team")

        with _patch_wandb(repo, mock_wandb):
            repo._get_active_run("test-project", "run-123")

        mock_wandb.init.assert_called_once()
        call_kwargs = mock_wandb.init.call_args[1]
        assert call_kwargs["entity"] == "my-team"
        assert call_kwargs["project"] == "test-project"
        assert call_kwargs["id"] == "run-123"
        assert call_kwargs["resume"] == "must"

    def test_get_active_run_omits_entity_when_none(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo()

        with _patch_wandb(repo, mock_wandb):
            repo._get_active_run("test-project", "run-123")

        call_kwargs = mock_wandb.init.call_args[1]
        assert "entity" not in call_kwargs

    def test_get_active_run_caches_run(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo()

        with _patch_wandb(repo, mock_wandb):
            run1 = repo._get_active_run("test-project", "run-123")
            run2 = repo._get_active_run("test-project", "run-123")

        assert mock_wandb.init.call_count == 1
        assert run1 is run2

    def test_get_wandb_path_with_entity(self):
        repo = _make_repo(entity="my-team")
        assert repo._get_wandb_path("proj") == "my-team/proj"
        assert repo._get_wandb_path("proj", "run-1") == "my-team/proj/run-1"

    def test_get_wandb_path_without_entity(self):
        repo = _make_repo()
        assert repo._get_wandb_path("proj") == "proj"
        assert repo._get_wandb_path("proj", "run-1") == "proj/run-1"

    def test_finish(self):
        repo = _make_repo()
        mock_run = MagicMock()
        repo._active_run = mock_run

        repo.finish()

        mock_run.finish.assert_called_once()
        assert repo._active_run is None

    def test_finish_when_no_active_run(self):
        repo = _make_repo()
        repo.finish()  # should not raise


# -------- write_domain Tests --------


class TestWriteDomain:
    def test_write_project(self):
        repo = _make_repo()
        project = domain.Project(name="test-project")

        repo.write_domain(project, "test-project")

        assert repo._current_project is project

    def test_write_experiment(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo(entity="my-team")
        repo._current_project = domain.Project(name="test-project")

        experiment = domain.Experiment(name="test-exp", project_name="test-project")

        with _patch_wandb(repo, mock_wandb):
            repo.write_domain(experiment, "test-project", experiment_id=experiment.id)

        mock_wandb.init.assert_called_once()
        call_kwargs = mock_wandb.init.call_args[1]
        assert call_kwargs["entity"] == "my-team"
        assert call_kwargs["project"] == "test-project"
        assert call_kwargs["name"] == "test-exp"
        # experiment.id should be mutated to the run's id
        assert experiment.id == "run-123"

    def test_write_metric(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo()
        repo._active_run = mock_run

        metric = domain.Metric(name="accuracy", value=0.95)

        with _patch_wandb(repo, mock_wandb):
            repo.write_domain(metric, "proj", experiment_id="run-123")

        mock_wandb.log.assert_called_once_with({"accuracy": 0.95})
        assert "_rubicon_metric_accuracy" in mock_run.config

    def test_write_parameter(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo()
        repo._active_run = mock_run

        param = domain.Parameter(name="learning_rate", value=0.01)

        with _patch_wandb(repo, mock_wandb):
            repo.write_domain(param, "proj", experiment_id="run-123")

        assert mock_run.config["learning_rate"] == 0.01
        assert "_rubicon_parameter_learningrate" in mock_run.config

    def test_write_feature(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo()
        repo._active_run = mock_run

        feature = domain.Feature(name="age", importance=0.8)

        with _patch_wandb(repo, mock_wandb):
            repo.write_domain(feature, "proj", experiment_id="run-123")

        assert "_rubicon_feature_age" in mock_run.config
        mock_wandb.log.assert_called_once_with({"age_importance": 0.8})

    def test_write_feature_no_importance(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo()
        repo._active_run = mock_run

        feature = domain.Feature(name="age")

        with _patch_wandb(repo, mock_wandb):
            repo.write_domain(feature, "proj", experiment_id="run-123")

        assert "_rubicon_feature_age" in mock_run.config
        mock_wandb.log.assert_not_called()

    def test_write_artifact_metadata(self):
        repo = _make_repo()
        repo._active_run = MagicMock()
        repo._active_run.config = {}

        artifact = domain.Artifact(name="my-model")
        repo.write_domain(
            artifact,
            "proj",
            experiment_id="run-123",
            entity_identifier="orig-id",
            entity_type="Artifact",
        )

        assert "_rubicon_artifact_orig-id" in repo._active_run.config

    def test_write_dataframe_metadata(self):
        repo = _make_repo()
        repo._active_run = MagicMock()
        repo._active_run.config = {}

        df = domain.Dataframe(description="test df")
        repo.write_domain(df, "proj", experiment_id="run-123")

        assert f"_rubicon_dataframe_{df.id}" in repo._active_run.config


# -------- read_domain Tests --------


class TestReadDomain:
    def test_read_project_cached(self):
        repo = _make_repo()
        project = domain.Project(name="test-project")
        repo._current_project = project

        result = repo.read_domain(domain.Project, "test-project")
        assert result is project

    def test_read_project_not_found(self):
        repo = _make_repo()
        mock_api = MagicMock()
        mock_api.runs.side_effect = Exception("not found")

        with patch.object(type(repo), "api", new_callable=lambda: property(lambda self: mock_api)):
            with pytest.raises(RubiconException, match="No project"):
                repo.read_domain(domain.Project, "nonexistent")


# -------- read_domains Tests --------


class TestReadDomains:
    def test_read_projects_raises(self):
        repo = _make_repo()
        with pytest.raises(RubiconException, match="doesn't support listing"):
            repo.read_domains(domain.Project)

    def test_read_experiments(self):
        repo = _make_repo()
        mock_api = MagicMock()
        mock_run = MagicMock()
        mock_run.config = {
            "_rubicon_experiment_metadata": '{"name": "exp1", "project_name": "proj"}'
        }
        mock_api.runs.return_value = [mock_run]

        with patch.object(type(repo), "api", new_callable=lambda: property(lambda self: mock_api)):
            experiments = repo.read_domains(domain.Experiment, "proj")

        assert len(experiments) == 1
        assert experiments[0].name == "exp1"


# -------- remove_domain Tests --------


class TestRemoveDomain:
    def test_remove_raises(self):
        repo = _make_repo()
        with pytest.raises(RubiconException, match="doesn't support deletion"):
            repo.remove_domain(domain.Experiment, "proj", experiment_id="run-123")


# -------- Convenience Override Tests --------


class TestCreateArtifact:
    def test_create_artifact_mutates_id(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo()
        repo._active_run = mock_run

        artifact = domain.Artifact(name="my-model")
        original_id = artifact.id

        with _patch_wandb(repo, mock_wandb):
            repo.create_artifact(artifact, b"data", "proj", experiment_id="run-123")

        assert artifact.id == f"{original_id}:my-model"

    def test_create_artifact_requires_experiment_id(self):
        repo = _make_repo()
        artifact = domain.Artifact(name="my-model")

        with pytest.raises(RubiconException, match="project-level artifacts"):
            repo.create_artifact(artifact, b"data", "proj")


class TestTags:
    def test_add_tags_syncs_native_experiment_tags(self):
        mock_wandb, mock_run = _mock_wandb()
        repo = _make_repo()
        repo._active_run = mock_run

        with _patch_wandb(repo, mock_wandb):
            repo.write_domain(
                TagUpdate(added_tags=["tag_a"]),
                "proj",
                experiment_id="run-123",
                entity_identifier="run-123",
                entity_type="Experiment",
            )

        assert "tag_a" in mock_run.tags

    def test_remove_tags_syncs_native_experiment_tags(self):
        mock_wandb, mock_run = _mock_wandb()
        mock_run.tags = ["tag_a", "tag_b"]
        repo = _make_repo()
        repo._active_run = mock_run

        with _patch_wandb(repo, mock_wandb):
            repo.write_domain(
                TagUpdate(removed_tags=["tag_a"]),
                "proj",
                experiment_id="run-123",
                entity_identifier="run-123",
                entity_type="Experiment",
            )

        assert "tag_a" not in mock_run.tags
        assert "tag_b" in mock_run.tags

    def test_add_tags_requires_experiment_id(self):
        repo = _make_repo()
        with pytest.raises(RubiconException, match="experiment_id is required"):
            repo.write_domain(
                TagUpdate(added_tags=["tag"]),
                "proj",
            )

    def test_get_tags_requires_experiment_id(self):
        repo = _make_repo()
        with pytest.raises(RubiconException, match="experiment_id is required"):
            repo.read_domains(TagUpdate, "proj")


class TestComments:
    def test_add_comments_requires_experiment_id(self):
        repo = _make_repo()
        with pytest.raises(RubiconException, match="experiment_id is required"):
            repo.write_domain(
                CommentUpdate(added_comments=["comment"]),
                "proj",
            )

    def test_get_comments_requires_experiment_id(self):
        repo = _make_repo()
        with pytest.raises(RubiconException, match="experiment_id is required"):
            repo.read_domains(CommentUpdate, "proj")
