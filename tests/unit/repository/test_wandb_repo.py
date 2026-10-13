from unittest.mock import MagicMock, patch

import pytest

from rubicon_ml import Rubicon, domain
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import WandBRepository


def _make_repo(**kwargs):
    return WandBRepository(warn=False, **kwargs)


class _FakeRuns:
    """Mimics W&B's lazy ``Runs``: the server is only queried -- and a missing
    project only raises -- when the result is sized or iterated, not when
    ``api.runs(...)`` is called.
    """

    def __init__(self, exists):
        self._exists = exists

    def __len__(self):
        if not self._exists:
            raise ValueError("Could not find project")
        return 1

    def __iter__(self):
        if not self._exists:
            raise ValueError("Could not find project")
        return iter([])


def _mock_api(exists):
    api = MagicMock()
    api.runs.return_value = _FakeRuns(exists)
    return api


def test_get_project_returns_cached_project_for_same_name():
    repo = _make_repo(entity="my-team")
    api = _mock_api(exists=True)

    with patch.object(type(repo), "api", new_callable=lambda: property(lambda self: api)):
        first = repo.get_project("project-a")
        second = repo.get_project("project-a")

    assert first is second
    assert second.name == "project-a"


def test_get_project_does_not_return_cache_for_different_name():
    """Regression: `get_project` must not return a cached project of a different name."""
    repo = _make_repo(entity="my-team")
    api = _mock_api(exists=True)

    with patch.object(type(repo), "api", new_callable=lambda: property(lambda self: api)):
        project_a = repo.get_project("project-a")
        project_b = repo.get_project("project-b")

    assert project_a.name == "project-a"
    assert project_b.name == "project-b"


def test_get_project_raises_when_project_does_not_exist():
    """Regression: `get_project` must force evaluation of W&B's lazy `runs` query so a
    nonexistent project raises instead of fabricating a project for any name.
    """
    repo = _make_repo(entity="my-team")
    api = _mock_api(exists=False)

    with patch.object(type(repo), "api", new_callable=lambda: property(lambda self: api)):
        with pytest.raises(RubiconException, match="No project with name 'missing' found."):
            repo.get_project("missing")


def test_get_or_create_project_writes_to_all_backends_in_composite(tmp_path):
    """Regression: in a composite config, a nonexistent project reported as existing by
    the W&B backend must not short-circuit `get_or_create_project` and skip writing the
    project to the other backends.
    """
    mock_wandb = MagicMock()
    mock_run = MagicMock()
    mock_run.id = "run-abc"
    mock_run.name = "experiment-a"
    mock_wandb.init.return_value = mock_run
    mock_wandb.Api.return_value = _mock_api(exists=False)

    root_dir = str(tmp_path / "rubicon-data")

    with patch.object(
        WandBRepository, "wandb", new_callable=lambda: property(lambda self: mock_wandb)
    ):
        composite = Rubicon(
            composite_config=[
                {"persistence": "filesystem", "root_dir": root_dir},
                {"persistence": "wandb", "entity": "my-team", "root_dir": "WANDB"},
            ],
        )

        project = composite.get_or_create_project("composite-project")
        project.log_experiment("experiment-a")

    # the filesystem backend must have received the project despite W&B's lazy existence check
    filesystem_client = Rubicon(persistence="filesystem", root_dir=root_dir)
    assert filesystem_client.get_project("composite-project").name == "composite-project"


@patch("rubicon_ml.repository.wandb.WandBRepository.wandb", new_callable=lambda: property(lambda self: MagicMock()))
def test_get_active_run_passes_entity(mock_wandb_prop):
    mock_wandb = MagicMock()
    mock_run = MagicMock()
    mock_wandb.init.return_value = mock_run

    repo = _make_repo(entity="my-team")

    with patch.object(type(repo), "wandb", new_callable=lambda: property(lambda self: mock_wandb)):
        repo._get_active_run("test-project", "run-123")

    mock_wandb.init.assert_called_once()
    call_kwargs = mock_wandb.init.call_args[1]
    assert call_kwargs["entity"] == "my-team"
    assert call_kwargs["project"] == "test-project"
    assert call_kwargs["id"] == "run-123"
    assert call_kwargs["resume"] == "must"


@patch("rubicon_ml.repository.wandb.WandBRepository.wandb", new_callable=lambda: property(lambda self: MagicMock()))
def test_get_active_run_omits_entity_when_none(mock_wandb_prop):
    mock_wandb = MagicMock()
    mock_run = MagicMock()
    mock_wandb.init.return_value = mock_run

    repo = _make_repo()

    with patch.object(type(repo), "wandb", new_callable=lambda: property(lambda self: mock_wandb)):
        repo._get_active_run("test-project", "run-123")

    mock_wandb.init.assert_called_once()
    call_kwargs = mock_wandb.init.call_args[1]
    assert "entity" not in call_kwargs


@patch("rubicon_ml.repository.wandb.WandBRepository.wandb", new_callable=lambda: property(lambda self: MagicMock()))
def test_persist_experiment_passes_entity(mock_wandb_prop):
    mock_wandb = MagicMock()
    mock_run = MagicMock()
    mock_run.id = "run-456"
    mock_run.name = "test-run"
    mock_wandb.init.return_value = mock_run

    repo = _make_repo(entity="my-team")
    repo._current_project = domain.Project(name="test-project")

    experiment = domain.Experiment(name="test-experiment", project_name="test-project")

    with patch.object(type(repo), "wandb", new_callable=lambda: property(lambda self: mock_wandb)):
        repo._persist_domain(experiment, "unused-path")

    mock_wandb.init.assert_called_once()
    call_kwargs = mock_wandb.init.call_args[1]
    assert call_kwargs["entity"] == "my-team"
    assert call_kwargs["project"] == "test-project"


@patch("rubicon_ml.repository.wandb.WandBRepository.wandb", new_callable=lambda: property(lambda self: MagicMock()))
def test_persist_experiment_omits_entity_when_none(mock_wandb_prop):
    mock_wandb = MagicMock()
    mock_run = MagicMock()
    mock_run.id = "run-456"
    mock_run.name = "test-run"
    mock_wandb.init.return_value = mock_run

    repo = _make_repo()
    repo._current_project = domain.Project(name="test-project")

    experiment = domain.Experiment(name="test-experiment", project_name="test-project")

    with patch.object(type(repo), "wandb", new_callable=lambda: property(lambda self: mock_wandb)):
        repo._persist_domain(experiment, "unused-path")

    mock_wandb.init.assert_called_once()
    call_kwargs = mock_wandb.init.call_args[1]
    assert "entity" not in call_kwargs
