from unittest.mock import MagicMock, patch

from rubicon_ml import domain
from rubicon_ml.repository import WandBRepository


def _make_repo(**kwargs):
    return WandBRepository(warn=False, **kwargs)


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
