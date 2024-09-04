import warnings


def _check_for_prefect_extras():
    try:
        import prefect  # noqa F401
    except ImportError:
        install_command = "pip install rubicon[prefect]"
        message = f"Install `prefect` with `{install_command}`."

        raise ImportError(message)


warnings.warn(
    "The `rubicon_ml.workflow.prefect` module is deprecated and will be removed in an upcoming release."
    "`rubicon_ml` can still be leveraged within custom tasks.",
    DeprecationWarning,
)

_check_for_prefect_extras()

from rubicon_ml.workflow.prefect.tasks import (  # noqa E402
    create_experiment_task,
    get_or_create_project_task,
    log_artifact_task,
    log_dataframe_task,
    log_feature_task,
    log_metric_task,
    log_parameter_task,
)

__all__ = [
    "create_experiment_task",
    "get_or_create_project_task",
    "log_artifact_task",
    "log_dataframe_task",
    "log_feature_task",
    "log_metric_task",
    "log_parameter_task",
]
