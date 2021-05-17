from prefect import task

from rubicon_ml import Rubicon


@task
def get_or_create_project_task(
    persistence, root_dir, project_name, auto_git_enabled=False, **kwargs
):
    """Get or create a project within a `prefect` flow.

    This `prefect` task can be used within a flow to
    create a new project or get an existing one. It should
    be the entry point to any `prefect` flow that logs
    data to Rubicon.

    Parameters
    ----------
    persistence : str
        The persistence type to be passed to the
        `Rubicon` constructor.
    root_dir : str
        The root directory to be passed to the
        `Rubicon` constructor.
    project_name : str
        The name of the project to get or create.
    auto_git_enabled : bool, optional
        True to use the `git` command to automatically log
        relevant repository information to projects and
        experiments logged with the client instance created
        in this task, False otherwise. Defaults to False.
    kwargs : dict
        Additional keyword arguments to be passed to
        `Rubicon.create_project`.

    Returns
    -------
    rubicon.client.Project
        The project with name `project_name`.
    """
    rubicon = Rubicon(persistence=persistence, root_dir=root_dir, auto_git_enabled=auto_git_enabled)
    project = rubicon.get_or_create_project(project_name, **kwargs)

    return project


@task
def create_experiment_task(project, **kwargs):
    """Create an experiment within a `prefect` flow.

    This `prefect` task can be used within a flow to
    create a new experiment under an existing project.

    Parameters
    ----------
    project : rubicon.client.Project
        The project under which the experiment will be created.
    kwargs : dict
        Keyword arguments to be passed to
        `Project.log_experiment`.

    Returns
    -------
    rubicon.client.Experiment
        The created experiment.
    """
    return project.log_experiment(**kwargs)


@task
def log_artifact_task(parent, **kwargs):
    """Log an artifact within a `prefect` flow.

    This `prefect` task can be used within a flow to
    log an artifact to an existing project or experiment.

    Parameters
    ----------
    parent : rubicon.client.Project or rubicon.client.Experiment
        The project or experiment to log the artifact to.
    kwargs : dict
        Keyword arguments to be passed to
        `Project.log_artifact` or `Experiment.log_artifact`.

    Returns
    -------
    rubicon.client.Artifact
        The logged artifact.
    """
    return parent.log_artifact(**kwargs)


@task
def log_dataframe_task(parent, df, **kwargs):
    """Log a dataframe within a `prefect` flow.

    This `prefect` task can be used within a flow to
    log a dataframe to an existing project or experiment.

    Parameters
    ----------
    parent : rubicon.client.Project or rubicon.client.Experiment
        The project or experiment to log the dataframe to.
    df : pandas.DataFrame or dask.dataframe.DataFrame
        The `pandas` or `dask` dataframe to log.
    kwargs : dict
        Additional keyword arguments to be passed to
        `Project.log_dataframe` or `Experiment.log_dataframe`.

    Returns
    -------
    rubicon.client.Dataframe
        The logged dataframe.
    """
    return parent.log_dataframe(df, **kwargs)


@task
def log_feature_task(experiment, feature_name, **kwargs):
    """Log a feature within a `prefect` flow.

    This `prefect` task can be used within a flow to
    log a feature to an existing experiment.

    Parameters
    ----------
    experiment : rubicon.client.Experiment
        The experiment to log a new feature to.
    feature_name : str
        The name of the feature to log. Passed to
        `Experiment.log_feature` as `name`.
    kwargs : dict
        Additional keyword arguments to be passed to
        `Experiment.log_feature`.

    Returns
    -------
    rubicon.client.Feature
        The logged feature.
    """
    return experiment.log_feature(feature_name, **kwargs)


@task
def log_metric_task(experiment, metric_name, metric_value, **kwargs):
    """Log a metric within a `prefect` flow.

    This `prefect` task can be used within a flow to
    log a metric to an existing experiment.

    Parameters
    ----------
    experiment : rubicon.client.Experiment
        The experiment to log a new metric to.
    metric_name : str
        The name of the metric to log. Passed to
        `Experiment.log_metric` as `name`.
    metric_value : str
        The value of the metric to log. Passed to
        `Experiment.log_metric` as `value`.
    kwargs : dict
        Additional keyword arguments to be passed to
        `Experiment.log_metric`.

    Returns
    -------
    rubicon.client.Metric
        The logged metric.
    """
    return experiment.log_metric(metric_name, metric_value, **kwargs)


@task
def log_parameter_task(experiment, parameter_name, parameter_value, **kwargs):
    """Log a parameter within a `prefect` flow.

    This `prefect` task can be used within a flow to
    log a parameter to an existing experiment.

    Parameters
    ----------
    experiment : rubicon.client.Experiment
        The experiment to log a new parameter to.
    parameter_name : str
        The name of the parameter to log. Passed to
        `Experiment.log_parameter` as `name`.
    parameter_value : str
        The value of the parameter to log. Passed to
        `Experiment.log_parameter` as `value`.
    kwargs : dict
        Additional keyword arguments to be passed to
        `Experiment.log_parameter`.

    Returns
    -------
    rubicon.client.Parameter
        The logged parameter.
    """
    return experiment.log_parameter(parameter_name, parameter_value, **kwargs)
