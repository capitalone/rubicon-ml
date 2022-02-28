from sklearn.pipeline import Pipeline, _name_estimators

from rubicon_ml.client.project import Project
from rubicon_ml.sklearn.estimator_logger import EstimatorLogger
from rubicon_ml.sklearn.utils import log_parameter_with_warning


class RubiconPipeline(Pipeline):
    """An extension of `sklearn.pipeline.Pipeline` that automatically
    creates a Rubicon `experiment` under the provided `project` and logs
    the pipeline's `parameters` and `metrics` to it.

    A single pipeline run will result in a single `experiment` logged with
    its corresponding `parameters` and `metrics` pulled from the pipeline's
    estimators.

    Parameters
    ----------
    project : rubicon.client.Project
        The rubicon project to log to.
    steps : list
        List of (name, transform) tuples (implementing fit/transform) that are chained,
        in the order in which they are chained, with the last object an estimator.
    user_defined_loggers : dict, optional
        A dict mapping the estimator name to a corresponding user defined logger.
        See the example below for more details.
    experiment_kwargs : dict, optional
        Additional keyword arguments to be passed to
        `project.log_experiment()`.
    memory : str or object with the joblib.Memory interface, default=None
        Used to cache the fitted transformers of the pipeline. By default,
        no caching is performed. If a string is given, it is the path to
        the caching directory. Enabling caching triggers a clone of
        the transformers before fitting. Therefore, the transformer
        instance given to the pipeline cannot be inspected
        directly. Use the attribute ``named_steps`` or ``steps`` to
        inspect estimators within the pipeline. Caching the
        transformers is advantageous when fitting is time consuming. (docstring source: Scikit-Learn)
    verbose : bool, default=False
        If True, the time elapsed while fitting each step will be printed as it
        is completed. (docstring source: Scikit-Learn)

    Examples
    --------
    >>> pipeline = RubiconPipeline(
    ...     project,
    ...     [
    ...         ("vect", CountVectorizer()),
    ...         ("tfidf", TfidfTransformer()),
    ...         ("clf", SGDClassifier()),
    ...     ],
    ...     user_defined_loggers = {
    ...         "vect": FilterEstimatorLogger(select=["input", "decode_error", "max_df"]),
    ...         "tfidf": FilterEstimatorLogger(ignore_all=True),
    ...         "clf": FilterEstimatorLogger(ignore=["alpha", "penalty"]),
    ...     }
    ... )
    """

    def __init__(
        self,
        project,
        steps,
        user_defined_loggers={},
        experiment_kwargs={"name": "RubiconPipeline experiment"},
        memory=None,
        verbose=False,
    ):
        self.project = project
        self.user_defined_loggers = user_defined_loggers
        self.experiment_kwargs = experiment_kwargs

        self.experiment = None

        super().__init__(steps, memory=memory, verbose=verbose)

    def fit(self, X, y=None, tags=None, log_fit_params=True, **fit_params):
        """Fit the model and automatically log the `fit_params`
        to Rubicon. Optionally, pass `tags` to update the experiment's
        tags.

        Parameters
        ----------
        X : iterable
            Training data. Must fulfill input requirements of first step of the pipeline.
        y : iterable, optional
            Training targets. Must fulfill label requirements for all steps of the pipeline.
        tags : list, optional
            Additional tags to add to the experiment during the fit.
        log_fit_params : bool, optional
            True to log the values passed as `fit_params` to this pipeline's experiment.
            Defaults to True.
        fit_params : dict, optional
            Additional keyword arguments to be passed to
            `sklearn.pipeline.Pipeline.fit()`.
        """
        pipeline = super().fit(X, y, **fit_params)

        self.experiment = self.project.log_experiment(**self.experiment_kwargs)
        if tags is not None:
            self.experiment.add_tags(tags)

        for step_name, estimator in self.steps:
            logger = self.get_estimator_logger(step_name, estimator)
            logger.log_parameters()

        if log_fit_params:
            for name, value in fit_params.items():
                log_parameter_with_warning(self.experiment, name, value)

        return pipeline

    def score(self, X, y=None, sample_weight=None):
        """Score with the final estimator and automatically
        log the results to Rubicon.

        Parameters
        ----------
        X : iterable
            Data to predict on. Must fulfill input requirements of first step of the pipeline.
        y : iterable, optional
            Targets used for scoring. Must fulfill label requirements for all steps of the pipeline.
        sample_weight : list, optional
            If not None, this argument is passed as sample_weight keyword argument to the
            score method of the final estimator.
        """
        score = super().score(X, y, sample_weight)

        logger = self.get_estimator_logger()
        logger.log_metric("score", score)

        return score

    def get_estimator_logger(self, step_name=None, estimator=None):
        """Get a logger for the estimator. By default, the logger will
        have the current experiment set.
        """
        logger = self.user_defined_loggers.get(step_name) or EstimatorLogger()

        logger.set_experiment(self.experiment)

        if step_name:
            logger.set_step_name(step_name)

        if estimator:
            logger.set_estimator(estimator)

        return logger


def make_pipeline(
    project,
    *steps,
    experiment_kwargs={"name": "RubiconPipeline experiment"},
    memory=None,
    verbose=False
):
    """Wrapper around RubicionPipeline(). Does not require naming for estimators. Their names are set to the lowercase strings of their types.

    Parameters
    ----------
    project : rubicon.client.Project
        The rubicon project to log to.
    steps : list
        List of  estimator objects or (estimator, logger) tuples (implementing fit/transform) that are chained,
        in the order in which they are chained, with the last object an estimator. (doc string source: Scikit-Learn)
    experiment_kwargs : dict, optional
        Additional keyword arguments to be passed to
        `project.log_experiment()`.
    memory : str or object with the joblib.Memory interface, default=None
        Used to cache the fitted transformers of the pipeline. By default,
        no caching is performed. If a string is given, it is the path to
        the caching directory. Enabling caching triggers a clone of
        the transformers before fitting. Therefore, the transformer
        instance given to the pipeline cannot be inspected
        directly. Use the attribute ``named_steps`` or ``steps`` to
        inspect estimators within the pipeline. Caching the
        transformers is advantageous when fitting is time consuming. (docstring source: Scikit-Learn)
    verbose : bool, default=False
        If True, the time elapsed while fitting each step will be printed as it
        is completed. (docstring source: Scikit-Learn)

    """
    steps, loggers = _split_steps_loggers(steps)

    steps = _name_estimators(steps)
    user_defined_loggers = _name_loggers(steps, loggers)

    if type(project) != Project:
        raise ValueError(
            "project" + str(project) + " must be of type rubicon_ml.client.project.Project"
        )

    return RubiconPipeline(project, steps, user_defined_loggers, experiment_kwargs, memory, verbose)


def _split_steps_loggers(steps):
    """
    Parameters
    ----------
        steps: List of  estimator or tuples of (estimator,logger).
    Returns
    -------
        Tuple of named estimators list and ordered loggers list
    """
    ret_loggers = []
    ret_steps = []
    for step in steps:
        if isinstance(step, tuple):
            ret_loggers.append(step[1])
            ret_steps.append(step[0])
        else:
            ret_loggers.append(None)
            ret_steps.append(step)
    return ret_steps, ret_loggers


def _name_loggers(steps, loggers):
    """
    Parameters
    ----------
        steps: List of  (name, estimator) tuples.
        loggers: List of logger objects
    Returns
    -------
        List of named logger (name, logger) tuples
    """
    named_loggers = {}
    for i in range(len(steps)):
        if loggers[i] is not None:
            named_loggers[str(steps[i][0])] = loggers[i]
    return named_loggers
