from sklearn.pipeline import Pipeline

from rubicon.exceptions import RubiconException
from rubicon.sklearn.estimator_logger import EstimatorLogger


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
    kwargs : dict
        Additional keyword arguments to be passed to
        `sklearn.pipeline.Pipeline()`.

    Example
    -------
    >>> pipeline = RubiconPipeline(
    >>>     project,
    >>>     [
    >>>         ("vect", CountVectorizer()),
    >>>         ("tfidf", TfidfTransformer()),
    >>>         ("clf", SGDClassifier()),
    >>>     ],
    >>>     user_defined_loggers = {
    >>>         "vect": FilterEstimatorLogger(select=["input", "decode_error", "max_df"]),
    >>>         "tfidf": FilterEstimatorLogger(ignore_all=True),
    >>>         "clf": FilterEstimatorLogger(ignore=["alpha", "penalty"]),
    >>>     }
    >>> )
    """

    def __init__(
        self,
        project,
        steps,
        user_defined_loggers={},
        experiment_kwargs={"name": "RubiconPipeline experiment"},
        **kwargs
    ):
        self.project = project
        self.user_defined_loggers = user_defined_loggers
        self.experiment_kwargs = experiment_kwargs

        self.experiment = None

        super().__init__(steps, **kwargs)

    def fit(self, X, y=None, tags=None, **fit_params):
        """Fit the model and automatically log the `fit_params`
        to Rubicon. Optionally, pass `tags` to update the experiment's
        tags.
        """
        pipeline = super().fit(X, y, **fit_params)

        self.experiment = self.project.log_experiment(**self.experiment_kwargs)
        if tags is not None:
            self.experiment.add_tags(tags)

        for step_name, estimator in self.steps:
            logger = self.get_estimator_logger(step_name, estimator)
            logger.log_parameters()

        return pipeline

    def score(self, X, y=None, sample_weight=None, experiment=None):
        """Score with the final estimator and automatically
        log the results to Rubicon.

        Parameters
        ----------
        experiment : rubicon.client.Experiment, optional
            The rubicon experiment to log the score metric to. If
            None, `self.experiment` will be used if available. Only
            necessary if this instance of `RubiconPipeline` has not
            been fit.
        """
        score = super().score(X, y, sample_weight)

        if experiment is not None:
            self.experiment = experiment

        logger = self.get_estimator_logger()
        logger.log_metric("score", score)

        return score

    def get_estimator_logger(self, step_name=None, estimator=None):
        """Get a logger for the estimator. By default, the logger will
        have the current experiment set.
        """
        if self.experiment is None:
            error_message = (
                "This instance of `RubiconPipeline` has no associated experiment. "
                "Fit this pipeline to generate one, or provide the `experiment` "
                "kwarg to the offending function call."
            )
            raise RubiconException(error_message)

        logger = self.user_defined_loggers.get(step_name) or EstimatorLogger()

        logger.set_experiment(self.experiment)

        if step_name:
            logger.set_step_name(step_name)

        if estimator:
            logger.set_estimator(estimator)

        return logger
