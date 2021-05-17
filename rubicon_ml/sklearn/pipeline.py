from sklearn.pipeline import Pipeline

from rubicon_ml.sklearn.estimator_logger import EstimatorLogger


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

        Parameters
        ----------
        X : iterable
            Training data. Must fulfill input requirements of first step of the pipeline.
        y : iterable, optional
            Training targets. Must fulfill label requirements for all steps of the pipeline.
        tags : list, optional
            Additional tags to add to the experiment during the fit.
        fit_params : dict
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
