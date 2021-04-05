from sklearn.pipeline import Pipeline

from rubicon.sklearn.base_logger import BaseLogger


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
    >>>         "vect": FilterLogger(select=["input", "decode_error", "max_df"]),
    >>>         "tfidf": FilterLogger(ignore_all=True),
    >>>         "clf": FilterLogger(ignore=["alpha", "penalty"]),
    >>>     }
    >>> )
    """

    def __init__(self, project, steps, user_defined_loggers={}, experiment_kwargs={'name': 'RubiconPipeline experiment'}, **kwargs):
        self.project = project
        self.user_defined_loggers = user_defined_loggers
        self.experiment_kwargs = experiment_kwargs

        self.experiment = project.log_experiment(**self.experiment_kwargs)
        self.logger = BaseLogger()

        super().__init__(steps, **kwargs)

    def fit(self, X, y=None, tags=None, **fit_params):
        pipeline = super().fit(X, y, **fit_params)

        # empty experiments are being logged during
        # the grid search run so using tags to track
        # the relevant data
        if tags is not None:
            self.experiment.add_tags(tags)

        for step_name, estimator in self.steps:
            # dynamically update logger if user defined one for this estimator
            if self.user_defined_loggers.get(step_name):
                self.set_logger(self.user_defined_loggers.get(step_name))

            self.logger.set_experiment(self.experiment)
            self.logger.set_step_name(step_name)
            self.logger.set_estimator(estimator)

            self.logger.log_parameters()

        return pipeline

    def score(self, X, y=None, sample_weight=None):
        score = super().score(X, y, sample_weight)
        self.logger.log_metric("accuracy", score)

        return score

    def set_logger(self, logger):
        self.logger = logger
