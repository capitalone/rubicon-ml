import warnings

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
    project : rubicon_ml.client.Project
        The rubicon project to log to.
    steps : list
        List of (name, transform) tuples (implementing fit/transform) that
        are chained, in the order in which they are chained, with the last
        object an estimator.
    user_defined_loggers : dict, optional
        A dict mapping the estimator name to a corresponding user defined
        logger. See the example below for more details.
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
        transformers is advantageous when fitting is time consuming.
        (docstring source: Scikit-Learn)
    verbose : bool, default=False
        If True, the time elapsed while fitting each step will be printed as
        it is completed. (docstring source: Scikit-Learn)
    ignore_warnings : bool, default=False
        If True, ignores warnings thrown by pipeline.

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
    ...         "vect": FilterEstimatorLogger(
    ...             select=["input", "decode_error", "max_df"],
    ...         ),
    ...         "tfidf": FilterEstimatorLogger(ignore_all=True),
    ...         "clf": FilterEstimatorLogger(
    ...             ignore=["alpha", "penalty"],
    ...         ),
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
        ignore_warnings=False,
    ):
        self.project = project
        self.user_defined_loggers = user_defined_loggers
        self.experiment_kwargs = experiment_kwargs

        self.experiment = None
        self.ignore_warnings = ignore_warnings

        super().__init__(steps, memory=memory, verbose=verbose)

    def fit(self, X, y=None, tags=None, log_fit_params=True, experiment=None, **fit_params):
        """Fit the model and automatically log the `fit_params`
        to rubicon-ml. Optionally, pass `tags` to update the experiment's
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
            Additional keyword arguments to be passed to `sklearn.pipeline.Pipeline.fit()`.
        experiment: rubicon_ml.experiment.client.Experiment, optional
            The experiment to log the to. If no experiment is provided the metrics are
            logged to a new experiment with self.experiment_kwargs.

        Returns
        -------
        rubicon_ml.sklearn.Pipeline
            This `RubiconPipeline`.
        """
        with warnings.catch_warnings():
            if self.ignore_warnings:
                warnings.simplefilter("ignore")
            pipeline = super().fit(X, y, **fit_params)

            if experiment is None:
                experiment = self.project.log_experiment(**self.experiment_kwargs)
            self.experiment = experiment

            if tags is not None:
                self.experiment.add_tags(tags)

            for step_name, estimator in self.steps:
                logger = self.get_estimator_logger(step_name, estimator)
                logger.log_parameters()

            if log_fit_params:
                for name, value in fit_params.items():
                    log_parameter_with_warning(self.experiment, name, value)
        return pipeline

    def score(self, X, y=None, sample_weight=None, experiment=None):
        """Score with the final estimator and automatically
        log the results to rubicon-ml.

        Parameters
        ----------
        X : iterable
            Data to predict on. Must fulfill input requirements of first step of the pipeline.
        y : iterable, optional
            Targets used for scoring. Must fulfill label requirements for all steps of the pipeline.
        sample_weight : list, optional
            If not None, this argument is passed as sample_weight keyword argument to the
            score method of the final estimator.
        experiment: rubicon_ml.experiment.client.Experiment, optional
            The experiment to log the score to. If no experiment is provided the score is logged
            to a new experiment with self.experiment_kwargs.

        Returns
        -------
        float
            Result of calling `score` on the final estimator.
        """
        with warnings.catch_warnings():
            if self.ignore_warnings:
                warnings.simplefilter("ignore")
            score = super().score(X, y, sample_weight)

            if experiment is not None:
                self.experiment = experiment
            elif self.experiment is None:
                self.experiment = self.project.log_experiment(**self.experiment_kwargs)

            logger = self.get_estimator_logger()
            logger.log_metric("score", score)

            # clear `self.experiment` to avoid duplicate metric logging errors
            self.experiment = None

        return score

    def score_samples(self, X, experiment=None):
        """Score samples with the final estimator and automatically
        log the results to rubicon-ml.

        Parameters
        ----------
        X : iterable
            Data to predict on. Must fulfill input requirements of first step of the pipeline.
        experiment: rubicon_ml.experiment.client.Experiment, optional
            The experiment to log the score to. If no experiment is provided the score is logged
            to a new experiment with self.experiment_kwargs.

        Returns
        -------
        ndarray of shape (n_samples,)
            Result of calling `score_samples` on the final estimator.
        """
        with warnings.catch_warnings():
            if self.ignore_warnings:
                warnings.simplefilter("ignore")
            score_samples = super().score_samples(X)

            if experiment is not None:
                self.experiment = experiment
            elif self.experiment is None:
                self.experiment = self.project.log_experiment(**self.experiment_kwargs)

            logger = self.get_estimator_logger()
            try:
                logger.log_metric("score_samples", score_samples)
            except TypeError:
                score_samples = score_samples.tolist()

                logger.log_metric("score_samples", score_samples)

            # clear `self.experiment` to avoid duplicate metric logging errors
            self.experiment = None

            return score_samples

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

    def __getitem__(self, ind):
        """Returns a sub-pipeline with the configured rubicon-ml loggers or
        a single estimator in the pipeline.

        Indexing with an integer will return an estimator; using a slice
        returns another Pipeline instance which copies a slice of this
        Pipeline. This copy is shallow: modifying (or fitting) estimators in
        the sub-pipeline will affect the larger pipeline and vice-versa.
        However, replacing a value in `step` will not affect a copy.
        (docstring source: Scikit-Learn)
        """
        if isinstance(ind, slice):
            if ind.step not in (1, None):
                raise ValueError("Pipeline slicing only supports a step of 1")

            user_defined_loggers_slice = self._get_logger_slice(self.steps[ind])

            return self.__class__(
                self.project,
                self.steps[ind],
                user_defined_loggers_slice,
                self.experiment_kwargs,
                memory=self.memory,
                verbose=self.verbose,
            )
        try:
            _, est = self.steps[ind]
        except TypeError:
            # Not an int, try get step by name
            return self.named_steps[ind]
        return est

    def _get_logger_slice(self, steps):
        """Given a slice of estimators, returns the associated slice of loggers."""
        user_defined_loggers_slice = {}

        for name, _ in steps:
            if name in self.user_defined_loggers:
                user_defined_loggers_slice[name] = self.user_defined_loggers[name]

        return user_defined_loggers_slice


def make_pipeline(
    project,
    *steps,
    experiment_kwargs={"name": "RubiconPipeline experiment"},
    memory=None,
    verbose=False
):
    """Wrapper around RubicionPipeline(). Does not require naming for estimators.
    Their names are set to the lowercase strings of their types.

    Parameters
    ----------
    project : rubicon_ml.client.Project
        The rubicon project to log to.
    steps : list
        List of  estimator objects or (estimator, logger) tuples (implementing
        fit/transform) that are chained, in the order in which they are chained,
        with the last object an estimator. (docstring source: Scikit-Learn)
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
        transformers is advantageous when fitting is time consuming.
        (docstring source: Scikit-Learn)
    verbose : bool, default=False
        If True, the time elapsed while fitting each step will be printed as it
        is completed. (docstring source: Scikit-Learn)

    Returns
    -------
    rubicon_ml.sklearn.Pipeline
        A `RubiconPipeline` with project `project` and steps `steps`.
    """
    steps, loggers = _split_steps_loggers(steps)

    steps = _name_estimators(steps)
    user_defined_loggers = _name_loggers(steps, loggers)

    if not isinstance(project, Project):
        raise ValueError(
            "project" + str(project) + " must be of type rubicon_ml.client.project.Project"
        )

    return RubiconPipeline(project, steps, user_defined_loggers, experiment_kwargs, memory, verbose)


def _split_steps_loggers(steps):
    """Splits the loggers and returns the estimators in the format
    scikit-learn expects them.

    Parameters
    ----------
    steps: list of sklearn.Estimator or tuples of (sklearn.Estimator,
    rubicon_ml.sklearn.EstimatorLogger).
        The steps and estimator loggers to split.

    Returns
    -------
    list of sklearn.Estimator and list of rubicon_ml.sklearn.EstimatorLogger
        The ordered lists of estimators and rubicon-ml loggers.
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
    """Names loggers in the format rubicon-ml expects them.

    Parameters
    ----------
    steps: list of tuples of (str, sklearn.Estimator)
        The named estimator steps.
    loggers: list of rubicon_ml.sklearn.EstimatorLogger
        The rubicon-ml loggers.

    Returns
    -------
    dict of str, rubicon_ml.sklearn.EstimatorLogger
        The named rubicon-ml loggers.
    """
    named_loggers = {}

    for i in range(len(steps)):
        if loggers[i] is not None:
            named_loggers[str(steps[i][0])] = loggers[i]

    return named_loggers
