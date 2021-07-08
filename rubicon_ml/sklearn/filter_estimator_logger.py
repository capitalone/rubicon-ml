from rubicon_ml.exceptions import RubiconException
from rubicon_ml.sklearn.estimator_logger import EstimatorLogger
from rubicon_ml.sklearn.utils import log_parameter_with_warning


class FilterEstimatorLogger(EstimatorLogger):
    """The filter logger for sklearn estimators. Use this
    logger to either select or ignore specific parameters
    for logging.

    Parameters
    ----------
    estimator : a sklearn estimator, optional
        The estimator
    experiment : rubicon.client.Experiment, optional
        The experiment to log the parameters and metrics to.
    step_name : str, optional
        The name of the pipeline step.
    select : list, optional
        The list of parameters on this estimator that you'd like
        to log. All other parameters will be ignored.
    ignore : list, optional
        The list of parameters on this estimator that you'd like
        to ignore by not logging. The other parameters will be logged.
    ignore_all : bool, optional
        Ignore all parameters if true.
    """

    def __init__(
        self,
        estimator=None,
        experiment=None,
        step_name=None,
        select=[],
        ignore=[],
        ignore_all=False,
    ):
        if ignore and select:
            raise RubiconException("provide either `select` OR `ignore`, not both")

        self.ignore = ignore
        self.ignore_all = ignore_all
        self.select = select

        super().__init__(estimator=estimator, experiment=experiment, step_name=step_name)

    def log_parameters(self):
        if self.ignore_all:
            return

        for name, value in self.estimator.get_params().items():
            if (self.ignore and name not in self.ignore) or (self.select and name in self.select):
                log_parameter_with_warning(self.experiment, f"{self.step_name}__{name}", value)
