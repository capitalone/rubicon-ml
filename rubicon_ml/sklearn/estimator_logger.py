from rubicon_ml.sklearn.utils import log_parameter_with_warning


class EstimatorLogger:
    """The base logger for sklearn estimators. By default,
    it will log all of the estimator's parameters.

    Parameters
    ----------
    estimator : a sklearn estimator, optional
        The estimator.
    experiment : rubicon.client.Experiment, optional
        The experiment to log the parameters and metrics to.
    step_name : str, optional
        The name of the pipeline step.
    """

    def __init__(self, estimator=None, experiment=None, step_name=None):
        self.estimator = estimator
        self.experiment = experiment
        self.step_name = step_name

    def log_parameters(self):
        for name, value in self.estimator.get_params().items():
            log_parameter_with_warning(self.experiment, f"{self.step_name}__{name}", value)

    def log_metric(self, name, value):
        self.experiment.log_metric(name, value=value)

    def set_estimator(self, estimator):
        self.estimator = estimator

    def set_experiment(self, experiment):
        self.experiment = experiment

    def set_step_name(self, step_name):
        self.step_name = step_name
