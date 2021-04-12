import warnings


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

    def _log_parameter_to_rubicon(self, name, value):
        try:
            self.experiment.log_parameter(name=f"{self.step_name}__{name}", value=value)
        except Exception:
            warning = (
                f"step '{self.step_name}' failed to write parameter '{name}' with value {value} "
                f"of type {type(value)}. try using the `FilterEstimatorLogger` with `ignore=['{name}']`"
            )

            warnings.warn(warning)

    def log_parameters(self):
        for name, value in self.estimator.get_params().items():
            self._log_parameter_to_rubicon(name, value)

    def log_metric(self, name, value):
        self.experiment.log_metric(name, value=value)

    def set_estimator(self, estimator):
        self.estimator = estimator

    def set_experiment(self, experiment):
        self.experiment = experiment

    def set_step_name(self, step_name):
        self.step_name = step_name
