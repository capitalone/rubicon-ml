import warnings


class BaseEstimatorLogger:
    def __init__(self):
        self.estimator = None
        self.experiment = None
        self.step_name = None

    def _log_parameter_to_rubicon(self, name, value):
        try:
            self.experiment.log_parameter(name=f"{self.step_name}__{name}", value=value)
        except Exception:
            warning = (
                f"step '{self.step_name}' failed to write parameter '{name}' with value {value} "
                f"of type {type(value)}. try using the `FilteredLogger` with `ignore='{name}'`"
            )

            warnings.warn(warning)

    def log_parameters(self):
        for name, value in self.estimator.get_params().items():
            self._log_parameter_to_rubicon(name, value)

    def log_metrics(self):
        pass

    def set_estimator(self, estimator):
        self.estimator = estimator

    def set_experiment(self, experiment):
        self.experiment = experiment

    def set_step_name(self, step_name):
        self.step_name = step_name
