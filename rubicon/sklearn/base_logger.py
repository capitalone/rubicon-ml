import warnings


class BaseEstimatorLogger:
    def __init__(self, experiment, step_name, estimator):
        self.estimator = estimator
        self.experiment = experiment
        self.step_name = step_name

    def _log_parameter_to_rubicon(self, name, value):
        try:
            self.experiment.log_parameter(name=name, value=value)
        except Exception:
            warning = (
                f"step '{self.step_name}' failed to write parameter '{name}' with value {value} "
                f"of type {type(value)}. try using the `FilteredLogger` with `ignore='{name}'`"
            )

            warnings.warn(warning)

    def log_parameters(self):
        for name, value in self.estimator.get_params().items():
            self._log_parameter_to_rubicon(f"{self.step_name}__{name}", value)

    def log_metrics(self):
        pass
