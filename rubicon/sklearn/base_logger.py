import warnings


class BaseEstimatorLogger:
    def __init__(self, experiment, step_name, estimator):
        self.estimator = estimator
        self.experiment = experiment
        self.step_name = step_name

    def log_parameters(self):
        for name, value in self.estimator.get_params().items():
            try:
                self.experiment.log_parameter(name=f"{self.step_name}__{name}", value=value)
            except Exception:
                warning = (
                    f"step '{self.step_name}' failed to write parameter '{name}' with value {value} "
                    f"of type {type(value)}. try using the `IgnoreLogger` with the parameter '{name}'"
                )

                warnings.warn(warning)

    def log_metrics(self):
        pass
