import warnings

from .base_logger import BaseEstimatorLogger


class IgnoreLogger(BaseEstimatorLogger):
    def __init__(self, experiment, step_name, estimator, ignore=[]):
        self.ignore = ignore

        super().__init__(experiment, step_name, estimator)

    def log_parameters(self):
        for name, value in self.estimator.get_params().items():
            if name not in self.ignore:
                try:
                    self.experiment.log_parameter(name=f"{self.step_name}__{name}", value=value)
                except Exception:
                    warning = (
                        f"step '{self.step_name}' failed to write parameter '{name}' with value {value} "
                        f"of type {type(value)}. try using the `IgnoreLogger` with the parameter '{name}'"
                    )

                    warnings.warn(warning)
