import warnings

from rubicon.exceptions import RubiconException
from rubicon.sklearn.base_logger import BaseEstimatorLogger


class FilteredEstimatorLogger(BaseEstimatorLogger):
    def __init__(self, experiment, step_name, estimator, select=[], ignore=[], ignore_all=False):
        if ignore and select:
            raise RubiconException("provide either `select` OR `ignore`, not both")

        self.ignore = ignore
        self.ignore_all = ignore_all
        self.select = select

        super().__init__(experiment, step_name, estimator)

    def log_parameters(self):
        if self.ignore_all:
            return

        for name, value in self.estimator.get_params().items():
            if (self.ignore and name not in self.ignore) or (self.select and name in self.select):
                self._log_parameter_to_rubicon(f"{self.step_name}__{name}", value)
