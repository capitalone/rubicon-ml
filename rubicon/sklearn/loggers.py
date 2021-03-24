from .base_logger import BaseEstimatorLogger


class StandardScalerLogger(BaseEstimatorLogger):
    """
    This is an example, remove before final PR.
    """

    def __init__(self, experiment, estimator_name):
        super().__init__(experiment, estimator_name)

    def log(self, parameters={}, metrics={}):
        ignore_params_list = ["copy"]
        for ignore_param in ignore_params_list:
            parameters.pop(ignore_param, None)

        super().log(parameters, metrics)
