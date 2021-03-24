class BaseEstimatorLogger:
    def __init__(self, experiment, estimator_name):
        self.experiment = experiment
        self.estimator_name = estimator_name

    def log(self, parameters={}, metrics={}):
        for name, value in parameters.items():
            self.experiment.log_parameter(name=f"{self.estimator_name}__{name}", value=value)

        for name, value in metrics.items():
            self.experiment.log_metric(name=f"{self.estimator_name}__{name}", value=value)
