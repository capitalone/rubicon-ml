class BaseEstimatorLogger:
    def __init__(self, experiment):
        self.experiment = experiment

    def log(self, prefix="", parameters={}, metrics={}):
        for name, value in parameters.items():
            self.experiment.log_parameter(name=f"{prefix}__{name}", value=value)

        for name, value in metrics.items():
            self.experiment.log_metric(name=f"{prefix}__{name}", value=value)
