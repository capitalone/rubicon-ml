class BaseEstimatorLogger:
    def __init__(self, experiment):
        self.experiment = experiment

    def log(self, parameters={}, metrics={}):
        pass
