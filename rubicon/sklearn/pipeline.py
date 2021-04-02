from sklearn.pipeline import Pipeline

from rubicon.sklearn.base_logger import BaseLogger


class RubiconPipeline(Pipeline):
    def __init__(self, project, steps, user_defined_loggers={}, memory=None, verbose=False):
        self.project = project
        self.user_defined_loggers = user_defined_loggers
        self.experiment = project.log_experiment("Logged from a RubiconPipeline")

        super().__init__(steps, memory=memory, verbose=verbose)

    def fit(self, X, y=None, tags=None, **fit_params):
        pipeline = super().fit(X, y, **fit_params)

        # empty experiments are being logged during
        # the grid search run so using tags to track
        # the relevant data
        if tags is not None:
            self.experiment.add_tags(tags)

        for step_name, estimator in self.steps:
            logger = self.user_defined_loggers.get(step_name) or BaseLogger()

            logger.set_experiment(self.experiment)
            logger.set_step_name(step_name)
            logger.set_estimator(estimator)

            logger.log_parameters()

        return pipeline

    def score(self, X, y=None, sample_weight=None):
        score = super().score(X, y, sample_weight)

        self.experiment.log_metric("accuracy", value=score)

        return score
