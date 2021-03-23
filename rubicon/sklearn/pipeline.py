from sklearn.pipeline import Pipeline

from rubicon.sklearn import get_logger


class RubiconPipeline(Pipeline):
    def __init__(self, steps, project, memory=None, verbose=False):
        self.project = project
        self.experiment = project.log_experiment("Logged from a RubiconPipeline")

        super().__init__(steps, memory=memory, verbose=verbose)

    def fit(self, X, y=None, **fit_params):
        pipeline = super().fit(X, y, **fit_params)

        for step_name, estimator in self.steps:
            print(f"Pipeline step: {step_name}")
            logger_cls = get_logger(estimator.__class__.__name__)
            logger = logger_cls(self.experiment)

            print("log the relavant params...")
            logger.log(prefix=step_name, parameters=estimator.get_params())

        return pipeline

    def score(self, X, y=None, sample_weight=None):
        score = super().score(X, y, sample_weight)

        print("log the score as a metric...")
        self.experiment.log_metric("dunno", value=score)

        return score
