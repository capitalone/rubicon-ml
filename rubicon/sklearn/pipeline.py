from sklearn.pipeline import Pipeline


class RubiconPipeline(Pipeline):
    def __init__(self, steps, project, memory=None, verbose=False):
        self.project = project
        self.experiment = project.log_experiment('Logged from a RubiconPipeline')
        
        super().__init__(steps, memory=memory, verbose=verbose)

    def fit(self, X, y=None, **fit_params):
        pipeline = super().fit(X, y, **fit_params)
        
        step_names, _ = zip(*self.steps)
        print(f"Pipeline steps: {step_names}")
        
        print(f"log the relavant params...")
        for name, value in self._relavant_params().items():
            self.experiment.log_parameter(name, value=value)

        return pipeline

    def score(self, X, y=None, sample_weight=None):
        score = super().score(X, y, sample_weight)
        
        print("log the score as a metric...")
        self.experiment.log_metric("dunno", value=score)
        
        return score


    def _relavant_params(self):
        fit_params_steps = {}
        for pname, pval in self.get_params().items():
            if '__' in pname:
                fit_params_steps[pname] = pval

        return fit_params_steps