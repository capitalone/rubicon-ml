from sklearn.pipeline import Pipeline


class RubiconPipeline(Pipeline):
    def __init__(self, steps, project, memory=None, verbose=False):
        self.project = project

        super().__init__(steps, memory=memory, verbose=verbose)
