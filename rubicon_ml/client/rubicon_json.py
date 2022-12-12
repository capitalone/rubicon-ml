from jsonpath_ng.ext import parse

from rubicon_ml import Experiment, Project, Rubicon


class RubiconJSON:
    def __init__(self, rubicon_entities):
        self._rubicon_json = self.project_to_json(rubicon_entities)

    def search(self, query):
        return parse(query).find(self._rubicon_json)

    def rubicon_to_json(self, rubicon):

        if not isinstance(rubicon, Rubicon):
            if not isinstance(rubicon, list) or not all(
                [isinstance(rb, Rubicon) for rb in rubicon]
            ):
                raise ValueError(
                    "`rubicon_objects` must be of type `Rubicon` or `list` of type `Rubicon`"
                )

        if not isinstance(rubicon, list):
            rubicon = [rubicon]

        json = None
        for rb in rubicon:
            for pr in rb.projects():
                if json is None:
                    json = self.project_to_json(pr)
                else:
                    new_json = self.project_to_json(pr)
                    for p in new_json["project"]:
                        json["project"].append(p)

        return json

    def convert_to_json(self, rubicon_objects=None, projects=None, experiments=None):

        json = None
        if rubicon_objects is not None:
            json = self.rubicon_to_json(rubicon_objects)
        if projects is not None:
            if json is None:
                json = self.project_to_json(projects)
            else:
                new_json = self.project_to_json(projects)
                for pr in new_json["project"]:
                    json["project"].append(pr)
        if experiments is not None:
            if json is None:
                json = self.experiment_to_json(experiments)
            else:
                new_json = self.experiment_to_json(experiments)
                if json.get("experiment") is None:
                    json["experiment"] = []
                for e in new_json["experiment"]:
                    json["experiment"].append(e)

        return json

    def experiment_to_json(self, experiment):
        if not isinstance(experiment, Experiment):
            if not isinstance(experiment, list) or not all(
                [isinstance(e, Experiment) for e in experiment]
            ):
                raise ValueError(
                    "`experiment` must be of type `Experiment` or `list` of type `Experiment`"
                )

        if not isinstance(experiment, list):
            experiment = [experiment]

        json = {}
        json["experiment"] = []
        for e in experiment:
            experiment_json = e._domain.__dict__
            experiment_json["feature"] = []
            for f in e.features():
                experiment_json["feature"].append(f._domain.__dict__)

            experiment_json["parameter"] = []
            for p in e.parameters():
                experiment_json["parameter"].append(p._domain.__dict__)

            experiment_json["metric"] = []
            for m in e.metrics():
                experiment_json["metric"].append(m._domain.__dict__)

            experiment_json["artifact"] = []
            for a in e.artifacts():
                experiment_json["artifact"].append(a._domain.__dict__)

            experiment_json["dataframe"] = []
            for d in e.dataframes():
                experiment_json["dataframe"].append(d._domain.__dict__)

            json["experiment"].append(experiment_json)

        return json

    def project_to_json(self, project):
        if not isinstance(project, Project):
            if not isinstance(project, list) or not all(
                [isinstance(pr, Project) for pr in project]
            ):
                raise ValueError("`project` must be of type `Project` or `list` of type `Project`")

        if not isinstance(project, list):
            project = [project]

        json = {}
        json["project"] = []
        for pr in project:
            project_json = pr._domain.__dict__
            project_json["artifact"] = []
            for a in pr.artifacts():
                project_json["artifact"].append(a._domain.__dict__)

            project_json["dataframe"] = []
            for d in pr.dataframes():
                project_json["dataframe"].append(d._domain.__dict__)

            experiment_json = self.experiment_to_json(pr.experiments())
            project_json["experiment"] = experiment_json["experiment"]

            json["project"].append(project_json)

        return json
