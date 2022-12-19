from jsonpath_ng.ext import parse

from rubicon_ml.client import (
    Artifact,
    Dataframe,
    Experiment,
    Feature,
    Metric,
    Parameter,
    Project,
    Rubicon,
)
from rubicon_ml.domain import (
    Artifact as DomainArtifact,
    Dataframe as DomainDataframe,
    Experiment as DomainExperiment,
    Feature as DomainFeature,
    Metric as DomainMetric,
    Parameter as DomainParameter,
    Project as DomainProject,
)


class NoOpParent:
    """A read-only parent object"""

    @property
    def _config(self):
        return None


class RubiconJSON:
    def __init__(self, rubicon_objects=None, projects=None, experiments=None):
        self._json = self._convert_to_json(rubicon_objects, projects, experiments)

    def search(self, query, return_type=None):
        if return_type is not None:
            return_type = return_type.lower()
            if return_type not in [
                "artifact",
                "dataframe",
                "experiment",
                "feature",
                "metric",
                "parameter",
                "project",
            ]:
                raise ValueError(
                    "`return_type` must be artifact, dataframe, experiment, feature, metric, parameter, or project."
                )

        res = parse(query).find(self._json)
        if not return_type:
            return res

        return_objects = []
        if return_type == "artifact":
            for match in res:
                for i in range(len(match.value)):
                    return_objects.append(Artifact(DomainArtifact(**match.value[i]), NoOpParent()))
        elif return_type == "dataframe":
            for match in res:
                for i in range(len(match.value)):
                    return_objects.append(
                        Dataframe(DomainDataframe(**match.value[i]), NoOpParent())
                    )
        elif return_type == "experiment":
            for match in res:
                for key in ["feature", "parameter", "metric", "artifact", "dataframe"]:
                    if key in match.value:
                        del match.value[key]
                return_objects.append(Experiment(DomainExperiment(**match.value), NoOpParent()))
        elif return_type == "feature":
            for match in res:
                for i in range(len(match.value)):
                    return_objects.append(Feature(DomainFeature(**match.value[i]), NoOpParent()))
        elif return_type == "metric":
            for match in res:
                for i in range(len(match.value)):
                    return_objects.append(Metric(DomainMetric(**match.value[i]), NoOpParent()))
        elif return_type == "parameter":
            for match in res:
                for i in range(len(match.value)):
                    return_objects.append(
                        Parameter(DomainParameter(**match.value[i]), NoOpParent())
                    )
        elif return_type == "project":
            for match in res:
                for key in ["artifact", "dataframe", "experiment"]:
                    if key in match.value:
                        del match.value[key]
                return_objects.append(Project(DomainProject(**match.value), NoOpParent()))

        return return_objects

    def _convert_to_json(self, rubicon_objects=None, projects=None, experiments=None):

        json = None
        if rubicon_objects is not None:
            if not isinstance(rubicon_objects, Rubicon):
                if not isinstance(rubicon_objects, list) or not all(
                    [isinstance(rb, Rubicon) for rb in rubicon_objects]
                ):
                    raise ValueError(
                        "`rubicon_objects` must be of type `Rubicon` or `list` of type `Rubicon`"
                    )
            json = self._rubicon_to_json(rubicon_objects)

        if projects is not None:
            if not isinstance(projects, Project):
                if not isinstance(projects, list) or not all(
                    [isinstance(pr, Project) for pr in projects]
                ):
                    raise ValueError(
                        "`projects` must be of type `Project` or `list` of type `Project`"
                    )
            if json is None:
                json = self._projects_to_json(projects)
            else:
                new_json = self._projects_to_json(projects)
                for pr in new_json["project"]:
                    json["project"].append(pr)

        if experiments is not None:
            if not isinstance(experiments, Experiment):
                if not isinstance(experiments, list) or not all(
                    [isinstance(e, Experiment) for e in experiments]
                ):
                    raise ValueError(
                        "`experiments` must be of type `Experiment` or `list` of type `Experiment`"
                    )
            if json is None:
                json = self._experiments_to_json(experiments)
            else:
                new_json = self._experiments_to_json(experiments)
                if json.get("experiment") is None:
                    json["experiment"] = []
                for e in new_json["experiment"]:
                    json["experiment"].append(e)

        return json

    def _experiments_to_json(self, experiments):

        if not isinstance(experiments, list):
            experiments = [experiments]

        json = {}
        json["experiment"] = []
        for e in experiments:
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

    def _projects_to_json(self, projects):

        if not isinstance(projects, list):
            projects = [projects]

        json = {}
        json["project"] = []
        for pr in projects:
            project_json = pr._domain.__dict__
            project_json["artifact"] = []
            for a in pr.artifacts():
                project_json["artifact"].append(a._domain.__dict__)

            project_json["dataframe"] = []
            for d in pr.dataframes():
                project_json["dataframe"].append(d._domain.__dict__)

            experiment_json = self._experiments_to_json(pr.experiments())
            project_json["experiment"] = experiment_json["experiment"]

            json["project"].append(project_json)

        return json

    def _rubicon_to_json(self, rubicon_objects):

        if not isinstance(rubicon_objects, list):
            rubicon_objects = [rubicon_objects]

        json = None
        for rb in rubicon_objects:
            for pr in rb.projects():
                if json is None:
                    json = self._projects_to_json(pr)
                else:
                    new_json = self._projects_to_json(pr)
                    for p in new_json["project"]:
                        json["project"].append(p)

        return json

    @property
    def json(self):
        return self._json
