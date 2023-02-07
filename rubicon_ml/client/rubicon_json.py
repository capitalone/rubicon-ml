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
    """
    RubiconJSON is a converting class which converts top-level `rubicon_ml` objects,
    `projects`, and `experiments` into a JSON structured `dict` for
    JSONPath-like querying.

    Parameters
    ----------
    rubicon_objects : rubicon.client.Rubicon or `list` of type rubicon.client.Rubicon
    projects : rubicon.client.Project or `list` of type rubicon.client.Project
    experiments : rubicon.client.Experiment or `list` of type rubicon.client.Experiment
    """

    def __init__(self, rubicon_objects=None, projects=None, experiments=None):
        self._json = self._convert_to_json(rubicon_objects, projects, experiments)
        if projects is not None and not isinstance(projects, list):
            projects = [projects]
        if rubicon_objects is not None and not isinstance(rubicon_objects, list):
            rubicon_objects = [rubicon_objects]
        if experiments is not None and not isinstance(experiments, list):
            experiments = [experiments]

        self._rubicon_objects = rubicon_objects
        self._projects = projects
        self._experiments = experiments

    def _fetch_query_string_type(self, query_string):
        split = query_string.split(".")
        if split[-1][-1] == "]":
            str = split[-1].split("[")[0]
        else:
            str = split[-2].split("[")[0]
        return str

    def search(self, query, return_type=None):

        """
        Query the JSON generated from the RubiconJSON instantiation in a JSONPath-like manner.
        Can return queries as `rubicon_ml.client` objects by specifying return_type parameter.
        Will return as JSON structured `dict` by default.

        Parameters
        ----------
        query: JSONPath-like query
        return_type: "artifact", "dataframe", "experiment", "feature", "metric", "parameter", or "project" (optional)
        """

        if return_type is not None:

            return_types = {
                "artifact",
                "dataframe",
                "experiment",
                "feature",
                "metric",
                "parameter",
                "project",
            }
            if return_type not in return_types:
                raise ValueError(
                    "`return_type` must be artifact, dataframe, experiment, feature, metric, parameter, or project."
                )

        res = parse(query).find(self._json)
        if not return_type:
            return res

        return_objects = []
        query_string_type = self._fetch_query_string_type(
            query_string=query, return_type=return_type
        )
        if return_type == "artifact":

            for match in res:

                if isinstance(match.value, list):
                    for i in range(len(match.value)):
                        id = match.value[i]["id"]
                        parent = self._fetch_parent_object(
                            queried_object=query_string_type, id=id, return_type="artifact"
                        )
                        return_objects.append(Artifact(DomainArtifact(**match.value[i]), parent))
                else:
                    id = match.value["id"]
                    parent = self._fetch_parent_object(
                      queried_object=query_string_type, id=id, return_type="artifact"
                    ) 
                    return_objects.append(Artifact(DomainArtifact(**match.value), parent))
        elif return_type == "dataframe":
            for match in res:
                if isinstance(match.value, list):
                    for i in range(len(match.value)):
                        id = match.value[i]["id"]
                        parent = self._fetch_parent_object(
                            queried_object=query_string_type, id=id, return_type="dataframe"
                        )
                        return_objects.append(Dataframe(DomainDataframe(**match.value[i]), parent))
                else:
                    id = match.value["id"]
                    parent = self._fetch_parent_object(
                        queried_object=query_string_type, id=id, return_type="dataframe"
                    )
                    return_objects.append(Dataframe(DomainDataframe(**match.value), parent))
        

        elif return_type == "experiment":
            for match in res:
                if isinstance(match.value, list):
                    for i in range(len(match.value)):
                        for key in ["feature", "parameter", "metric", "artifact", "dataframe"]:
                            if key in match.value:
                                del match.value[key]
                    id = match.value["id"]
                    parent = self._fetch_parent_object(
                        queried_object=query_string_type, id=id, return_type="experiment"
                    )
                    return_objects.append(Experiment(DomainExperiment(**match.value), parent))
                else:
                    for key in ["feature", "parameter", "metric", "artifact", "dataframe"]:
                        if key in match.value:
                            del match.value[key]
                    id = match.value["id"]
                    parent = self._fetch_parent_object(
                        queried_object=query_string_type, id=id, return_type="experiment"
                    )
                    return_objects.append(Experiment(DomainExperiment(**match.value), parent))
                    
        elif return_type == "feature":
            for match in res:
                if isinstance(match.value, list):
                    for i in range(len(match.value)):
                        id = match.value[i]["id"]
                        parent = self._fetch_parent_object(
                            queried_object=query_string_type, id=id, return_type="feature"
                        )
                        return_objects.append(Feature(DomainFeature(**match.value[i]), parent))
                else:
                    id = match.value["id"]
                    parent = self._fetch_parent_object(
                        queried_object=query_string_type, id=id, return_type="feature"
                    )
                    return_objects.append(Feature(DomainFeature(**match.value), parent))

        elif return_type == "metric":
            for match in res:
                if isinstance(match.value, list):
                    for i in range(len(match.value)):
                        id = match.value[i]["id"]
                        parent = self._fetch_parent_object(
                            queried_object=query_string_type, id=id, return_type="metric"
                        )
                        return_objects.append(Metric(DomainMetric(**match.value[i]), parent))
                else:
                    id = match.value["id"]
                    parent = self._fetch_parent_object(
                        queried_object=query_string_type, id=id, return_type="metric"
                    )
                    return_objects.append(Metric(DomainMetric(**match.value), parent))
                    
               

        elif return_type == "parameter":
            for match in res:
                if isinstance(match.value, list):
                    for i in range(len(match.value)):
                        id = match.value[i]["id"]
                        parent = self._fetch_parent_object(
                            queried_object=query_string_type, id=id, return_type="parameter"
                        )
                        return_objects.append(Parameter(DomainParameter(**match.value[i]), parent))
                else:
                        id = match.value["id"]
                        parent = self._fetch_parent_object(
                            queried_object=query_string_type, id=id, return_type="parameter"
                        )
                        return_objects.append(Parameter(DomainParameter(**match.value), parent))
 
        elif return_type == "project":
            for match in res:
                if isinstance(match.value, list):
                    for i in range(len(match.value)):
                        for key in ["artifact", "dataframe", "experiment"]:
                            if key in match.value[i]:
                                del match.value[i][key]
                        id = match.value[i]["id"]
                        parent = self._fetch_parent_object(
                            queried_object=query_string_type, id=id, return_type="project"
                        )
                        return_objects.append(
                            Project(DomainProject(**match.value[i]), NoOpParent())
                        )
                else:
                    for key in ["artifact", "dataframe", "experiment"]:
                        if key in match.value:
                            del match.value[key]
                    id = match.value["id"]
                    parent = self._fetch_parent_object(
                        queried_object=query_string_type, id=id, return_type="project"
                    )
                    return_objects.append(Project(DomainProject(**match.value), parent))

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
            self._experiments = experiments

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
            self._projects = projects

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
            self._rubicon_objects = rubicon_objects

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

    def _fetch_parent_object(self, queried_object, id, return_type):
        """
        Returns the appropriate parent of the queried object to eventually append
        to the return list of the query
        """
        if (
            (
                queried_object == "experiment"
                and (self._experiments is None and self._projects is None)
            )
            or queried_object == "project"
            and (self._rubicon_objects is None and self._projects is None)
        ):

            raise ValueError("Required input not given to idenifty proper parent of queried object")

        if return_type == "project":
            for rb in self._rubicon_objects:
                for project in rb.projects():
                    if id == project.id:
                        return rb.config
        else:
            parent = None
            if self._experiments is not None and return_type in [
                "metric",
                "feature",
                "parameter",
                "dataframe",
                "artifact",
            ]:
                if return_type in ["metric", "feature", "parameter"]:
                    for experiment in self._experiments:
                        experiment_children = {
                            "metric": experiment.metrics,
                            "feature": experiment.features,
                            "parameter": experiment.parameters,
                        }

                        for ret in experiment_children[return_type]():
                            if id == ret.id:
                                parent = experiment
                                return experiment
                else:
                    for experiment in self._experiments:
                        experiment_children = {
                            "dataframe": experiment.dataframes,
                            "artifact": experiment.artifacts,
                        }
                        for ret in experiment_children[return_type]():
                            if id == ret.id:
                                parent = experiment
                                return experiment

            if parent is None and self._projects is not None:
                if return_type in ["metric", "feature", "parameter"]:

                    for project in self._projects:
                        for experiment in project.experiments():
                            experiment_children = {
                                "metric": experiment.metrics,
                                "feature": experiment.features,
                                "parameter": experiment.parameters,
                            }

                            for ret in experiment_children[return_type]():
                                if id == ret.id:
                                    parent = experiment
                                    return experiment
                else:

                    if queried_object == "project" or return_type == "experiment":
                        for project in self._projects:
                            project_children = {
                                "experiment": project.experiment,
                                "dataframe": project.dataframe,
                                "artifact": project.artifact,
                            }

                            try:
                                parent = project_children[return_type](id=id)
                            except Exception:
                                continue
                            if parent is not None:
                                parent = project
                                return project
                    elif queried_object == "experiment":
                        for project in self._projects:
                            for experiment in project.experiments():
                                experiment_children = {
                                    "dataframe": experiment.dataframes,
                                    "artifact": experiment.artifacts,
                                }
                                for ret in experiment_children[return_type]():
                                    if id == ret.id:
                                        parent = experiment
                                        return experiment

            if parent is None and self._rubicon_objects is not None:
                if return_type in ["metric", "feature", "parameter"]:
                    # Parent of queried object is Experiments
                    for rb in self._rubicon_objects:
                        for project in rb.projects():
                            for experiment in project.experiments():
                                experiment_children = {
                                    "metric": experiment.metrics,
                                    "feature": experiment.features,
                                    "parameter": experiment.parameters,
                                }
                                for ret in experiment_children[return_type]():
                                    if id == ret.id:
                                        return experiment

                else:
                    # Parent of queried object is Project
                    for rb in self._rubicon_objects:
                        for project in rb.projects():
                            project_children = {
                                "experiment": project.experiment,
                                "dataframe": project.dataframe,
                                "artifact": project.artifact,
                            }

                            try:
                                parent = project_children[return_type](id=id)
                            except Exception:
                                continue
                            if parent is not None:
                                return project

        return parent

    @property
    def json(self):
        return self._json
