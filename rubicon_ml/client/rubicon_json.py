import copy
import numbers
from typing import Any, Dict, List, Optional, Type, Union

from jsonpath_ng.ext import parse

from rubicon_ml.client import Experiment, Project, Rubicon


class RubiconJSON:
    """`RubiconJSON` converts top-level `rubicon_ml` objects, `projects`, and `experiments`
    into a JSON structured dictionary for JSONPath-like querying with `jsonpath-ng`.

    Parameters
    ----------
    rubicon_objects : rubicon.client.Rubicon or `list` of type rubicon.client.Rubicon
        Top-level `rubicon-ml` objects to convert to JSON for querying.
    projects : rubicon.client.Project or `list` of type rubicon.client.Project
        `rubicon-ml` projects to convert to JSON for querying.
    experiments : rubicon.client.Experiment or `list` of type rubicon.client.Experiment
        `rubicon-ml` experiments to convert to JSON for querying.
    """

    def __init__(
        self,
        rubicon_objects: Optional[List[Rubicon]] = None,
        projects: Optional[List[Project]] = None,
        experiments: Optional[List[Experiment]] = None,
    ):
        if rubicon_objects:
            rubicon_objects = self._validate_input(rubicon_objects, Rubicon, "rubicon_objects")
        if projects:
            projects = self._validate_input(projects, Project, "projects")
        if experiments:
            experiments = self._validate_input(experiments, Experiment, "experiments")

        self._json = self._convert_to_json(rubicon_objects, projects, experiments)
        self._json_numeric = self._convert_to_json(
            rubicon_objects, projects, experiments, filter_nonnumeric=True
        )

    def _validate_input(
        self,
        objects: Union[
            Rubicon, List[Rubicon], Project, List[Project], Experiment, List[Experiment]
        ],
        obj_cls: Union[Type[Rubicon], Type[Project], Type[Experiment]],
        obj_name: str,
    ):
        formatted_objects = [objects] if not isinstance(objects, list) else objects

        if not all([isinstance(o, obj_cls) for o in formatted_objects]):
            raise ValueError(f"`{obj_name}` must be `list` of type `{obj_cls}`.")

        return formatted_objects

    def _convert_to_json(
        self,
        rubicon_objects: Optional[List[Rubicon]] = None,
        projects: Optional[List[Project]] = None,
        experiments: Optional[List[Experiment]] = None,
        filter_nonnumeric: bool = False,
    ):
        rubicon_json = {}

        if rubicon_objects is not None:
            rubicon_json["project"] = self._rubicon_to_json(
                rubicon_objects, filter_nonnumeric=filter_nonnumeric
            )["project"]

        if projects is not None:
            project_json = rubicon_json.get("project", [])
            project_json.extend(
                self._projects_to_json(projects, filter_nonnumeric=filter_nonnumeric)["project"]
            )
            rubicon_json["project"] = project_json

        if experiments is not None:
            rubicon_json["experiment"] = self._experiments_to_json(
                experiments, filter_nonnumeric=filter_nonnumeric
            )["experiment"]

        return rubicon_json

    def _experiments_to_json(self, experiments: List[Experiment], filter_nonnumeric: bool = False):
        rubicon_json: Dict[str, Any] = {"experiment": []}

        for e in experiments:
            experiment_json = copy.deepcopy(e._domain.__dict__)
            experiment_json["feature"] = [f._domain.__dict__ for f in e.features()]

            experiment_json["parameter"] = []
            for parameter in e.parameters():
                if not filter_nonnumeric or isinstance(parameter.value, numbers.Number):
                    experiment_json["parameter"].append(parameter._domain.__dict__)

            experiment_json["metric"] = []
            for metric in e.metrics():
                if not filter_nonnumeric or isinstance(metric.value, numbers.Number):
                    experiment_json["metric"].append(metric._domain.__dict__)

            experiment_json["artifact"] = [a._domain.__dict__ for a in e.artifacts()]
            experiment_json["dataframe"] = [d._domain.__dict__ for d in e.dataframes()]

            rubicon_json["experiment"].append(experiment_json)

        return rubicon_json

    def _projects_to_json(self, projects: List[Project], filter_nonnumeric: bool = False):
        rubicon_json: Dict[str, Any] = {"project": []}

        for p in projects:
            project_json = p._domain.__dict__
            project_json["artifact"] = [a._domain.__dict__ for a in p.artifacts()]
            project_json["dataframe"] = [d._domain.__dict__ for d in p.dataframes()]

            experiment_json = self._experiments_to_json(
                p.experiments(), filter_nonnumeric=filter_nonnumeric
            )
            project_json["experiment"] = experiment_json["experiment"]

            rubicon_json["project"].append(project_json)

        return rubicon_json

    def _rubicon_to_json(self, rubicon_objects: List[Rubicon], filter_nonnumeric: bool = False):
        rubicon_json: Dict[str, Any] = {"project": []}

        for r in rubicon_objects:
            rubicon_json["project"].extend(
                self._projects_to_json(r.projects(), filter_nonnumeric=filter_nonnumeric)["project"]
            )

        return rubicon_json

    def search(self, query: str):
        """
        Query the JSON generated from the RubiconJSON instantiation in a JSONPath-like manner.
        Can return queries as `rubicon_ml.client` objects by specifying return_type parameter.
        Will return as JSON structured `dict` by default.

        Parameters
        ----------
        query: JSONPath-like query
        """

        if ">" in query or "<" in query:
            # non-numerics break greater than and less than comparisons in `jsonpath_ng`
            # so we use the json with replaced non-numeric values when '>' or '<' appear
            # in the `query`
            json = self._json_numeric
        else:
            json = self._json

        return parse(query).find(json)

    @property
    def json(self):
        """
        The json representation of the `rubicon-ml` objects.
        """
        return self._json

    @property
    def json_numeric(self):
        """
        The json representation of the `rubicon-ml` objects with numeric values.
        """
        return self._json_numeric
