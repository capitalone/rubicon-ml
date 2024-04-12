from __future__ import annotations

from typing import TYPE_CHECKING, List

from rubicon_ml import domain
from rubicon_ml.client import (
    ArtifactMixin,
    Base,
    CommentMixin,
    DataframeMixin,
    Feature,
    Metric,
    Parameter,
    TagMixin,
)
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.client.utils.tags import filter_children
from rubicon_ml.exceptions import RubiconException

if TYPE_CHECKING:
    from rubicon_ml.client import Project
    from rubicon_ml.domain import Experiment as ExperimentDomain


class Experiment(Base, ArtifactMixin, DataframeMixin, TagMixin, CommentMixin):
    """A client experiment.

    An `experiment` represents a model run and is identified by
    its 'created_at' time. It can have `metrics`, `parameters`,
    `features`, `dataframes`, and `artifacts` logged to it.

    An `experiment` is logged to a `project`.

    Parameters
    ----------
    domain : rubicon.domain.Experiment
        The experiment domain model.
    parent : rubicon.client.Project
        The project that the experiment is logged to.
    """

    def __init__(self, domain: ExperimentDomain, parent: Project):
        super().__init__(domain, parent._config)

        self._domain: ExperimentDomain

        self._parent = parent
        self._artifacts = []
        self._dataframes = []
        self._metrics = []
        self._features = []
        self._parameters = []

    def _get_identifiers(self):
        """Get the experiment's project's name and the experiment's ID."""
        return self.project.name, self.id

    @failsafe
    def log_metric(
        self,
        name: str,
        value: float,
        directionality: str = "score",
        description: str = None,
        tags: list[str] = [],
        comments: list[str] = [],
    ) -> Metric:
        """Create a metric under the experiment.

        Parameters
        ----------
        name : str
            The metric's name.
        value : float
            The metric's value.
        directionality : str, optional
            The metric's directionality. Must be one of
            ["score", "loss"], where "score" represents
            a metric to maximize, while "loss" represents a
            metric to minimize. Defaults to "score".
        description : str, optional
            The metric's description. Use to provide additional
            context.
        tags : list of str, optional
            Values to tag the experiment with. Use tags to organize and
            filter your metrics.
        comments : list of str, optional
            Values to comment the experiment with. Use comments to organize and
            filter your metrics.

        Returns
        -------
        rubicon.client.Metric
            The created metric.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        if not isinstance(comments, list) or not all(
            [isinstance(comment, str) for comment in comments]
        ):
            raise ValueError("`comments` must be `list` of type `str`")

        metric = domain.Metric(
            name,
            value,
            directionality=directionality,
            description=description,
            tags=tags,
            comments=comments,
        )
        for repo in self.repositories:
            repo.create_metric(metric, self.project.name, self.id)

        return Metric(metric, self)

    @failsafe
    def metrics(self, name=None, tags=[], qtype="or"):
        """Get the metrics logged to this experiment.

        Parameters
        ----------
        name : str, optional
            The name value to filter results on.
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.Metric
            The metrics previously logged to this experiment.
        """
        return_err = None

        for repo in self.repositories:
            try:
                metrics = [Metric(m, self) for m in repo.get_metrics(self.project.name, self.id)]
            except Exception as err:
                return_err = err
            else:
                self._metrics = filter_children(metrics, tags, qtype, name)

                return self._metrics

        self._raise_rubicon_exception(return_err)

    @failsafe
    def metric(self, name=None, id=None):
        """Get a metric.

        Parameters
        ----------
        name : str, optional
            The name of the metric to get.
        id : str, optional
            The id of the metric to get.

        Returns
        -------
        rubicon.client.Metric
            The metric with name `name` or id `id`.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            return_err = None

            for repo in self.repositories:
                try:
                    metric = repo.get_metric(self.project.name, self.id, name)
                except Exception as err:
                    return_err = err
                else:
                    return Metric(metric, self)

            self._raise_rubicon_exception(return_err)
        else:
            return [m for m in self.metrics() if m.id == id][0]

    @failsafe
    def log_feature(
        self,
        name: str,
        description: str = None,
        importance: float = None,
        tags: list[str] = [],
        comments: list[str] = [],
    ) -> Feature:
        """Create a feature under the experiment.

        Parameters
        ----------
        name : str
            The features's name.
        description : str
            The feature's description. Use to provide
            additional context.
        importance : float
            The feature's importance.
        tags : list of str, optional
            Values to tag the experiment with. Use tags to organize and
            filter your features.
        comments : list of str, optional
            Values to comment the experiment with. Use comments to organize and
            filter your features.

        Returns
        -------
        rubicon.client.Feature
            The created feature.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        if not isinstance(comments, list) or not all(
            [isinstance(comment, str) for comment in comments]
        ):
            raise ValueError("`comments` must be `list` of type `str`")

        feature = domain.Feature(
            name, description=description, importance=importance, tags=tags, comments=comments
        )

        for repo in self.repositories:
            repo.create_feature(feature, self.project.name, self.id)

        return Feature(feature, self)

    @failsafe
    def features(self, name=None, tags=[], qtype="or"):
        """Get the features logged to this experiment.

         Parameters
        ----------
        name : str, optional
            The name value to filter results on.
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.Feature
            The features previously logged to this experiment.
        """
        return_err = None

        for repo in self.repositories:
            try:
                features = [Feature(f, self) for f in repo.get_features(self.project.name, self.id)]
            except Exception as err:
                return_err = err
            else:
                self._features = filter_children(features, tags, qtype, name)

                return self._features

        self._raise_rubicon_exception(return_err)

    @failsafe
    def feature(self, name=None, id=None):
        """Get a feature.

        Parameters
        ----------
        name : str, optional
            The name of the feature to get.
        id : str, optional
            The id of the feature to get.

        Returns
        -------
        rubicon.client.Feature
            The feature with name `name` or id `id`.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            return_err = None

            for repo in self.repositories:
                try:
                    feature = repo.get_feature(self.project.name, self.id, name)
                except Exception as err:
                    return_err = err
                else:
                    return Feature(feature, self)

            self._raise_rubicon_exception(return_err)
        else:
            return [f for f in self.features() if f.id == id][0]

    @failsafe
    def log_parameter(
        self,
        name: str,
        value: object = None,
        description: str = None,
        tags: list[str] = [],
        comments: list[str] = [],
    ) -> Parameter:
        """Create a parameter under the experiment.

        Parameters
        ----------
        name : str
            The parameter's name.
        value : object, optional
            The parameter's value. Can be an object of any JSON
            serializable (via rubicon.utils.DomainJSONEncoder)
            type.
        description : str, optional
            The parameter's description. Use to provide
            additional context.
        tags : list of str, optional
            Values to tag the parameter with. Use tags to organize and
            filter your parameters.
        comments : list of str, optional
            Values to comment the experiment with. Use comments to organize and
            filter your features.

        Returns
        -------
        rubicon.client.Parameter
            The created parameter.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        if not isinstance(comments, list) or not all(
            [isinstance(comment, str) for comment in comments]
        ):
            raise ValueError("`comments` must be `list` of type `str`")

        parameter = domain.Parameter(
            name, value=value, description=description, tags=tags, comments=comments
        )

        for repo in self.repositories:
            repo.create_parameter(parameter, self.project.name, self.id)

        return Parameter(parameter, self)

    @failsafe
    def parameters(self, name=None, tags=[], qtype="or"):
        """Get the parameters logged to this experiment.

        Parameters
        ----------
        name : str, optional
            The name value to filter results on.
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.Parameter
            The parameters previously logged to this experiment.
        """
        return_err = None

        for repo in self.repositories:
            try:
                parameters = [
                    Parameter(p, self) for p in repo.get_parameters(self.project.name, self.id)
                ]
            except Exception as err:
                return_err = err
            else:
                self._parameters = filter_children(parameters, tags, qtype, name)

                return self._parameters

        self._raise_rubicon_exception(return_err)

    @failsafe
    def parameter(self, name=None, id=None):
        """Get a parameter.

        Parameters
        ----------
        name : str, optional
            The name of the parameter to get.
        id : str, optional
            The id of the parameter to get.

        Returns
        -------
        rubicon.client.Parameter
            The parameter with name `name` or id `id`.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            return_err = None

            for repo in self.repositories:
                try:
                    parameter = repo.get_parameter(self.project.name, self.id, name)
                except Exception as err:
                    return_err = err
                else:
                    return Parameter(parameter, self)

            self._raise_rubicon_exception(return_err)
        else:
            return [p for p in self.parameters() if p.id == id][0]

    def add_child_experiment(self, experiment: Experiment):
        """Add tags to denote `experiment` as a descendent of this experiment.

        Parameters
        ----------
        experiment : rubicon_ml.client.Experiment
            The experiment to mark as a descendent of this experiment.

        Raises
        ------
        RubiconException
            If `experiment` and this experiment are not logged to the same project.
        """
        if experiment.project.id != self.project.id:
            raise RubiconException(
                "Descendents must be logged to the same project. Project"
                f"{experiment.project.id} does not match project {self.project.id}."
            )

        child_tag = f"child:{experiment.id}"
        parent_tag = f"parent:{self.id}"

        self.add_tags([child_tag])
        experiment.add_tags([parent_tag])

    def _get_experiments_from_tags(self, tag_key: str):
        """Get the experiments with `experiment_id`s in this experiment's tags
        that match the format `tag_key:experiment_id`.

        Returns
        -------
        list of rubicon_ml.client.Experiment
            The experiments with `experiment_id`s in this experiment's tags.
        """
        try:
            experiment_ids = self.tags[tag_key]
        except KeyError:
            return []

        if not isinstance(experiment_ids, list):
            experiment_ids = [experiment_ids]

        return [self.project.experiment(id=exp_id) for exp_id in experiment_ids]

    def get_child_experiments(self) -> List[Experiment]:
        """Get the experiments that are tagged as children of this experiment.

        Returns
        -------
        list of rubicon_ml.client.Experiment
            The experiments that are tagged as children of this experiment.
        """
        return self._get_experiments_from_tags("child")

    def get_parent_experiments(self) -> List[Experiment]:
        """Get the experiments that are tagged as parents of this experiment.

        Returns
        -------
        list of rubicon_ml.client.Experiment
            The experiments that are tagged as parents of this experiment.
        """
        return self._get_experiments_from_tags("parent")

    @property
    def id(self):
        """Get the experiment's id."""
        return self._domain.id

    @property
    def name(self):
        """Get the experiment's name."""
        return self._domain.name

    @property
    def description(self):
        """Get the experiment's description."""
        return self._domain.description

    @property
    def model_name(self):
        """Get the experiment's model name."""
        return self._domain.model_name

    @property
    def branch_name(self):
        """Get the experiment's branch name."""
        return self._domain.branch_name

    @property
    def commit_hash(self):
        """Get the experiment's commit hash."""
        return self._domain.commit_hash

    @property
    def training_metadata(self):
        """Get the project's training metadata."""
        training_metadata = self._domain.training_metadata.training_metadata

        if len(training_metadata) == 1:
            training_metadata = training_metadata[0]

        return training_metadata

    @property
    def created_at(self):
        """Get the time the experiment was created."""
        return self._domain.created_at

    @property
    def project(self):
        """Get the project client object that this experiment
        belongs to.
        """
        return self._parent
