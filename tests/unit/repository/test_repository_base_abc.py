"""Tests for the RepositoryBase abstract base class."""

import pandas as pd
import pytest

from rubicon_ml import domain
from rubicon_ml.domain.comment_update import CommentUpdate
from rubicon_ml.domain.tag_update import TagUpdate
from rubicon_ml.repository.base import RepositoryBase


# -------- Concrete test implementation --------


class ConcreteRepository(RepositoryBase):
    """Minimal implementation of RepositoryBase for testing."""

    PROTOCOL = "test"

    def __init__(self):
        self._written = []
        self._read_domain_return = None
        self._read_domains_return = []

    def write_domain(self, domain_obj, project_name, **kwargs):
        self._written.append((domain_obj, project_name, kwargs))

    def read_domain(self, domain_cls, project_name, **kwargs):
        return self._read_domain_return

    def read_domains(self, domain_cls, project_name=None, **kwargs):
        return self._read_domains_return

    def remove_domain(self, domain_cls, project_name, **kwargs):
        pass

    def write_artifact_data(self, data, project_name, artifact_id, **kwargs):
        self._written.append(("artifact_data", data, project_name, artifact_id, kwargs))

    def read_artifact_data(self, project_name, artifact_id, **kwargs):
        return b"artifact_bytes"

    def write_dataframe_data(self, df, project_name, dataframe_id, **kwargs):
        self._written.append(("dataframe_data", df, project_name, dataframe_id, kwargs))

    def read_dataframe_data(self, project_name, dataframe_id, **kwargs):
        return pd.DataFrame()


# -------- Tests: ABC contract --------


class TestRepositoryBaseABC:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError, match="abstract method"):
            RepositoryBase()

    def test_concrete_impl_can_be_instantiated(self):
        repo = ConcreteRepository()
        assert isinstance(repo, RepositoryBase)

    def test_protocol_attribute(self):
        repo = ConcreteRepository()
        assert repo.PROTOCOL == "test"

    def test_protocol_default_is_none(self):
        assert RepositoryBase.PROTOCOL is None

    def test_incomplete_impl_cannot_be_instantiated(self):
        class IncompleteRepository(RepositoryBase):
            def write_domain(self, domain_obj, project_name, **kwargs):
                pass

            # Missing all other abstract methods

        with pytest.raises(TypeError, match="abstract method"):
            IncompleteRepository()


# -------- Tests: Project convenience methods --------


class TestProjectConvenience:
    def test_create_project(self):
        repo = ConcreteRepository()
        project = domain.Project(name="Test Project")
        repo.create_project(project)

        assert len(repo._written) == 1
        obj, name, kwargs = repo._written[0]
        assert obj is project
        assert name == "Test Project"

    def test_get_project(self):
        repo = ConcreteRepository()
        expected = domain.Project(name="Test Project")
        repo._read_domain_return = expected

        result = repo.get_project("Test Project")
        assert result is expected

    def test_get_projects(self):
        repo = ConcreteRepository()
        expected = [domain.Project(name="P1"), domain.Project(name="P2")]
        repo._read_domains_return = expected

        result = repo.get_projects()
        assert result is expected


# -------- Tests: Experiment convenience methods --------


class TestExperimentConvenience:
    def test_create_experiment(self):
        repo = ConcreteRepository()
        experiment = domain.Experiment(name="Test Exp", project_name="proj")
        repo.create_experiment(experiment)

        obj, name, kwargs = repo._written[0]
        assert obj is experiment
        assert name == "proj"
        assert kwargs["experiment_id"] == experiment.id

    def test_get_experiment(self):
        repo = ConcreteRepository()
        expected = domain.Experiment(name="Test Exp", project_name="proj")
        repo._read_domain_return = expected

        result = repo.get_experiment("proj", expected.id)
        assert result is expected

    def test_get_experiments(self):
        repo = ConcreteRepository()
        expected = [domain.Experiment(name="E1", project_name="proj")]
        repo._read_domains_return = expected

        result = repo.get_experiments("proj")
        assert result is expected


# -------- Tests: Artifact convenience methods --------


class TestArtifactConvenience:
    def test_create_artifact(self):
        repo = ConcreteRepository()
        artifact = domain.Artifact(name="test_artifact")
        data = b"test_data"

        repo.create_artifact(artifact, data, "proj", experiment_id="exp-1")

        # First write: domain metadata
        obj, name, kwargs = repo._written[0]
        assert obj is artifact
        assert name == "proj"
        assert kwargs["experiment_id"] == "exp-1"
        assert kwargs["entity_identifier"] == artifact.id
        assert kwargs["entity_type"] == "Artifact"

        # Second write: binary data
        assert repo._written[1][0] == "artifact_data"
        assert repo._written[1][1] == data
        assert repo._written[1][3] == artifact.id

    def test_get_artifact_metadata(self):
        repo = ConcreteRepository()
        expected = domain.Artifact(name="test")
        repo._read_domain_return = expected

        result = repo.get_artifact_metadata("proj", expected.id, experiment_id="exp-1")
        assert result is expected

    def test_get_artifacts_metadata(self):
        repo = ConcreteRepository()
        expected = [domain.Artifact(name="a1")]
        repo._read_domains_return = expected

        result = repo.get_artifacts_metadata("proj", experiment_id="exp-1")
        assert result is expected

    def test_get_artifact_data(self):
        repo = ConcreteRepository()
        result = repo.get_artifact_data("proj", "art-1", experiment_id="exp-1")
        assert result == b"artifact_bytes"

    def test_delete_artifact(self):
        repo = ConcreteRepository()
        # Should not raise
        repo.delete_artifact("proj", "art-1", experiment_id="exp-1")


# -------- Tests: Dataframe convenience methods --------


class TestDataframeConvenience:
    def test_create_dataframe(self):
        repo = ConcreteRepository()
        dataframe = domain.Dataframe()
        data = pd.DataFrame({"a": [1]})

        repo.create_dataframe(dataframe, data, "proj", experiment_id="exp-1")

        obj, name, kwargs = repo._written[0]
        assert obj is dataframe
        assert kwargs["entity_type"] == "Dataframe"

        assert repo._written[1][0] == "dataframe_data"

    def test_get_dataframe_data(self):
        repo = ConcreteRepository()
        result = repo.get_dataframe_data("proj", "df-1", experiment_id="exp-1")
        assert isinstance(result, pd.DataFrame)


# -------- Tests: Feature/Metric/Parameter convenience methods --------


class TestFeatureConvenience:
    def test_create_feature(self):
        repo = ConcreteRepository()
        feature = domain.Feature(name="feat_1")
        repo.create_feature(feature, "proj", "exp-1")

        obj, name, kwargs = repo._written[0]
        assert obj is feature
        assert kwargs["entity_identifier"] == "feat_1"
        assert kwargs["entity_type"] == "Feature"

    def test_get_feature(self):
        repo = ConcreteRepository()
        expected = domain.Feature(name="feat_1")
        repo._read_domain_return = expected

        result = repo.get_feature("proj", "exp-1", "feat_1")
        assert result is expected


class TestMetricConvenience:
    def test_create_metric(self):
        repo = ConcreteRepository()
        metric = domain.Metric(name="acc", value=0.95)
        repo.create_metric(metric, "proj", "exp-1")

        obj, name, kwargs = repo._written[0]
        assert obj is metric
        assert kwargs["entity_identifier"] == "acc"
        assert kwargs["entity_type"] == "Metric"


class TestParameterConvenience:
    def test_create_parameter(self):
        repo = ConcreteRepository()
        param = domain.Parameter(name="lr", value=0.01)
        repo.create_parameter(param, "proj", "exp-1")

        obj, name, kwargs = repo._written[0]
        assert obj is param
        assert kwargs["entity_identifier"] == "lr"
        assert kwargs["entity_type"] == "Parameter"


# -------- Tests: Tag convenience methods --------


class TestTagConvenience:
    def test_add_tags(self):
        repo = ConcreteRepository()
        repo.add_tags("proj", ["tag_a", "tag_b"], experiment_id="exp-1", entity_type="Experiment")

        obj, name, kwargs = repo._written[0]
        assert isinstance(obj, TagUpdate)
        assert obj.added_tags == ["tag_a", "tag_b"]
        assert obj.removed_tags == []
        assert kwargs["experiment_id"] == "exp-1"
        assert kwargs["entity_type"] == "Experiment"

    def test_remove_tags(self):
        repo = ConcreteRepository()
        repo.remove_tags("proj", ["tag_a"], experiment_id="exp-1", entity_type="Experiment")

        obj, name, kwargs = repo._written[0]
        assert isinstance(obj, TagUpdate)
        assert obj.removed_tags == ["tag_a"]
        assert obj.added_tags == []

    def test_get_tags(self):
        repo = ConcreteRepository()
        repo._read_domains_return = [
            TagUpdate(added_tags=["tag_a", "tag_b"]),
            TagUpdate(removed_tags=["tag_a"]),
        ]

        result = repo.get_tags("proj", experiment_id="exp-1", entity_type="Experiment")

        assert len(result) == 2
        assert result[0] == {"added_tags": ["tag_a", "tag_b"], "removed_tags": []}
        assert result[1] == {"added_tags": [], "removed_tags": ["tag_a"]}

    def test_get_tags_empty(self):
        repo = ConcreteRepository()
        repo._read_domains_return = []

        result = repo.get_tags("proj", experiment_id="exp-1", entity_type="Experiment")
        assert result == []


# -------- Tests: Comment convenience methods --------


class TestCommentConvenience:
    def test_add_comments(self):
        repo = ConcreteRepository()
        repo.add_comments("proj", ["comment_a"], experiment_id="exp-1", entity_type="Experiment")

        obj, name, kwargs = repo._written[0]
        assert isinstance(obj, CommentUpdate)
        assert obj.added_comments == ["comment_a"]
        assert obj.removed_comments == []

    def test_remove_comments(self):
        repo = ConcreteRepository()
        repo.remove_comments("proj", ["comment_a"], experiment_id="exp-1", entity_type="Experiment")

        obj, name, kwargs = repo._written[0]
        assert isinstance(obj, CommentUpdate)
        assert obj.removed_comments == ["comment_a"]

    def test_get_comments(self):
        repo = ConcreteRepository()
        repo._read_domains_return = [
            CommentUpdate(added_comments=["hello"]),
            CommentUpdate(removed_comments=["hello"]),
        ]

        result = repo.get_comments("proj", experiment_id="exp-1", entity_type="Experiment")

        assert len(result) == 2
        assert result[0] == {"added_comments": ["hello"], "removed_comments": []}
        assert result[1] == {"added_comments": [], "removed_comments": ["hello"]}

    def test_get_comments_empty(self):
        repo = ConcreteRepository()
        repo._read_domains_return = []

        result = repo.get_comments("proj", experiment_id="exp-1", entity_type="Experiment")
        assert result == []
