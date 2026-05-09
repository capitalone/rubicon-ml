"""Tests for the RepositoryBase abstract base class."""

import pandas as pd
import pytest

from rubicon_ml import domain
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
