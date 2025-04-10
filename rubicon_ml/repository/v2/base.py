from abc import ABC, abstractmethod


class BaseRepository(ABC):
    """Abstract base for rubicon-ml backend repositories."""

    # core read/writes

    @abstractmethod
    def read_domain(self, *args, **kwargs):
        ...

    @abstractmethod
    def read_domains(self, *args, **kwargs):
        ...

    @abstractmethod
    def write_domain(self, *args, **kwargs):
        ...

    # binary read/writes

    @abstractmethod
    def read_artifact_data(self, *args, **kwargs):
        ...

    @abstractmethod
    def write_artifact_data(self, *args, **kwargs):
        ...

    @abstractmethod
    def read_dataframe_data(self, *args, **kwargs):
        ...

    @abstractmethod
    def write_dataframe_data(self, *args, **kwargs):
        ...

    # domain read/writes

    def read_project_metadata(self, *args, **kwargs):
        project = self.read_domain(*args, **kwargs)

        return project

    def read_projects_metadata(self, *args, **kwargs):
        projects = self.read_domains(*args, **kwargs)

        return projects

    def write_project_metadata(self, *args, **kwargs):
        self.write_domain(*args, **kwargs)

    def read_experiment_metadata(self, *args, **kwargs):
        experiment = self.read_domain(*args, **kwargs)

        return experiment

    def read_experiments_metadata(self, *args, **kwargs):
        experiments = self.read_domains(*args, **kwargs)

        return experiments

    def write_experiment_metadata(self, *args, **kwargs):
        self.write_domain(*args, **kwargs)

    # TODO: features, metrics, parameters, artifacts, dataframes


class BaseRepositoryV2(BaseRepository):
    """`BaseRepository` alias for testing and integration."""

    pass
