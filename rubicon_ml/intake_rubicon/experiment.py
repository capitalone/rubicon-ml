from rubicon_ml import __version__
from rubicon_ml.client import Rubicon
from rubicon_ml.intake_rubicon.base import DataSourceMixin


class ExperimentSource(DataSourceMixin):
    """An Intake data source for reading `rubicon` experiments.

    Parameters
    ----------
    urlpath : str
        The root directory the `rubicon` data is logged to.
    project_name : str
        The name of the `rubicon` project to load.
    experiment_id : str
        The ID of the `rubicon` experiment to load.
    """

    version = __version__

    container = "python"
    name = "rubicon_ml_experiment"

    def __init__(
        self, urlpath, project_name, experiment_id, metadata=None, storage_options=None, **kwargs
    ):
        self._experiment_id = experiment_id

        super().__init__(
            urlpath, project_name, metadata=metadata, storage_options=storage_options, **kwargs
        )

    def _get_schema(self):
        """Load the experiment with ID `self._experiment_id`
        from the project named `self._project_name`.
        """
        self._rubicon = Rubicon(persistence="filesystem", root_dir=self._urlpath)
        self._rubicon_object = self._rubicon.get_project(self._project_name).experiment(
            self._experiment_id
        )

        self._metadata.update(
            {
                "experiment": {
                    "name": self._rubicon_object.name,
                    "id": self._rubicon_object.id,
                    "created_id": self._rubicon_object.created_at,
                    "commit_hash": self._rubicon_object.commit_hash,
                    "tags": self._rubicon_object.tags,
                    "project": self._rubicon_object.project,
                }
            }
        )

        return super()._get_schema()
