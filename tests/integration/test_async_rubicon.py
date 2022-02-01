import asyncio
import uuid

import pandas as pd
import pytest

from rubicon_ml.client.asynchronous import Rubicon

filesystems = [
    pytest.param(
        Rubicon(persistence="filesystem", root_dir="s3://change-me"),
        marks=pytest.mark.write_files,
    ),
]


@pytest.mark.parametrize("rubicon", filesystems)
def test_rubicon(rubicon, request):
    async def _test_rubicon(rubicon):
        written_project = await rubicon.create_project(name=f"Test Project {uuid.uuid4()}")
        written_experiment = await written_project.log_experiment(
            name=f"Test Experiment {uuid.uuid4()}"
        )

        await written_experiment.add_tags(["x", "y"])
        await written_experiment.remove_tags(["x"])

        (
            written_feature,
            written_parameter,
            written_metric,
            written_project_artifact,
            written_experiment_artifact,
            written_project_dataframe,
            written_experiment_dataframe,
        ) = await asyncio.gather(
            written_experiment.log_feature(name=f"Test Feature {uuid.uuid4()}"),
            written_experiment.log_parameter(name=f"Test Parameter {uuid.uuid4()}", value=8),
            written_experiment.log_metric(name=f"Test Feature {uuid.uuid4()}", value=24),
            written_project.log_artifact(
                name=f"Test Artifact {uuid.uuid4()}", data_bytes=b"test artifact data"
            ),
            written_experiment.log_artifact(
                name=f"Test Artifact {uuid.uuid4()}", data_bytes=b"test artifact data"
            ),
            written_project.log_dataframe(df=pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])),
            written_experiment.log_dataframe(df=pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])),
        )

        await asyncio.gather(
            written_project_dataframe.add_tags(["x", "y"]),
            written_experiment_dataframe.add_tags(["x", "y"]),
        )

        await asyncio.gather(
            written_project_dataframe.remove_tags(["x"]),
            written_experiment_dataframe.remove_tags(["x"]),
        )

        read_project = await rubicon.get_project(name=written_project.name)
        assert written_project.id == read_project.id

        read_experiments = await read_project.experiments()
        assert len(read_experiments) == 1
        assert written_experiment.id == read_experiments[0].id

        read_experiment = read_experiments[0]
        assert await written_experiment.tags == await read_experiment.tags

        (
            read_features,
            read_parameters,
            read_metrics,
            read_project_artifacts,
            read_experiment_artifacts,
            read_project_dataframes,
            read_experiment_dataframes,
        ) = await asyncio.gather(
            read_experiment.features(),
            read_experiment.parameters(),
            read_experiment.metrics(),
            read_project.artifacts(),
            read_experiment.artifacts(),
            read_project.dataframes(),
            read_experiment.dataframes(),
        )

        assert len(read_features) == 1
        assert written_feature.id == read_features[0].id

        assert len(read_parameters) == 1
        assert written_parameter.id == read_parameters[0].id
        assert written_parameter.value == read_parameters[0].value

        assert len(read_metrics) == 1
        assert written_metric.id == read_metrics[0].id
        assert written_metric.value == read_metrics[0].value

        assert len(read_project_artifacts) == 1
        assert written_project_artifact.id == read_project_artifacts[0].id
        assert await written_project_artifact.data == await read_project_artifacts[0].data

        await read_project.delete_artifacts([read_project_artifacts[0].id])
        assert len(await read_project.artifacts()) == 0

        assert len(read_experiment_artifacts) == 1
        assert written_experiment_artifact.id == read_experiment_artifacts[0].id
        assert await written_experiment_artifact.data == await read_experiment_artifacts[0].data

        assert len(read_project_dataframes) == 1
        assert written_project_dataframe.id == read_project_dataframes[0].id
        assert written_project_dataframe.get_data().equals(read_project_dataframes[0].get_data())
        assert await written_project_dataframe.tags == await read_project_dataframes[0].tags

        await read_project.delete_dataframes([read_project_dataframes[0].id])
        assert len(await read_project.dataframes()) == 0

        assert len(read_experiment_dataframes) == 1
        assert written_experiment_dataframe.id == read_experiment_dataframes[0].id
        assert written_experiment_dataframe.get_data().equals(
            read_experiment_dataframes[0].get_data()
        )
        assert await written_experiment_dataframe.tags == await read_experiment_dataframes[0].tags

        await rubicon.repository.filesystem._rm(rubicon.repository.root_dir, recursive=True)
        await rubicon.repository.filesystem._s3.close()

    if "change-me" in rubicon.repository.root_dir:
        root_dir = request.config.getoption("s3-path")

        if root_dir is None:
            pytest.fail("`root_dir` cannot be None. Run `pytest` with `--s3-path`.")

        rubicon.repository.root_dir = root_dir

    loop = asyncio.get_event_loop()
    loop.run_until_complete(_test_rubicon(rubicon))
