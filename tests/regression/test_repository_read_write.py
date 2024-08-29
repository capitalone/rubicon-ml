import os
import tempfile
import uuid

import fsspec
import pandas as pd

from rubicon_ml import domain
from rubicon_ml.repository import LocalRepository
from rubicon_ml.repository.utils import json, slugify

ARTIFACT_BINARY = b"artifact"
COMMENTS_TO_ADD = ["comment_a", "comment_b"]
COMMENTS_TO_REMOVE = ["comment_a"]
DATAFRAME = pd.DataFrame([[0]])
TAGS_TO_ADD = ["added_a", "added_b"]
TAGS_TO_REMOVE = ["added_a"]


def test_read_regression(
    artifact_project_json,
    artifact_experiment_json,
    dataframe_project_json,
    dataframe_experiment_json,
    experiment_json,
    feature_json,
    metric_json,
    parameter_json,
    project_json,
):
    """Tests that `rubicon_ml` can read each domain entity from the filesystem."""
    filesystem = fsspec.filesystem("file")

    with tempfile.TemporaryDirectory() as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = LocalRepository(root_dir=root_dir)

        def __test_additional_tags_and_comments(
            tag_comment_dir, project_name, **entity_identification_kwargs
        ):
            add_tag_path = os.path.join(tag_comment_dir, f"tags_{uuid.uuid4()}.json")
            with filesystem.open(add_tag_path, "w") as file:
                file.write(json.dumps({"added_tags": TAGS_TO_ADD}))

            remove_tag_path = os.path.join(tag_comment_dir, f"tags_{uuid.uuid4()}.json")
            with filesystem.open(remove_tag_path, "w") as file:
                file.write(json.dumps({"removed_tags": TAGS_TO_REMOVE}))

            add_comment_path = os.path.join(tag_comment_dir, f"comments_{uuid.uuid4()}.json")
            with filesystem.open(add_comment_path, "w") as file:
                file.write(json.dumps({"added_comments": COMMENTS_TO_ADD}))

            remove_comment_path = os.path.join(tag_comment_dir, f"comments_{uuid.uuid4()}.json")
            with filesystem.open(remove_comment_path, "w") as file:
                file.write(json.dumps({"removed_comments": COMMENTS_TO_REMOVE}))

            additional_tags = repository.get_tags(
                project_name,
                **entity_identification_kwargs,
            )
            additional_comments = repository.get_comments(
                project_name,
                **entity_identification_kwargs,
            )

            return (
                additional_tags[0]["added_tags"] == TAGS_TO_ADD
                and additional_tags[1]["removed_tags"] == TAGS_TO_REMOVE
                and additional_comments[0]["added_comments"] == COMMENTS_TO_ADD
                and additional_comments[1]["removed_comments"] == COMMENTS_TO_REMOVE
            )

        expected_project_dir = os.path.join(root_dir, slugify(project_json["name"]))
        expected_project_path = os.path.join(expected_project_dir, "metadata.json")

        filesystem.mkdirs(expected_project_dir, exist_ok=True)
        with filesystem.open(expected_project_path, "w") as file:
            file.write(json.dumps(project_json))

        project = repository.get_project(project_json["name"]).__dict__

        assert project == project_json

        expected_experiment_dir = os.path.join(
            expected_project_dir,
            "experiments",
            experiment_json["id"],
        )
        expected_experiment_path = os.path.join(expected_experiment_dir, "metadata.json")

        filesystem.mkdirs(expected_experiment_dir, exist_ok=True)
        with filesystem.open(expected_experiment_path, "w") as file:
            file.write(json.dumps(experiment_json))

        experiment = repository.get_experiment(
            project_json["name"],
            experiment_json["id"],
        ).__dict__

        assert experiment == experiment_json
        assert __test_additional_tags_and_comments(
            expected_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=experiment_json["id"],
            entity_type="Experiment",
        )

        expected_feature_dir = os.path.join(
            expected_experiment_dir,
            "features",
            slugify(feature_json["name"]),
        )
        expected_feature_path = os.path.join(expected_feature_dir, "metadata.json")

        filesystem.mkdirs(expected_feature_dir, exist_ok=True)
        with filesystem.open(expected_feature_path, "w") as file:
            file.write(json.dumps(feature_json))

        feature = repository.get_feature(
            project_json["name"],
            experiment_json["id"],
            feature_json["name"],
        ).__dict__

        assert feature == feature_json
        assert __test_additional_tags_and_comments(
            expected_feature_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=feature_json["name"],
            entity_type="Feature",
        )

        expected_metric_dir = os.path.join(
            expected_experiment_dir,
            "metrics",
            slugify(metric_json["name"]),
        )
        expected_metric_path = os.path.join(expected_metric_dir, "metadata.json")

        filesystem.mkdirs(expected_metric_dir, exist_ok=True)
        with filesystem.open(expected_metric_path, "w") as file:
            file.write(json.dumps(metric_json))

        metric = repository.get_metric(
            project_json["name"],
            experiment_json["id"],
            metric_json["name"],
        ).__dict__

        assert metric == metric_json
        assert __test_additional_tags_and_comments(
            expected_metric_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=metric_json["name"],
            entity_type="Metric",
        )

        expected_parameter_dir = os.path.join(
            expected_experiment_dir,
            "parameters",
            slugify(parameter_json["name"]),
        )
        expected_parameter_path = os.path.join(expected_parameter_dir, "metadata.json")

        filesystem.mkdirs(expected_parameter_dir, exist_ok=True)
        with filesystem.open(expected_parameter_path, "w") as file:
            file.write(json.dumps(parameter_json))

        parameter = repository.get_parameter(
            project_json["name"],
            experiment_json["id"],
            parameter_json["name"],
        ).__dict__

        assert parameter == parameter_json
        assert __test_additional_tags_and_comments(
            expected_parameter_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=parameter_json["name"],
            entity_type="Parameter",
        )

        expected_artifact_project_dir = os.path.join(
            expected_project_dir,
            "artifacts",
            artifact_project_json["id"],
        )
        expected_artifact_project_path = os.path.join(
            expected_artifact_project_dir, "metadata.json"
        )
        expected_artifact_project_data_path = os.path.join(expected_artifact_project_dir, "data")

        filesystem.mkdirs(expected_artifact_project_dir, exist_ok=True)
        with filesystem.open(expected_artifact_project_path, "w") as file:
            file.write(json.dumps(artifact_project_json))
        with filesystem.open(expected_artifact_project_data_path, "wb") as file:
            file.write(ARTIFACT_BINARY)

        artifact_project = repository.get_artifact_metadata(
            project_json["name"],
            artifact_project_json["id"],
        ).__dict__
        artifact_project_data = repository.get_artifact_data(
            project_json["name"],
            artifact_project_json["id"],
        )

        assert artifact_project == artifact_project_json
        assert artifact_project_data == ARTIFACT_BINARY
        assert __test_additional_tags_and_comments(
            expected_artifact_project_dir,
            project_json["name"],
            entity_identifier=artifact_project_json["id"],
            entity_type="Artifact",
        )

        expected_artifact_experiment_dir = os.path.join(
            expected_experiment_dir,
            "artifacts",
            artifact_experiment_json["id"],
        )
        expected_artifact_experiment_path = os.path.join(
            expected_artifact_experiment_dir, "metadata.json"
        )
        expected_artifact_experiment_data_path = os.path.join(
            expected_artifact_experiment_dir, "data"
        )

        filesystem.mkdirs(expected_artifact_experiment_dir, exist_ok=True)
        with filesystem.open(expected_artifact_experiment_path, "w") as file:
            file.write(json.dumps(artifact_experiment_json))
        with filesystem.open(expected_artifact_experiment_data_path, "wb") as file:
            file.write(ARTIFACT_BINARY)

        artifact_experiment = repository.get_artifact_metadata(
            project_json["name"],
            artifact_experiment_json["id"],
            experiment_json["id"],
        ).__dict__
        artifact_experiment_data = repository.get_artifact_data(
            project_json["name"],
            artifact_experiment_json["id"],
            experiment_json["id"],
        )

        assert artifact_experiment == artifact_experiment_json
        assert artifact_experiment_data == ARTIFACT_BINARY
        assert __test_additional_tags_and_comments(
            expected_artifact_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=artifact_experiment_json["id"],
            entity_type="Artifact",
        )

        expected_dataframe_project_dir = os.path.join(
            expected_project_dir,
            "dataframes",
            dataframe_project_json["id"],
        )
        expected_dataframe_project_path = os.path.join(
            expected_dataframe_project_dir, "metadata.json"
        )
        expected_dataframe_project_data_dir = os.path.join(expected_dataframe_project_dir, "data")
        expected_dataframe_project_data_path = os.path.join(
            expected_dataframe_project_data_dir, "data.parquet"
        )

        filesystem.mkdirs(expected_dataframe_project_dir, exist_ok=True)
        filesystem.mkdirs(expected_dataframe_project_data_dir, exist_ok=True)
        with filesystem.open(expected_dataframe_project_path, "w") as file:
            file.write(json.dumps(dataframe_project_json))
        DATAFRAME.to_parquet(expected_dataframe_project_data_path)

        dataframe_project = repository.get_dataframe_metadata(
            project_json["name"],
            dataframe_project_json["id"],
        ).__dict__
        dataframe_project_data = repository.get_dataframe_data(
            project_json["name"],
            dataframe_project_json["id"],
        )

        assert dataframe_project == dataframe_project_json
        assert dataframe_project_data.equals(DATAFRAME)
        assert __test_additional_tags_and_comments(
            expected_dataframe_project_dir,
            project_json["name"],
            entity_identifier=dataframe_project_json["id"],
            entity_type="Dataframe",
        )

        expected_dataframe_experiment_dir = os.path.join(
            expected_experiment_dir,
            "dataframes",
            dataframe_experiment_json["id"],
        )
        expected_dataframe_experiment_path = os.path.join(
            expected_dataframe_experiment_dir, "metadata.json"
        )
        expected_dataframe_experiment_data_dir = os.path.join(
            expected_dataframe_experiment_dir, "data"
        )
        expected_dataframe_experiment_data_path = os.path.join(
            expected_dataframe_experiment_data_dir, "data.parquet"
        )

        filesystem.mkdirs(expected_dataframe_experiment_dir, exist_ok=True)
        filesystem.mkdirs(expected_dataframe_experiment_data_dir, exist_ok=True)
        with filesystem.open(expected_dataframe_experiment_path, "w") as file:
            file.write(json.dumps(dataframe_experiment_json))
        DATAFRAME.to_parquet(expected_dataframe_experiment_data_path)

        dataframe_experiment = repository.get_dataframe_metadata(
            project_json["name"],
            dataframe_experiment_json["id"],
            experiment_json["id"],
        ).__dict__
        dataframe_experiment_data = repository.get_dataframe_data(
            project_json["name"],
            dataframe_experiment_json["id"],
            experiment_json["id"],
        )

        assert dataframe_experiment == dataframe_experiment_json
        assert dataframe_experiment_data.equals(DATAFRAME)
        assert __test_additional_tags_and_comments(
            expected_dataframe_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=dataframe_experiment_json["id"],
            entity_type="Dataframe",
        )


def test_read_write_regression(
    artifact_project_json,
    artifact_experiment_json,
    dataframe_project_json,
    dataframe_experiment_json,
    experiment_json,
    feature_json,
    metric_json,
    parameter_json,
    project_json,
):
    """Tests that `rubicon_ml` can read each domain entity that it wrote."""
    with tempfile.TemporaryDirectory() as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = LocalRepository(root_dir=root_dir)

        def __test_additional_tags_and_comments(project_name, **entity_identification_kwargs):
            repository.add_tags(
                project_name,
                TAGS_TO_ADD,
                **entity_identification_kwargs,
            )
            repository.remove_tags(
                project_name,
                TAGS_TO_REMOVE,
                **entity_identification_kwargs,
            )
            additional_tags = repository.get_tags(
                project_name,
                **entity_identification_kwargs,
            )
            repository.add_comments(
                project_name,
                COMMENTS_TO_ADD,
                **entity_identification_kwargs,
            )
            repository.remove_comments(
                project_name,
                COMMENTS_TO_REMOVE,
                **entity_identification_kwargs,
            )
            additional_comments = repository.get_comments(
                project_name,
                **entity_identification_kwargs,
            )

            return (
                additional_tags[0]["added_tags"] == TAGS_TO_ADD
                and additional_tags[1]["removed_tags"] == TAGS_TO_REMOVE
                and additional_comments[0]["added_comments"] == COMMENTS_TO_ADD
                and additional_comments[1]["removed_comments"] == COMMENTS_TO_REMOVE
            )

        repository.create_project(domain.Project(**project_json))
        project = repository.get_project(project_json["name"]).__dict__

        assert project == project_json

        repository.create_experiment(domain.Experiment(**experiment_json))
        experiment = repository.get_experiment(
            project_json["name"],
            experiment_json["id"],
        ).__dict__

        assert experiment == experiment_json
        assert __test_additional_tags_and_comments(
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=experiment_json["id"],
            entity_type="Experiment",
        )

        repository.create_feature(
            domain.Feature(**feature_json),
            project_json["name"],
            experiment_json["id"],
        )
        feature = repository.get_feature(
            project_json["name"],
            experiment_json["id"],
            feature_json["name"],
        ).__dict__

        assert feature == feature_json
        assert __test_additional_tags_and_comments(
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=feature_json["name"],
            entity_type="Feature",
        )

        repository.create_metric(
            domain.Metric(**metric_json),
            project_json["name"],
            experiment_json["id"],
        )
        metric = repository.get_metric(
            project_json["name"],
            experiment_json["id"],
            metric_json["name"],
        ).__dict__

        assert metric == metric_json
        assert __test_additional_tags_and_comments(
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=metric_json["name"],
            entity_type="Metric",
        )

        repository.create_parameter(
            domain.Parameter(**parameter_json),
            project_json["name"],
            experiment_json["id"],
        )
        parameter = repository.get_parameter(
            project_json["name"],
            experiment_json["id"],
            parameter_json["name"],
        ).__dict__

        assert parameter == parameter_json
        assert __test_additional_tags_and_comments(
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=parameter_json["name"],
            entity_type="Parameter",
        )

        repository.create_artifact(
            domain.Artifact(**artifact_project_json),
            ARTIFACT_BINARY,
            project_json["name"],
        )
        artifact_project = repository.get_artifact_metadata(
            project_json["name"],
            artifact_project_json["id"],
        ).__dict__
        artifact_project_data = repository.get_artifact_data(
            project_json["name"],
            artifact_project_json["id"],
        )

        assert artifact_project == artifact_project_json
        assert artifact_project_data == ARTIFACT_BINARY
        assert __test_additional_tags_and_comments(
            project_json["name"],
            entity_identifier=artifact_project_json["id"],
            entity_type="Artifact",
        )

        repository.create_artifact(
            domain.Artifact(**artifact_experiment_json),
            ARTIFACT_BINARY,
            project_json["name"],
            experiment_json["id"],
        )
        artifact_experiment = repository.get_artifact_metadata(
            project_json["name"],
            artifact_experiment_json["id"],
            experiment_json["id"],
        ).__dict__
        artifact_experiment_data = repository.get_artifact_data(
            project_json["name"],
            artifact_experiment_json["id"],
            experiment_json["id"],
        )

        assert artifact_experiment == artifact_experiment_json
        assert artifact_experiment_data == ARTIFACT_BINARY
        assert __test_additional_tags_and_comments(
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=artifact_experiment_json["id"],
            entity_type="Artifact",
        )

        repository.create_dataframe(
            domain.Dataframe(**dataframe_project_json),
            DATAFRAME,
            project_json["name"],
        )
        dataframe_project = repository.get_dataframe_metadata(
            project_json["name"],
            dataframe_project_json["id"],
        ).__dict__
        dataframe_project_data = repository.get_dataframe_data(
            project_json["name"],
            dataframe_project_json["id"],
        )

        assert dataframe_project == dataframe_project_json
        assert dataframe_project_data.equals(DATAFRAME)
        assert __test_additional_tags_and_comments(
            project_json["name"],
            entity_identifier=dataframe_project_json["id"],
            entity_type="Dataframe",
        )

        repository.create_dataframe(
            domain.Dataframe(**dataframe_experiment_json),
            DATAFRAME,
            project_json["name"],
            experiment_json["id"],
        )
        dataframe_experiment = repository.get_dataframe_metadata(
            project_json["name"],
            dataframe_experiment_json["id"],
            experiment_json["id"],
        ).__dict__
        dataframe_experiment_data = repository.get_dataframe_data(
            project_json["name"],
            dataframe_experiment_json["id"],
            experiment_json["id"],
        )

        assert dataframe_experiment == dataframe_experiment_json
        assert dataframe_experiment_data.equals(DATAFRAME)
        assert __test_additional_tags_and_comments(
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=dataframe_experiment_json["id"],
            entity_type="Dataframe",
        )


def test_write_regression(
    artifact_project_json,
    artifact_experiment_json,
    dataframe_project_json,
    dataframe_experiment_json,
    experiment_json,
    feature_json,
    metric_json,
    parameter_json,
    project_json,
):
    """Tests that `rubicon_ml` can write each domain entity to the filesystem."""
    filesystem = fsspec.filesystem("file")

    with tempfile.TemporaryDirectory() as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = LocalRepository(root_dir=root_dir)

        def __test_additional_tags_and_comments(
            tag_dir, project_name, **entity_identification_kwargs
        ):
            is_passing = True

            repository.add_tags(
                project_name,
                TAGS_TO_ADD,
                **entity_identification_kwargs,
            )
            repository.remove_tags(
                project_name,
                TAGS_TO_REMOVE,
                **entity_identification_kwargs,
            )

            tag_path = os.path.join(tag_dir, "tags_*.json")
            tag_files = filesystem.glob(tag_path, detail=True)
            for tag_file in tag_files:
                with filesystem.open(tag_file, "r") as file:
                    tags = json.loads(file.read())

                    if "added_tags" in tags:
                        is_passing &= tags["added_tags"] == TAGS_TO_ADD
                    if "removed_tags" in tags:
                        is_passing &= tags["removed_tags"] == TAGS_TO_REMOVE

            repository.add_comments(
                project_name,
                COMMENTS_TO_ADD,
                **entity_identification_kwargs,
            )
            repository.remove_comments(
                project_name,
                COMMENTS_TO_REMOVE,
                **entity_identification_kwargs,
            )

            comment_path = os.path.join(tag_dir, "comments_*.json")
            comment_files = filesystem.glob(comment_path, detail=True)
            for comment_file in comment_files:
                with filesystem.open(comment_file, "r") as file:
                    comments = json.loads(file.read())

                    if "added_comments" in comments:
                        is_passing &= comments["added_comments"] == COMMENTS_TO_ADD
                    if "removed_tags" in comments:
                        is_passing &= comments["removed_comments"] == COMMENTS_TO_REMOVE

            return is_passing

        repository.create_project(domain.Project(**project_json))

        expected_project_dir = os.path.join(root_dir, slugify(project_json["name"]))
        expected_project_path = os.path.join(expected_project_dir, "metadata.json")

        with filesystem.open(expected_project_path, "r") as file:
            project = json.loads(file.read())

        assert project == project_json

        repository.create_experiment(domain.Experiment(**experiment_json))

        expected_experiment_dir = os.path.join(
            expected_project_dir,
            "experiments",
            experiment_json["id"],
        )
        expected_experiment_path = os.path.join(expected_experiment_dir, "metadata.json")

        with filesystem.open(expected_experiment_path, "r") as file:
            experiment = json.loads(file.read())

        assert experiment == experiment_json
        assert __test_additional_tags_and_comments(
            expected_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=experiment_json["id"],
            entity_type="Experiment",
        )

        repository.create_feature(
            domain.Feature(**feature_json),
            project_json["name"],
            experiment_json["id"],
        )

        expected_feature_dir = os.path.join(
            expected_experiment_dir,
            "features",
            slugify(feature_json["name"]),
        )
        expected_feature_path = os.path.join(expected_feature_dir, "metadata.json")

        with filesystem.open(expected_feature_path, "r") as file:
            feature = json.loads(file.read())

        assert feature == feature_json
        assert __test_additional_tags_and_comments(
            expected_feature_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=feature_json["name"],
            entity_type="Feature",
        )

        repository.create_metric(
            domain.Metric(**metric_json),
            project_json["name"],
            experiment_json["id"],
        )

        expected_metric_dir = os.path.join(
            expected_experiment_dir,
            "metrics",
            slugify(metric_json["name"]),
        )
        expected_metric_path = os.path.join(expected_metric_dir, "metadata.json")

        with filesystem.open(expected_metric_path, "r") as file:
            metric = json.loads(file.read())

        assert metric == metric_json
        assert __test_additional_tags_and_comments(
            expected_metric_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=metric_json["name"],
            entity_type="Metric",
        )

        repository.create_parameter(
            domain.Parameter(**parameter_json),
            project_json["name"],
            experiment_json["id"],
        )

        expected_parameter_dir = os.path.join(
            expected_experiment_dir,
            "parameters",
            slugify(parameter_json["name"]),
        )
        expected_parameter_path = os.path.join(expected_parameter_dir, "metadata.json")

        with filesystem.open(expected_parameter_path, "r") as file:
            parameter = json.loads(file.read())

        assert parameter == parameter_json
        assert __test_additional_tags_and_comments(
            expected_parameter_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=parameter_json["name"],
            entity_type="Parameter",
        )

        repository.create_artifact(
            domain.Artifact(**artifact_project_json),
            ARTIFACT_BINARY,
            project_json["name"],
        )

        expected_artifact_project_dir = os.path.join(
            expected_project_dir,
            "artifacts",
            artifact_project_json["id"],
        )
        expected_artifact_project_path = os.path.join(
            expected_artifact_project_dir, "metadata.json"
        )
        expected_artifact_project_data_path = os.path.join(expected_artifact_project_dir, "data")

        with filesystem.open(expected_artifact_project_path, "r") as file:
            artifact_project = json.loads(file.read())
        with filesystem.open(expected_artifact_project_data_path, "rb") as file:
            artifact_project_data = file.read()

        assert artifact_project == artifact_project_json
        assert artifact_project_data == ARTIFACT_BINARY
        assert __test_additional_tags_and_comments(
            expected_artifact_project_dir,
            project_json["name"],
            entity_identifier=artifact_project_json["id"],
            entity_type="Artifact",
        )

        repository.create_artifact(
            domain.Artifact(**artifact_experiment_json),
            ARTIFACT_BINARY,
            project_json["name"],
            experiment_json["id"],
        )

        expected_artifact_experiment_dir = os.path.join(
            expected_experiment_dir,
            "artifacts",
            artifact_experiment_json["id"],
        )
        expected_artifact_experiment_path = os.path.join(
            expected_artifact_experiment_dir, "metadata.json"
        )
        expected_artifact_experiment_data_path = os.path.join(
            expected_artifact_experiment_dir, "data"
        )

        with filesystem.open(expected_artifact_experiment_path, "r") as file:
            artifact_experiment = json.loads(file.read())
        with filesystem.open(expected_artifact_experiment_data_path, "rb") as file:
            artifact_experiment_data = file.read()

        assert artifact_experiment == artifact_experiment_json
        assert artifact_experiment_data == ARTIFACT_BINARY
        assert __test_additional_tags_and_comments(
            expected_artifact_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=artifact_experiment_json["id"],
            entity_type="Artifact",
        )

        repository.create_dataframe(
            domain.Dataframe(**dataframe_project_json),
            DATAFRAME,
            project_json["name"],
        )

        expected_dataframe_project_dir = os.path.join(
            expected_project_dir,
            "dataframes",
            dataframe_project_json["id"],
        )
        expected_dataframe_project_path = os.path.join(
            expected_dataframe_project_dir, "metadata.json"
        )
        expected_dataframe_project_data_path = os.path.join(
            expected_dataframe_project_dir, "data", "data.parquet"
        )

        with filesystem.open(expected_dataframe_project_path, "r") as file:
            dataframe_project = json.loads(file.read())
        dataframe_project_data = pd.read_parquet(expected_dataframe_project_data_path)

        assert dataframe_project == dataframe_project_json
        assert dataframe_project_data.equals(DATAFRAME)
        assert __test_additional_tags_and_comments(
            expected_dataframe_project_dir,
            project_json["name"],
            entity_identifier=dataframe_project_json["id"],
            entity_type="Dataframe",
        )

        repository.create_dataframe(
            domain.Dataframe(**dataframe_experiment_json),
            DATAFRAME,
            project_json["name"],
            experiment_json["id"],
        )

        expected_dataframe_experiment_dir = os.path.join(
            expected_experiment_dir,
            "dataframes",
            dataframe_experiment_json["id"],
        )
        expected_dataframe_experiment_path = os.path.join(
            expected_dataframe_experiment_dir, "metadata.json"
        )
        expected_dataframe_experiment_data_path = os.path.join(
            expected_dataframe_experiment_dir, "data", "data.parquet"
        )

        with filesystem.open(expected_dataframe_experiment_path, "r") as file:
            dataframe_experiment = json.loads(file.read())
        dataframe_experiment_data = pd.read_parquet(expected_dataframe_experiment_data_path)

        assert dataframe_experiment == dataframe_experiment_json
        assert dataframe_experiment_data.equals(DATAFRAME)
        assert __test_additional_tags_and_comments(
            expected_dataframe_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=dataframe_experiment_json["id"],
            entity_type="Dataframe",
        )


def test_delete_regression(
    artifact_project_json,
    artifact_experiment_json,
    dataframe_project_json,
    dataframe_experiment_json,
    experiment_json,
    project_json,
):
    """Tests that `rubicon_ml` can delete artifacts and dataframes from the filesystem."""
    filesystem = fsspec.filesystem("file")

    with tempfile.TemporaryDirectory() as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = LocalRepository(root_dir=root_dir)

        repository.create_artifact(
            domain.Artifact(**artifact_project_json),
            ARTIFACT_BINARY,
            project_json["name"],
        )

        expected_project_dir = os.path.join(root_dir, slugify(project_json["name"]))
        expected_artifact_project_path = os.path.join(
            expected_project_dir,
            "artifacts",
            artifact_project_json["id"],
            "metadata.json",
        )

        assert filesystem.exists(expected_artifact_project_path)

        repository.delete_artifact(
            project_json["name"],
            artifact_project_json["id"],
        )

        assert not filesystem.exists(expected_artifact_project_path)

        repository.create_artifact(
            domain.Artifact(**artifact_experiment_json),
            ARTIFACT_BINARY,
            project_json["name"],
            experiment_json["id"],
        )

        expected_experiment_dir = os.path.join(
            expected_project_dir,
            "experiments",
            experiment_json["id"],
        )
        expected_artifact_experiment_path = os.path.join(
            expected_experiment_dir,
            "artifacts",
            artifact_experiment_json["id"],
            "metadata.json",
        )

        assert filesystem.exists(expected_artifact_experiment_path)

        repository.delete_artifact(
            project_json["name"],
            artifact_experiment_json["id"],
            experiment_json["id"],
        )

        assert not filesystem.exists(expected_artifact_experiment_path)

        repository.create_dataframe(
            domain.Dataframe(**dataframe_project_json),
            DATAFRAME,
            project_json["name"],
        )

        expected_dataframe_project_path = os.path.join(
            expected_project_dir,
            "dataframes",
            dataframe_project_json["id"],
            "metadata.json",
        )

        assert filesystem.exists(expected_dataframe_project_path)

        repository.delete_dataframe(
            project_json["name"],
            dataframe_project_json["id"],
        )

        assert not filesystem.exists(expected_dataframe_project_path)

        repository.create_dataframe(
            domain.Dataframe(**dataframe_experiment_json),
            DATAFRAME,
            project_json["name"],
            experiment_json["id"],
        )

        expected_dataframe_experiment_path = os.path.join(
            expected_experiment_dir,
            "dataframes",
            dataframe_experiment_json["id"],
            "metadata.json",
        )

        assert filesystem.exists(expected_dataframe_experiment_path)

        repository.delete_dataframe(
            project_json["name"],
            dataframe_experiment_json["id"],
            experiment_json["id"],
        )

        assert not filesystem.exists(expected_dataframe_experiment_path)
