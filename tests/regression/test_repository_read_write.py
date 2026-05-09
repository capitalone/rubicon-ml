import contextlib
import os
import tempfile
import uuid

import pandas as pd
import pytest

from rubicon_ml import domain
from rubicon_ml.domain.comment_update import CommentUpdate
from rubicon_ml.domain.tag_update import TagUpdate
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import LocalRepository, MemoryRepository, WandBRepository
from rubicon_ml.repository.utils import json, slugify

ARTIFACT_BINARY = b"artifact"
COMMENTS_TO_ADD = ["added comment a", "added comment b"]
COMMENTS_TO_REMOVE = ["added comment a"]
DATAFRAME = pd.DataFrame([[0]], columns=["column_a"])
REPOSITORIES_TO_TEST = [  # TODO: find local/CI S3 testing solution
    pytest.param(LocalRepository),
    pytest.param(MemoryRepository),
]
REPOSITORIES_TO_READ_WRITE_TEST = [
    *REPOSITORIES_TO_TEST,
    pytest.param(WandBRepository, marks=pytest.mark.wandb),
]
TAGS_TO_ADD = ["added_tag_a", "added_tag_b"]
TAGS_TO_REMOVE = ["added_tag_a"]


def _test_read_additional_tags_and_comments(
    repository, tag_comment_dir, project_name, **entity_identification_kwargs
):
    is_passing = True

    add_tag_path = os.path.join(tag_comment_dir, f"tags_{uuid.uuid4()}.json")
    with repository.filesystem.open(add_tag_path, "w") as file:
        file.write(json.dumps({"added_tags": TAGS_TO_ADD}))

    remove_tag_path = os.path.join(tag_comment_dir, f"tags_{uuid.uuid4()}.json")
    with repository.filesystem.open(remove_tag_path, "w") as file:
        file.write(json.dumps({"removed_tags": TAGS_TO_REMOVE}))

    additional_tags = repository.read_domains(
        TagUpdate,
        project_name,
        **entity_identification_kwargs,
    )
    for tag_update in additional_tags:
        if tag_update.added_tags:
            is_passing &= tag_update.added_tags == TAGS_TO_ADD
        if tag_update.removed_tags:
            is_passing &= tag_update.removed_tags == TAGS_TO_REMOVE

    add_comment_path = os.path.join(tag_comment_dir, f"comments_{uuid.uuid4()}.json")
    with repository.filesystem.open(add_comment_path, "w") as file:
        file.write(json.dumps({"added_comments": COMMENTS_TO_ADD}))

    remove_comment_path = os.path.join(tag_comment_dir, f"comments_{uuid.uuid4()}.json")
    with repository.filesystem.open(remove_comment_path, "w") as file:
        file.write(json.dumps({"removed_comments": COMMENTS_TO_REMOVE}))

    additional_comments = repository.read_domains(
        CommentUpdate,
        project_name,
        **entity_identification_kwargs,
    )
    for comment_update in additional_comments:
        if comment_update.added_comments:
            is_passing &= comment_update.added_comments == COMMENTS_TO_ADD
        if comment_update.removed_comments:
            is_passing &= comment_update.removed_comments == COMMENTS_TO_REMOVE

    return is_passing


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_project_regression(project_json, repository_class):
    """Tests that `rubicon_ml` can read the project domain entity from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_read_project_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_project_dir = os.path.join(root_dir, slugify(project_json["name"]))
        expected_project_path = os.path.join(
            root_dir, slugify(project_json["name"]), "metadata.json"
        )

        repository.filesystem.mkdirs(expected_project_dir, exist_ok=True)
        with repository.filesystem.open(expected_project_path, "w") as file:
            file.write(json.dumps(project_json))

        project = repository.get_project(project_json["name"]).__dict__

        assert project == project_json


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_experiment_regression(experiment_json, project_json, repository_class):
    """Tests that `rubicon_ml` can read the experiment domain entity from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_read_experiment_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_experiment_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
        )
        expected_experiment_path = os.path.join(expected_experiment_dir, "metadata.json")

        repository.filesystem.mkdirs(expected_experiment_dir, exist_ok=True)
        with repository.filesystem.open(expected_experiment_path, "w") as file:
            file.write(json.dumps(experiment_json))

        experiment = repository.read_domain(
            domain.Experiment,
            project_json["name"],
            experiment_id=experiment_json["id"],
        ).__dict__

        assert experiment == experiment_json
        assert _test_read_additional_tags_and_comments(
            repository,
            expected_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=experiment_json["id"],
            entity_type="Experiment",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_feature_regression(
    experiment_json,
    feature_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the feature domain entity from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_read_feature_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_feature_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "features",
            slugify(feature_json["name"]),
        )
        expected_feature_path = os.path.join(expected_feature_dir, "metadata.json")

        repository.filesystem.mkdirs(expected_feature_dir, exist_ok=True)
        with repository.filesystem.open(expected_feature_path, "w") as file:
            file.write(json.dumps(feature_json))

        feature = repository.read_domain(
            domain.Feature,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=feature_json["name"],
            entity_type="Feature",
        ).__dict__

        assert feature == feature_json
        assert _test_read_additional_tags_and_comments(
            repository,
            expected_feature_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=feature_json["name"],
            entity_type="Feature",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_metric_regression(
    experiment_json,
    metric_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the metric domain entity from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_read_metric_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_metric_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "metrics",
            slugify(metric_json["name"]),
        )
        expected_metric_path = os.path.join(expected_metric_dir, "metadata.json")

        repository.filesystem.mkdirs(expected_metric_dir, exist_ok=True)
        with repository.filesystem.open(expected_metric_path, "w") as file:
            file.write(json.dumps(metric_json))

        metric = repository.read_domain(
            domain.Metric,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=metric_json["name"],
            entity_type="Metric",
        ).__dict__

        assert metric == metric_json
        assert _test_read_additional_tags_and_comments(
            repository,
            expected_metric_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=metric_json["name"],
            entity_type="Metric",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_parameter_regression(
    experiment_json,
    parameter_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the parameter domain entity from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_read_parameter_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_parameter_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "parameters",
            slugify(parameter_json["name"]),
        )
        expected_parameter_path = os.path.join(expected_parameter_dir, "metadata.json")

        repository.filesystem.mkdirs(expected_parameter_dir, exist_ok=True)
        with repository.filesystem.open(expected_parameter_path, "w") as file:
            file.write(json.dumps(parameter_json))

        parameter = repository.read_domain(
            domain.Parameter,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=parameter_json["name"],
            entity_type="Parameter",
        ).__dict__

        assert parameter == parameter_json
        assert _test_read_additional_tags_and_comments(
            repository,
            expected_parameter_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=parameter_json["name"],
            entity_type="Parameter",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_artifact_project_regression(
    artifact_project_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the artifact (project) domain entity from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_artifact_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_artifact_project_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "artifacts",
            artifact_project_json["id"],
        )
        expected_artifact_project_path = os.path.join(
            expected_artifact_project_dir, "metadata.json"
        )
        expected_artifact_project_data_path = os.path.join(expected_artifact_project_dir, "data")

        repository.filesystem.mkdirs(expected_artifact_project_dir, exist_ok=True)
        with repository.filesystem.open(expected_artifact_project_path, "w") as file:
            file.write(json.dumps(artifact_project_json))
        with repository.filesystem.open(expected_artifact_project_data_path, "wb") as file:
            file.write(ARTIFACT_BINARY)

        artifact_project = repository.read_domain(
            domain.Artifact,
            project_json["name"],
            entity_identifier=artifact_project_json["id"],
            entity_type="Artifact",
        ).__dict__
        artifact_project_data = repository.read_artifact_data(
            project_json["name"],
            artifact_project_json["id"],
        )

        assert artifact_project == artifact_project_json
        assert artifact_project_data == ARTIFACT_BINARY
        assert _test_read_additional_tags_and_comments(
            repository,
            expected_artifact_project_dir,
            project_json["name"],
            entity_identifier=artifact_project_json["id"],
            entity_type="Artifact",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_artifact_experiment_regression(
    artifact_experiment_json,
    experiment_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the artifact (experiment) domain entity from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_artifact_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_artifact_experiment_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "artifacts",
            artifact_experiment_json["id"],
        )
        expected_artifact_experiment_path = os.path.join(
            expected_artifact_experiment_dir, "metadata.json"
        )
        expected_artifact_experiment_data_path = os.path.join(
            expected_artifact_experiment_dir, "data"
        )

        repository.filesystem.mkdirs(expected_artifact_experiment_dir, exist_ok=True)
        with repository.filesystem.open(expected_artifact_experiment_path, "w") as file:
            file.write(json.dumps(artifact_experiment_json))
        with repository.filesystem.open(expected_artifact_experiment_data_path, "wb") as file:
            file.write(ARTIFACT_BINARY)

        artifact_experiment = repository.read_domain(
            domain.Artifact,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=artifact_experiment_json["id"],
            entity_type="Artifact",
        ).__dict__
        artifact_experiment_data = repository.read_artifact_data(
            project_json["name"],
            artifact_experiment_json["id"],
            experiment_id=experiment_json["id"],
        )

        assert artifact_experiment == artifact_experiment_json
        assert artifact_experiment_data == ARTIFACT_BINARY
        assert _test_read_additional_tags_and_comments(
            repository,
            expected_artifact_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=artifact_experiment_json["id"],
            entity_type="Artifact",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_dataframe_project_regression(
    dataframe_project_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the datafrane (project) domain entity from the filesystem.

    The `MemoryRepository` skips dataframe data as the `pandas` API can not be used to write directly
    to memory. Dataframe data regression tests are covered by `test_read_write_regression`.
    """
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_dataframe_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_dataframe_project_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "dataframes",
            dataframe_project_json["id"],
        )
        expected_dataframe_project_path = os.path.join(
            expected_dataframe_project_dir, "metadata.json"
        )

        repository.filesystem.mkdirs(expected_dataframe_project_dir, exist_ok=True)
        with repository.filesystem.open(expected_dataframe_project_path, "w") as file:
            file.write(json.dumps(dataframe_project_json))

        dataframe_project = repository.read_domain(
            domain.Dataframe,
            project_json["name"],
            entity_identifier=dataframe_project_json["id"],
            entity_type="Dataframe",
        ).__dict__

        assert dataframe_project == dataframe_project_json
        assert _test_read_additional_tags_and_comments(
            repository,
            expected_dataframe_project_dir,
            project_json["name"],
            entity_identifier=dataframe_project_json["id"],
            entity_type="Dataframe",
        )

        if repository_class != MemoryRepository:
            expected_dataframe_project_data_dir = os.path.join(
                expected_dataframe_project_dir, "data"
            )
            expected_dataframe_project_data_path = os.path.join(
                expected_dataframe_project_data_dir, "data.parquet"
            )

            repository.filesystem.mkdirs(expected_dataframe_project_data_dir, exist_ok=True)
            DATAFRAME.to_parquet(expected_dataframe_project_data_path)

            dataframe_project_data = repository.read_dataframe_data(
                project_json["name"],
                dataframe_project_json["id"],
            )

            assert dataframe_project_data.equals(DATAFRAME)


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_read_dataframe_experiment_regression(
    dataframe_experiment_json,
    experiment_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the datafrane (experiment) domain entity from the filesystem.

    The `MemoryRepository` skips dataframe data as the `pandas` API can not be used to write directly
    to memory. Dataframe data regression tests are covered by `test_read_write_regression`.
    """
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_dataframe_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        expected_dataframe_experiment_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "dataframes",
            dataframe_experiment_json["id"],
        )
        expected_dataframe_experiment_path = os.path.join(
            expected_dataframe_experiment_dir, "metadata.json"
        )

        repository.filesystem.mkdirs(expected_dataframe_experiment_dir, exist_ok=True)
        with repository.filesystem.open(expected_dataframe_experiment_path, "w") as file:
            file.write(json.dumps(dataframe_experiment_json))

        dataframe_experiment = repository.read_domain(
            domain.Dataframe,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=dataframe_experiment_json["id"],
            entity_type="Dataframe",
        ).__dict__

        assert dataframe_experiment == dataframe_experiment_json
        assert _test_read_additional_tags_and_comments(
            repository,
            expected_dataframe_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=dataframe_experiment_json["id"],
            entity_type="Dataframe",
        )

        if repository_class != MemoryRepository:
            expected_dataframe_experiment_data_dir = os.path.join(
                expected_dataframe_experiment_dir, "data"
            )
            expected_dataframe_experiment_data_path = os.path.join(
                expected_dataframe_experiment_data_dir, "data.parquet"
            )

            repository.filesystem.mkdirs(expected_dataframe_experiment_data_dir, exist_ok=True)
            DATAFRAME.to_parquet(expected_dataframe_experiment_data_path)

            dataframe_experiment_data = repository.read_dataframe_data(
                project_json["name"],
                dataframe_experiment_json["id"],
                experiment_id=experiment_json["id"],
            )

            assert dataframe_experiment_data.equals(DATAFRAME)


def _write_additional_tags_and_comments(repository, project_name, **entity_identification_kwargs):
    repository.write_domain(
        TagUpdate(added_tags=TAGS_TO_ADD),
        project_name,
        **entity_identification_kwargs,
    )
    repository.write_domain(
        TagUpdate(removed_tags=TAGS_TO_REMOVE),
        project_name,
        **entity_identification_kwargs,
    )
    repository.write_domain(
        CommentUpdate(added_comments=COMMENTS_TO_ADD),
        project_name,
        **entity_identification_kwargs,
    )
    repository.write_domain(
        CommentUpdate(removed_comments=COMMENTS_TO_REMOVE),
        project_name,
        **entity_identification_kwargs,
    )


def _test_read_write_additional_tags_and_comments(
    repository, project_name, **entity_identification_kwargs
):
    is_passing = True

    additional_tags = repository.read_domains(
        TagUpdate,
        project_name,
        **entity_identification_kwargs,
    )

    for tag_update in additional_tags:
        if tag_update.added_tags:
            is_passing &= tag_update.added_tags == TAGS_TO_ADD
        if tag_update.removed_tags:
            is_passing &= tag_update.removed_tags == TAGS_TO_REMOVE

    additional_comments = repository.read_domains(
        CommentUpdate,
        project_name,
        **entity_identification_kwargs,
    )

    for comment_update in additional_comments:
        if comment_update.added_comments:
            is_passing &= comment_update.added_comments == COMMENTS_TO_ADD
        if comment_update.removed_comments:
            is_passing &= comment_update.removed_comments == COMMENTS_TO_REMOVE

    return is_passing


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
def test_read_write_project_regression(project_json, repository_class):
    """Tests that `rubicon_ml` can read the project domain entity that it wrote."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        repository.create_project(domain_project)
        project = repository.get_project(project_json["name"]).__dict__

        assert project == project_json


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
def test_read_write_experiment_regression(experiment_json, project_json, repository_class):
    """Tests that `rubicon_ml` can read the experiment domain entity that it wrote."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        domain_experiment = domain.Experiment(**experiment_json)
        repository.create_project(domain_project)
        repository.write_domain(
            domain_experiment, domain_project.name, experiment_id=domain_experiment.id
        )

        _write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_experiment.id,
            entity_type="Experiment",
        )

        if repository_class == WandBRepository:
            repository.finish()

        experiment = repository.read_domain(
            domain.Experiment,
            domain_project.name,
            experiment_id=domain_experiment.id,
        ).__dict__

        assert experiment == domain_experiment.__dict__
        assert _test_read_write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_experiment.id,
            entity_type="Experiment",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
@pytest.mark.parametrize("is_existing_experiment", [True, False])
def test_read_write_feature_regression(
    experiment_json,
    feature_json,
    project_json,
    repository_class,
    is_existing_experiment,
):
    """Tests that `rubicon_ml` can read the feature domain entity that it wrote."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_feature_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        domain_experiment = domain.Experiment(**experiment_json)
        domain_feature = domain.Feature(**feature_json)
        repository.create_project(domain_project)
        repository.write_domain(
            domain_experiment, domain_project.name, experiment_id=domain_experiment.id
        )

        if is_existing_experiment and repository_class == WandBRepository:
            repository.finish()
            repository.read_domain(
                domain.Experiment, domain_project.name, experiment_id=domain_experiment.id
            )

        repository.write_domain(
            domain_feature,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_feature.name,
            entity_type="Feature",
        )

        _write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_feature.name,
            entity_type="Feature",
        )

        if repository_class == WandBRepository:
            repository.finish()

        feature = repository.read_domain(
            domain.Feature,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_feature.name,
            entity_type="Feature",
        ).__dict__

        assert feature == domain_feature.__dict__
        assert _test_read_write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_feature.name,
            entity_type="Feature",
        )

    if repository_class == MemoryRepository:
        repository.filesystem.store = {}


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
@pytest.mark.parametrize("is_existing_experiment", [True, False])
def test_read_write_metric_regression(
    experiment_json,
    metric_json,
    project_json,
    repository_class,
    is_existing_experiment,
):
    """Tests that `rubicon_ml` can read the metric domain entity that it wrote."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_metric_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        domain_experiment = domain.Experiment(**experiment_json)
        domain_metric = domain.Metric(**metric_json)
        repository.create_project(domain_project)
        repository.write_domain(
            domain_experiment, domain_project.name, experiment_id=domain_experiment.id
        )

        if is_existing_experiment and repository_class == WandBRepository:
            repository.finish()
            repository.read_domain(
                domain.Experiment, domain_project.name, experiment_id=domain_experiment.id
            )

        repository.write_domain(
            domain_metric,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_metric.name,
            entity_type="Metric",
        )

        _write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_metric.name,
            entity_type="Metric",
        )

        if repository_class == WandBRepository:
            repository.finish()

        metric = repository.read_domain(
            domain.Metric,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_metric.name,
            entity_type="Metric",
        ).__dict__

        assert metric == domain_metric.__dict__
        assert _test_read_write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_metric.name,
            entity_type="Metric",
        )

    if repository_class == MemoryRepository:
        repository.filesystem.store = {}


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
@pytest.mark.parametrize("is_existing_experiment", [True, False])
def test_read_write_parameter_regression(
    experiment_json,
    parameter_json,
    project_json,
    repository_class,
    is_existing_experiment,
):
    """Tests that `rubicon_ml` can read the parameter domain entity that it wrote."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_parameter_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        domain_experiment = domain.Experiment(**experiment_json)
        domain_parameter = domain.Parameter(**parameter_json)
        repository.create_project(domain_project)
        repository.write_domain(
            domain_experiment, domain_project.name, experiment_id=domain_experiment.id
        )

        if is_existing_experiment and repository_class == WandBRepository:
            repository.finish()
            repository.read_domain(
                domain.Experiment, domain_project.name, experiment_id=domain_experiment.id
            )

        repository.write_domain(
            domain_parameter,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_parameter.name,
            entity_type="Parameter",
        )

        _write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_parameter.name,
            entity_type="Parameter",
        )

        if repository_class == WandBRepository:
            repository.finish()

        parameter = repository.read_domain(
            domain.Parameter,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_parameter.name,
            entity_type="Parameter",
        ).__dict__

        assert parameter == domain_parameter.__dict__
        assert _test_read_write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_parameter.name,
            entity_type="Parameter",
        )

    if repository_class == MemoryRepository:
        repository.filesystem.store = {}


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
def test_read_write_artifact_project_regression(
    artifact_project_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the artifact (project) domain entity that it wrote.

    For WandBRepository, this test verifies that project-level artifacts raise
    a RubiconException since W&B does not support project-level artifacts.
    """
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_artifact_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        domain_artifact = domain.Artifact(**artifact_project_json)
        repository.create_project(domain_project)

        if repository_class == WandBRepository:
            with pytest.raises(RubiconException, match="does not support project-level artifacts"):
                repository.create_artifact(
                    domain_artifact,
                    ARTIFACT_BINARY,
                    domain_project.name,
                )
            return

        repository.create_artifact(
            domain_artifact,
            ARTIFACT_BINARY,
            domain_project.name,
        )

        _write_additional_tags_and_comments(
            repository,
            domain_project.name,
            entity_identifier=domain_artifact.id,
            entity_type="Artifact",
        )

        artifact_project = repository.read_domain(
            domain.Artifact,
            domain_project.name,
            entity_identifier=domain_artifact.id,
            entity_type="Artifact",
        ).__dict__
        artifact_project_data = repository.read_artifact_data(
            domain_project.name,
            domain_artifact.id,
        )

        assert artifact_project == domain_artifact.__dict__
        assert artifact_project_data == ARTIFACT_BINARY
        assert _test_read_write_additional_tags_and_comments(
            repository,
            domain_project.name,
            entity_identifier=domain_artifact.id,
            entity_type="Artifact",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
@pytest.mark.parametrize("is_existing_experiment", [True, False])
def test_read_write_artifact_experiment_regression(
    artifact_experiment_json,
    experiment_json,
    project_json,
    repository_class,
    is_existing_experiment,
):
    """Tests that `rubicon_ml` can read the artifact (experiment) domain entity that it wrote."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_artifact_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        domain_experiment = domain.Experiment(**experiment_json)
        domain_artifact = domain.Artifact(**artifact_experiment_json)
        repository.create_project(domain_project)
        repository.write_domain(
            domain_experiment, domain_project.name, experiment_id=domain_experiment.id
        )

        if is_existing_experiment and repository_class == WandBRepository:
            repository.finish()
            repository.read_domain(
                domain.Experiment, domain_project.name, experiment_id=domain_experiment.id
            )

        repository.create_artifact(
            domain_artifact,
            ARTIFACT_BINARY,
            domain_project.name,
            domain_experiment.id,
        )

        _write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_artifact.id,
            entity_type="Artifact",
        )

        if repository_class == WandBRepository:
            repository.finish()

        artifact_experiment = repository.read_domain(
            domain.Artifact,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_artifact.id,
            entity_type="Artifact",
        ).__dict__
        artifact_experiment_data = repository.read_artifact_data(
            domain_project.name,
            domain_artifact.id,
            experiment_id=domain_experiment.id,
        )

        assert artifact_experiment == domain_artifact.__dict__
        assert artifact_experiment_data == ARTIFACT_BINARY
        assert _test_read_write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_artifact.id,
            entity_type="Artifact",
        )

    if repository_class == MemoryRepository:
        repository.filesystem.store = {}


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
def test_read_write_dataframe_project_regression(
    dataframe_project_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can read the dataframe (project) domain entity that it wrote.

    For WandBRepository, this test verifies that project-level dataframes raise
    a RubiconException since W&B does not support project-level dataframes.
    """
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_dataframe_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        domain_dataframe = domain.Dataframe(**dataframe_project_json)
        repository.create_project(domain_project)

        if repository_class == WandBRepository:
            with pytest.raises(RubiconException, match="does not support project-level dataframes"):
                repository.create_dataframe(
                    domain_dataframe,
                    DATAFRAME,
                    domain_project.name,
                )
            return

        repository.create_dataframe(
            domain_dataframe,
            DATAFRAME,
            domain_project.name,
        )

        _write_additional_tags_and_comments(
            repository,
            domain_project.name,
            entity_identifier=domain_dataframe.id,
            entity_type="Dataframe",
        )

        dataframe_project = repository.read_domain(
            domain.Dataframe,
            domain_project.name,
            entity_identifier=domain_dataframe.id,
            entity_type="Dataframe",
        ).__dict__
        dataframe_project_data = repository.read_dataframe_data(
            domain_project.name,
            domain_dataframe.id,
        )

        assert dataframe_project == domain_dataframe.__dict__
        assert dataframe_project_data.equals(DATAFRAME)
        assert _test_read_write_additional_tags_and_comments(
            repository,
            domain_project.name,
            entity_identifier=domain_dataframe.id,
            entity_type="Dataframe",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_READ_WRITE_TEST)
@pytest.mark.parametrize("is_existing_experiment", [True, False])
def test_read_write_dataframe_experiment_regression(
    dataframe_experiment_json,
    experiment_json,
    project_json,
    repository_class,
    is_existing_experiment,
):
    """Tests that `rubicon_ml` can read the dataframe (experiment) domain entity that it wrote."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_read_write_dataframe_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        domain_project = domain.Project(**project_json)
        domain_experiment = domain.Experiment(**experiment_json)
        domain_dataframe = domain.Dataframe(**dataframe_experiment_json)
        repository.create_project(domain_project)
        repository.write_domain(
            domain_experiment, domain_project.name, experiment_id=domain_experiment.id
        )

        if is_existing_experiment and repository_class == WandBRepository:
            repository.finish()
            repository.read_domain(
                domain.Experiment, domain_project.name, experiment_id=domain_experiment.id
            )

        repository.create_dataframe(
            domain_dataframe,
            DATAFRAME,
            domain_project.name,
            domain_experiment.id,
        )

        _write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_dataframe.id,
            entity_type="Dataframe",
        )

        if repository_class == WandBRepository:
            repository.finish()

        dataframe_experiment = repository.read_domain(
            domain.Dataframe,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_dataframe.id,
            entity_type="Dataframe",
        ).__dict__
        dataframe_experiment_data = repository.read_dataframe_data(
            domain_project.name,
            domain_dataframe.id,
            experiment_id=domain_experiment.id,
        )

        assert dataframe_experiment == domain_dataframe.__dict__
        assert dataframe_experiment_data.equals(DATAFRAME)
        assert _test_read_write_additional_tags_and_comments(
            repository,
            domain_project.name,
            experiment_id=domain_experiment.id,
            entity_identifier=domain_dataframe.id,
            entity_type="Dataframe",
        )

    if repository_class == MemoryRepository:
        repository.filesystem.store = {}


def _test_write_additional_tags_and_comments(
    repository, tag_dir, project_name, **entity_identification_kwargs
):
    is_passing = True

    repository.write_domain(
        TagUpdate(added_tags=TAGS_TO_ADD),
        project_name,
        **entity_identification_kwargs,
    )
    repository.write_domain(
        TagUpdate(removed_tags=TAGS_TO_REMOVE),
        project_name,
        **entity_identification_kwargs,
    )

    tag_path = os.path.join(tag_dir, "tags_*.json")
    tag_files = repository.filesystem.glob(tag_path, detail=True)
    for tag_file in tag_files:
        with repository.filesystem.open(tag_file, "r") as file:
            tags = json.loads(file.read())

            if "added_tags" in tags:
                is_passing &= tags["added_tags"] == TAGS_TO_ADD
            if "removed_tags" in tags:
                is_passing &= tags["removed_tags"] == TAGS_TO_REMOVE

    repository.write_domain(
        CommentUpdate(added_comments=COMMENTS_TO_ADD),
        project_name,
        **entity_identification_kwargs,
    )
    repository.write_domain(
        CommentUpdate(removed_comments=COMMENTS_TO_REMOVE),
        project_name,
        **entity_identification_kwargs,
    )

    comment_path = os.path.join(tag_dir, "comments_*.json")
    comment_files = repository.filesystem.glob(comment_path, detail=True)
    for comment_file in comment_files:
        with repository.filesystem.open(comment_file, "r") as file:
            comments = json.loads(file.read())

            if "added_comments" in comments:
                is_passing &= comments["added_comments"] == COMMENTS_TO_ADD
            if "removed_tags" in comments:
                is_passing &= comments["removed_comments"] == COMMENTS_TO_REMOVE

    return is_passing


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_project_regression(
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write a project domain entity to the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_write_project_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))

        expected_project_dir = os.path.join(root_dir, slugify(project_json["name"]))
        expected_project_path = os.path.join(expected_project_dir, "metadata.json")

        with repository.filesystem.open(expected_project_path, "r") as file:
            project = json.loads(file.read())

        assert project == project_json


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_experiment_regression(
    experiment_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write an experiment domain entity to the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_write_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        repository.write_domain(
            domain.Experiment(**experiment_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
        )

        expected_experiment_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
        )
        expected_experiment_path = os.path.join(expected_experiment_dir, "metadata.json")

        with repository.filesystem.open(expected_experiment_path, "r") as file:
            experiment = json.loads(file.read())

        assert experiment == experiment_json
        assert _test_write_additional_tags_and_comments(
            repository,
            expected_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=experiment_json["id"],
            entity_type="Experiment",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_feature_regression(
    experiment_json,
    feature_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write a feature domain entity to the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_write_feature_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        repository.write_domain(
            domain.Experiment(**experiment_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
        )
        repository.write_domain(
            domain.Feature(**feature_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=feature_json["name"],
            entity_type="Feature",
        )

        expected_feature_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "features",
            slugify(feature_json["name"]),
        )
        expected_feature_path = os.path.join(expected_feature_dir, "metadata.json")

        with repository.filesystem.open(expected_feature_path, "r") as file:
            feature = json.loads(file.read())

        assert feature == feature_json
        assert _test_write_additional_tags_and_comments(
            repository,
            expected_feature_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=feature_json["name"],
            entity_type="Feature",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_metric_regression(
    experiment_json,
    metric_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write a metric domain entity to the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_write_metric_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        repository.write_domain(
            domain.Experiment(**experiment_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
        )
        repository.write_domain(
            domain.Metric(**metric_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=metric_json["name"],
            entity_type="Metric",
        )

        expected_metric_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "metrics",
            slugify(metric_json["name"]),
        )
        expected_metric_path = os.path.join(expected_metric_dir, "metadata.json")

        with repository.filesystem.open(expected_metric_path, "r") as file:
            metric = json.loads(file.read())

        assert metric == metric_json
        assert _test_write_additional_tags_and_comments(
            repository,
            expected_metric_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=metric_json["name"],
            entity_type="Metric",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_parameter_regression(
    experiment_json,
    parameter_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write a parameter domain entity to the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(enter_result="./test_write_parameter_regression/")

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        repository.write_domain(
            domain.Experiment(**experiment_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
        )
        repository.write_domain(
            domain.Parameter(**parameter_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=parameter_json["name"],
            entity_type="Parameter",
        )

        expected_parameter_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "parameters",
            slugify(parameter_json["name"]),
        )
        expected_parameter_path = os.path.join(expected_parameter_dir, "metadata.json")

        with repository.filesystem.open(expected_parameter_path, "r") as file:
            parameter = json.loads(file.read())

        assert parameter == parameter_json
        assert _test_write_additional_tags_and_comments(
            repository,
            expected_parameter_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=parameter_json["name"],
            entity_type="Parameter",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_artifact_project_regression(
    artifact_project_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write an artifact (project) domain entity to the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_write_artifact_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        repository.create_artifact(
            domain.Artifact(**artifact_project_json),
            ARTIFACT_BINARY,
            project_json["name"],
        )

        expected_artifact_project_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "artifacts",
            artifact_project_json["id"],
        )
        expected_artifact_project_path = os.path.join(
            expected_artifact_project_dir, "metadata.json"
        )
        expected_artifact_project_data_path = os.path.join(expected_artifact_project_dir, "data")

        with repository.filesystem.open(expected_artifact_project_path, "r") as file:
            artifact_project = json.loads(file.read())
        with repository.filesystem.open(expected_artifact_project_data_path, "rb") as file:
            artifact_project_data = file.read()

        assert artifact_project == artifact_project_json
        assert artifact_project_data == ARTIFACT_BINARY
        assert _test_write_additional_tags_and_comments(
            repository,
            expected_artifact_project_dir,
            project_json["name"],
            entity_identifier=artifact_project_json["id"],
            entity_type="Artifact",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_artifact_experiment_regression(
    artifact_experiment_json,
    experiment_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write an artifact (experiment) domain entity to the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_write_artifact_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        repository.write_domain(
            domain.Experiment(**experiment_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
        )
        repository.create_artifact(
            domain.Artifact(**artifact_experiment_json),
            ARTIFACT_BINARY,
            project_json["name"],
            experiment_json["id"],
        )

        expected_artifact_experiment_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "artifacts",
            artifact_experiment_json["id"],
        )
        expected_artifact_experiment_path = os.path.join(
            expected_artifact_experiment_dir, "metadata.json"
        )
        expected_artifact_experiment_data_path = os.path.join(
            expected_artifact_experiment_dir, "data"
        )

        with repository.filesystem.open(expected_artifact_experiment_path, "r") as file:
            artifact_experiment = json.loads(file.read())
        with repository.filesystem.open(expected_artifact_experiment_data_path, "rb") as file:
            artifact_experiment_data = file.read()

        assert artifact_experiment == artifact_experiment_json
        assert artifact_experiment_data == ARTIFACT_BINARY
        assert _test_write_additional_tags_and_comments(
            repository,
            expected_artifact_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=artifact_experiment_json["id"],
            entity_type="Artifact",
        )


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_dataframe_project_regression(
    dataframe_project_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write a dataframe (project) domain entity to the filesystem.

    The `MemoryRepository` skips dataframe data as the `pandas` API can not be used to read directly
    from memory. Dataframe data regression tests are covered by `test_read_write_regression`.
    """
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_write_dataframe_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        repository.create_dataframe(
            domain.Dataframe(**dataframe_project_json),
            DATAFRAME,
            project_json["name"],
        )

        expected_dataframe_project_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "dataframes",
            dataframe_project_json["id"],
        )
        expected_dataframe_project_path = os.path.join(
            expected_dataframe_project_dir, "metadata.json"
        )
        with repository.filesystem.open(expected_dataframe_project_path, "r") as file:
            dataframe_project = json.loads(file.read())

        assert dataframe_project == dataframe_project_json
        assert _test_write_additional_tags_and_comments(
            repository,
            expected_dataframe_project_dir,
            project_json["name"],
            entity_identifier=dataframe_project_json["id"],
            entity_type="Dataframe",
        )

        if repository_class != MemoryRepository:
            expected_dataframe_project_data_path = os.path.join(
                expected_dataframe_project_dir, "data", "data.parquet"
            )

            dataframe_project_data = pd.read_parquet(expected_dataframe_project_data_path)

            assert dataframe_project_data.equals(DATAFRAME)


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_write_dataframe_experiment_regression(
    dataframe_experiment_json,
    experiment_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can write a dataframe (experiment) domain entity to the filesystem.

    The `MemoryRepository` skips dataframe data as the `pandas` API can not be used to read directly
    from memory. Dataframe data regression tests are covered by `test_read_write_regression`.
    """
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_write_dataframe_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_project(domain.Project(**project_json))
        repository.write_domain(
            domain.Experiment(**experiment_json),
            project_json["name"],
            experiment_id=experiment_json["id"],
        )
        repository.create_dataframe(
            domain.Dataframe(**dataframe_experiment_json),
            DATAFRAME,
            project_json["name"],
            experiment_json["id"],
        )

        expected_dataframe_experiment_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "dataframes",
            dataframe_experiment_json["id"],
        )
        expected_dataframe_experiment_path = os.path.join(
            expected_dataframe_experiment_dir, "metadata.json"
        )

        with repository.filesystem.open(expected_dataframe_experiment_path, "r") as file:
            dataframe_experiment = json.loads(file.read())

        assert dataframe_experiment == dataframe_experiment_json
        assert _test_write_additional_tags_and_comments(
            repository,
            expected_dataframe_experiment_dir,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=dataframe_experiment_json["id"],
            entity_type="Dataframe",
        )

        if repository_class != MemoryRepository:
            expected_dataframe_experiment_data_path = os.path.join(
                expected_dataframe_experiment_dir, "data", "data.parquet"
            )

            dataframe_experiment_data = pd.read_parquet(expected_dataframe_experiment_data_path)

            assert dataframe_experiment_data.equals(DATAFRAME)


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_delete_artifact_project_regression(
    artifact_project_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can delete an artifact (project) domain from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_delete_artifact_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)

        repository.create_artifact(
            domain.Artifact(**artifact_project_json),
            ARTIFACT_BINARY,
            project_json["name"],
        )

        expected_artifact_project_path = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "artifacts",
            artifact_project_json["id"],
            "metadata.json",
        )

        assert repository.filesystem.exists(expected_artifact_project_path)

        repository.remove_domain(
            domain.Artifact,
            project_json["name"],
            entity_identifier=artifact_project_json["id"],
            entity_type="Artifact",
        )

        assert not repository.filesystem.exists(expected_artifact_project_path)


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_delete_artifact_experiment_regression(
    artifact_experiment_json,
    experiment_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can delete an artifact (experiment) domain from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_delete_artifact_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)
        repository.create_artifact(
            domain.Artifact(**artifact_experiment_json),
            ARTIFACT_BINARY,
            project_json["name"],
            experiment_json["id"],
        )

        expected_experiment_dir = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
        )
        expected_artifact_experiment_path = os.path.join(
            expected_experiment_dir,
            "artifacts",
            artifact_experiment_json["id"],
            "metadata.json",
        )

        assert repository.filesystem.exists(expected_artifact_experiment_path)

        repository.remove_domain(
            domain.Artifact,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=artifact_experiment_json["id"],
            entity_type="Artifact",
        )

        assert not repository.filesystem.exists(expected_artifact_experiment_path)


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_delete_dataframe_project_regression(
    dataframe_project_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can delete a dataframe (project) domain from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_delete_dataframe_project_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)
        repository.create_dataframe(
            domain.Dataframe(**dataframe_project_json),
            DATAFRAME,
            project_json["name"],
        )

        expected_dataframe_project_path = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "dataframes",
            dataframe_project_json["id"],
            "metadata.json",
        )

        assert repository.filesystem.exists(expected_dataframe_project_path)

        repository.remove_domain(
            domain.Dataframe,
            project_json["name"],
            entity_identifier=dataframe_project_json["id"],
            entity_type="Dataframe",
        )

        assert not repository.filesystem.exists(expected_dataframe_project_path)


@pytest.mark.parametrize("repository_class", REPOSITORIES_TO_TEST)
def test_delete_dataframe_experiment_regression(
    dataframe_experiment_json,
    experiment_json,
    project_json,
    repository_class,
):
    """Tests that `rubicon_ml` can delete a dataframe (experiment) domain from the filesystem."""
    if repository_class == LocalRepository:
        temp_dir_context = tempfile.TemporaryDirectory()
    else:
        temp_dir_context = contextlib.nullcontext(
            enter_result="./test_delete_dataframe_experiment_regression/"
        )

    with temp_dir_context as temp_dir_name:
        root_dir = os.path.join(temp_dir_name, "test-rubicon-ml")
        repository = repository_class(root_dir=root_dir)
        repository.create_dataframe(
            domain.Dataframe(**dataframe_experiment_json),
            DATAFRAME,
            project_json["name"],
            experiment_json["id"],
        )

        expected_dataframe_experiment_path = os.path.join(
            root_dir,
            slugify(project_json["name"]),
            "experiments",
            experiment_json["id"],
            "dataframes",
            dataframe_experiment_json["id"],
            "metadata.json",
        )

        assert repository.filesystem.exists(expected_dataframe_experiment_path)

        repository.remove_domain(
            domain.Dataframe,
            project_json["name"],
            experiment_id=experiment_json["id"],
            entity_identifier=dataframe_experiment_json["id"],
            entity_type="Dataframe",
        )

        assert not repository.filesystem.exists(expected_dataframe_experiment_path)
