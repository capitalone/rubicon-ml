import os
import tempfile

import fsspec
import yaml

from rubicon_ml import Rubicon, publish


def test_publish(project_client):
    project = project_client
    experiment = project.log_experiment()

    catalog_yaml = publish(project.experiments())
    catalog = yaml.safe_load(catalog_yaml)

    assert f"experiment_{experiment.id.replace('-', '_')}" in catalog["sources"]
    assert (
        "rubicon_ml_experiment"
        == catalog["sources"][f"experiment_{experiment.id.replace('-', '_')}"]["driver"]
    )
    assert (
        experiment.repository.root_dir
        == catalog["sources"][f"experiment_{experiment.id.replace('-', '_')}"]["args"]["urlpath"]
    )
    assert (
        experiment.id
        == catalog["sources"][f"experiment_{experiment.id.replace('-', '_')}"]["args"][
            "experiment_id"
        ]
    )
    assert (
        project.name
        == catalog["sources"][f"experiment_{experiment.id.replace('-', '_')}"]["args"][
            "project_name"
        ]
    )


def test_publish_from_multiple_sources():
    rubicon_a = Rubicon(persistence="memory", root_dir="path/a")
    rubicon_b = Rubicon(persistence="memory", root_dir="path/b")

    experiment_a = rubicon_a.create_project("test").log_experiment()
    experiment_b = rubicon_b.create_project("test").log_experiment()

    catalog_yaml = publish([experiment_a, experiment_b])
    catalog = yaml.safe_load(catalog_yaml)

    assert (
        rubicon_a.repository.root_dir
        == catalog["sources"][f"experiment_{experiment_a.id.replace('-', '_')}"]["args"]["urlpath"]
    )
    assert (
        rubicon_b.repository.root_dir
        == catalog["sources"][f"experiment_{experiment_b.id.replace('-', '_')}"]["args"]["urlpath"]
    )
    assert (
        catalog["sources"][f"experiment_{experiment_a.id.replace('-', '_')}"]["args"]["urlpath"]
        != catalog["sources"][f"experiment_{experiment_b.id.replace('-', '_')}"]["args"]["urlpath"]
    )


def test_publish_to_file(project_client):
    project = project_client
    project.log_experiment()
    project.log_experiment()

    with tempfile.TemporaryDirectory() as temp_dir_name:
        catalog_file = os.path.join(temp_dir_name, "catalog.yaml")
        catalog_yaml = publish(project.experiments(), output_filepath=catalog_file)

        with fsspec.open(catalog_file, "r") as f:
            written_catalog = f.read()

    assert catalog_yaml == written_catalog


def test_update_catalog(project_client):
    project = project_client
    project.log_experiment()
    project.log_experiment()

    with tempfile.TemporaryDirectory() as temp_dir_name:
        catalog_file = os.path.join(temp_dir_name, "catalog.yaml")
        publish(project.experiments(), output_filepath=catalog_file)

        # add new experiments to project
        experiment_c = project.log_experiment()
        experiment_d = project.log_experiment()

        new_experiments = [experiment_c, experiment_d]

        # publish new experiments into the exisiting catalog
        updated_catalog = publish(
            base_catalog_filepath=catalog_file, experiments=new_experiments
        )

        with fsspec.open(catalog_file, "r") as f:
            written_catalog = f.read()

    assert updated_catalog == written_catalog
