import fsspec
import yaml

from rubicon_ml import publish


def test_publish(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    experiment = project.log_experiment()

    catalog_yaml = publish(project.experiments())
    catalog = yaml.safe_load(catalog_yaml)

    assert f"project_{project.id.replace('-', '_')}" in catalog["sources"]
    assert (
        "rubicon_ml_project"
        == catalog["sources"][f"project_{project.id.replace('-', '_')}"]["driver"]
    )
    assert (
        project.repository.root_dir
        == catalog["sources"][f"project_{project.id.replace('-', '_')}"]["args"]["urlpath"]
    )
    assert (
        project.name
        == catalog["sources"][f"project_{project.id.replace('-', '_')}"]["args"]["project_name"]
    )
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


def test_publish_to_file(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    project.log_experiment()
    project.log_experiment()

    catalog_yaml = publish(project.experiments(), output_filepath="memory://catalog.yml")

    with fsspec.open("memory://catalog.yml", "r") as f:
        written_catalog = f.read()

    assert catalog_yaml == written_catalog
