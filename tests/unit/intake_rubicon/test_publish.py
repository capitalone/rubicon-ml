import fsspec
import yaml

from rubicon_ml import Rubicon, publish
from rubicon_ml.viz.dataframe_plot import DataframePlot
from rubicon_ml.viz.experiments_table import ExperimentsTable
from rubicon_ml.viz.metric_correlation_plot import MetricCorrelationPlot
from rubicon_ml.viz.metric_lists_comparison import MetricListsComparison


def test_publish(project_client):
    # Experiment Table

    project = project_client
    experiment = project.log_experiment()
    visualization_object = ExperimentsTable()
    catalog_yaml = publish(project.experiments(), visualization_object)
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
    assert catalog["sources"]["experiment_table"] is not None

    # Metric Correlation Plot

    visualization_object = MetricCorrelationPlot()
    catalog_yaml = publish(project.experiments(), visualization_object)
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
    assert catalog["sources"]["metric_correlation_plot"] is not None

    # Dataframe Plot

    visualization_object = DataframePlot(dataframe_name="test_dataframe")
    catalog_yaml = publish(project.experiments(), visualization_object)
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
    assert catalog["sources"]["dataframe_plot"] is not None

    # MetricListComparison

    visualization_object = MetricListsComparison()
    catalog_yaml = publish(project.experiments(), visualization_object)
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
    assert catalog["sources"]["metric_list"] is not None


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

    catalog_yaml = publish(project.experiments(), output_filepath="memory://catalog.yml")

    with fsspec.open("memory://catalog.yml", "r") as f:
        written_catalog = f.read()

    assert catalog_yaml == written_catalog


def test_update_catalog(project_client):
    project = project_client
    project.log_experiment()
    project.log_experiment()

    publish(project.experiments(), output_filepath="memory://catalog.yml")

    # add new experiments to project
    experiment_c = project.log_experiment()
    experiment_d = project.log_experiment()

    new_experiments = [experiment_c, experiment_d]

    # publish new experiments into the exisiting catalog
    updated_catalog = publish(
        base_catalog_filepath="memory://catalog.yml", experiments=new_experiments
    )

    with fsspec.open("memory://catalog.yml", "r") as f:
        written_catalog = f.read()

    assert updated_catalog == written_catalog
