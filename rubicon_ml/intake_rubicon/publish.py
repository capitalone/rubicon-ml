import fsspec
import yaml


def publish(
    experiments,
    output_filepath=None,
    catalog_filepath=None,
):
    """Publish experiments to an `intake` catalog that can be
    read by the `intake-rubicon` driver.

    Parameters
    ----------
    experiments : list of rubicon_ml.client.experiment.Experiment
        The experiments to publish.
    output_filepath : str, optional
        The absolute or relative local filepath or S3 bucket
        and key to log the generated YAML file to. S3 buckets
        must be prepended with 's3://'. Defaults to None,
        which disables writing the generated YAML.

    Returns
    -------
    str
        The YAML string representation of the `intake` catalog
        containing the experiments `experiments`.
    """

    if catalog_filepath is not None:
        return update_catalog(filepath=catalog_filepath, new_experiments=experiments)
    catalog = {"sources": {}}

    for experiment in experiments:
        experiment_catalog = {
            "driver": "rubicon_ml_experiment",
            "args": {
                "experiment_id": experiment.id,
                "project_name": experiment.project.name,
                "urlpath": experiment.repository.root_dir,
            },
        }

        experiment_catalog_name = f"experiment_{experiment.id.replace('-', '_')}"
        catalog["sources"][experiment_catalog_name] = experiment_catalog

    catalog_yaml = yaml.dump(catalog)

    if output_filepath is not None:
        with fsspec.open(output_filepath, "w", auto_mkdir=False) as f:
            f.write(catalog_yaml)

    return catalog_yaml


def update_catalog(filepath, new_experiments):

    updated_catalog = {}
    for experiment in new_experiments:
        appended_experiment_catalog = {
            "driver": "rubicon_ml_experiment",
            "args": {
                "experiment_id": experiment.id,
                "project_name": experiment.project.name,
                "urlpath": experiment.repository.root_dir,
            },
        }

        experiment_catalog_name = f"experiment_{experiment.id.replace('-', '_')}"
        updated_catalog[experiment_catalog_name] = appended_experiment_catalog

    with open(filepath, "r") as yamlfile:
        curr_catalog = yaml.safe_load(yamlfile)
        curr_catalog["sources"].update(updated_catalog)

    if curr_catalog:
        with open(filepath, "w") as yamlfile:
            yaml.safe_dump(curr_catalog, yamlfile)

    return curr_catalog
