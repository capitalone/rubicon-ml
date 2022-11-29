import fsspec
import yaml


def publish(
    experiments,
    output_filepath=None,
    base_catalog_filepath=None,
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
    base_catalog_filepath : str, optional
        Similar to output_filepath except this argument is used as a
        base fileto update exisiting intake catalog. Defaults to None,
        where the function proceeds as originally intended.

    Returns
    -------
    str
        The YAML string representation of the `intake` catalog
        containing the experiments `experiments`.
    """

    if base_catalog_filepath is not None:
        return update_catalog(
            base_catalog_filepath=base_catalog_filepath,
            new_experiments=experiments,
            output_filepath=output_filepath,
        )
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


"""Helper function to update exisiting intake catalog.

    Parameters
    ----------
    base_catalog_filepath : str
        the absolute or relative catalog filepath or S3 bucket
        and key to log the generated YAML file to. S3 buckets
        must be prepended with 's3://. Retrieved from the parameter
        of the publish function. NOT optional
    new experiments : list of rubicon_ml.client.experiment.Experiment
        The new experiments to publish.
    output_catalog_filepath : str, optional
        absolute or relative filepath or S3 bucket
        and key to log the generated YAML file to. (S3 buckets
        must be prepended with 's3://) to  output the updated catalog into.
        Default is None, which resolves to dumping updated catalog into
        base_catalog_filepath path (primary use-case)

    Returns
    -------
    str
        The YAML string representation of the `intake` catalog
        containing the experiments `experiments`.
    """


def update_catalog(base_catalog_filepath, new_experiments, output_filepath=None):

    updated_catalog = {}
    for experiment in new_experiments:
        appended_experiment_catalog = {
            "args": {
                "experiment_id": experiment.id,
                "project_name": experiment.project.name,
                "urlpath": experiment.repository.root_dir,
            },
            "driver": "rubicon_ml_experiment",
        }

        experiment_catalog_name = f"experiment_{experiment.id.replace('-', '_')}"
        updated_catalog[experiment_catalog_name] = appended_experiment_catalog

    with fsspec.open(base_catalog_filepath, "r") as yamlfile:
        curr_catalog = yaml.safe_load(yamlfile)

        curr_catalog["sources"].update(updated_catalog)

    resulting_filepath = base_catalog_filepath if not output_filepath else output_filepath
    if curr_catalog:
        with fsspec.open(resulting_filepath, "w") as yamlfile:
            yaml.safe_dump(curr_catalog, yamlfile)
            updated_catalog = yaml.dump(curr_catalog)
            return updated_catalog

    return None
