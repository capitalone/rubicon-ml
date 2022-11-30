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
        Similar to `output_filepath` except this argument is used as a
        base base file to update an existing intake catalog. Defaults to None,
        creating a new intake catalog.

    Returns
    -------
    str
        The YAML string representation of the `intake` catalog
        containing the experiments `experiments`.
    """

    if base_catalog_filepath is not None:
        return _update_catalog(
            base_catalog_filepath=base_catalog_filepath,
            new_experiments=experiments,
            output_filepath=output_filepath,
        )

    catalog = _build_catalog(experiments=experiments)
    catalog_yaml = yaml.dump(catalog)

    if output_filepath is not None:
        with fsspec.open(output_filepath, "w", auto_mkdir=False) as f:
            f.write(catalog_yaml)

    return catalog_yaml


def _update_catalog(base_catalog_filepath, new_experiments, output_filepath=None):

    """Helper function to update exisiting intake catalog.

    Parameters
    ----------
    base_catalog_filepath : str
        the absolute or relative catalog filepath or S3 bucket
        and key to log the generated YAML file to. S3 buckets
        must be prepended with 's3://. Retrieved from the parameter
        of the publish function. NOT optional
    new_experiments : list of rubicon_ml.client.experiment.Experiment
         The new experiments to append to the catalog at
        `base_catalog_filepath`.
    output_catalog_filepath : str, optional
        absolute or relative filepath or S3 bucket
        and key to log the generated YAML file to. (S3 buckets
        must be prepended with 's3://) to  output the updated catalog into.
        Default is None, which resolves to dumping updated catalog into
        base_catalog_filepath path (primary use-case)

    Returns
    -------
    dict
        The dictionary of all sources given as experiments to eventually publish
    """

    updated_catalog = _build_catalog(experiments=new_experiments)

    with fsspec.open(base_catalog_filepath, "r") as yamlfile:
        curr_catalog = yaml.safe_load(yamlfile)

        curr_catalog["sources"].update(updated_catalog["sources"])

    resulting_filepath = base_catalog_filepath if not output_filepath else output_filepath

    with fsspec.open(resulting_filepath, "w") as yamlfile:
        yaml.safe_dump(curr_catalog, yamlfile)
        updated_catalog = yaml.dump(curr_catalog)

    return updated_catalog


def _build_catalog(experiments):

    """Helper function to build catalog dictionary from given experiments.

    Parameters
    ----------
    experiments : list of rubicon_ml.client.experiment.Experiment
        The expriments that are used to build the catalog to eventually publish
    Returns
    -------
    str
        The YAML string representation of the `intake` catalog
        containing the experiments `experiments`.
    """

    catalog = {"sources": {}}

    for experiment in experiments:
        appended_experiment_catalog = {
            "driver": "rubicon_ml_experiment",
            "args": {
                "experiment_id": experiment.id,
                "project_name": experiment.project.name,
                "urlpath": experiment.repository.root_dir,
            },
        }

        experiment_catalog_name = f"experiment_{experiment.id.replace('-', '_')}"
        catalog["sources"][experiment_catalog_name] = appended_experiment_catalog

    return catalog
