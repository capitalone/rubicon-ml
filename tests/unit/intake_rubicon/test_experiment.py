import os

import intake

from rubicon_ml.intake_rubicon.experiment import ExperimentSource

intake.register_driver("rubicon_ml_experiment", ExperimentSource, clobber=True)

root = os.path.dirname(__file__)
project_name = "intake-rubicon unit testing"
experiment_id = "5f6ca6a1-563f-4f34-a34b-b45997bc4a1f"


def test_source():
    source = ExperimentSource(os.path.join(root, "data"), project_name, experiment_id)
    assert source is not None

    source.discover()

    experiment = source.read()
    assert experiment is not None
    assert experiment.id == experiment_id

    metadata = source._schema.extra_metadata
    assert metadata["experiment"]["name"] == experiment.name

    source.close()


def test_catalog():
    cat = intake.open_catalog(os.path.join(root, "catalog.yml"))
    source = cat.experiment_a()
    source.discover()

    experiment = source.read()
    assert experiment is not None
    assert experiment.id == experiment_id
