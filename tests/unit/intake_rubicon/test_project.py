import os

import intake

from rubicon_ml.intake_rubicon.project import ProjectSource

intake.register_driver("rubicon_project", ProjectSource)

root = os.path.dirname(__file__)
project_name = "intake-rubicon unit testing"


def test_source():
    source = ProjectSource(os.path.join(root, "data"), project_name)
    assert source is not None

    source.discover()

    project = source.read()
    assert project is not None
    assert project.name == project_name

    metadata = source._schema.extra_metadata
    assert metadata["project"]["name"] == project_name

    source.close()


def test_catalog():
    cat = intake.open_catalog(os.path.join(root, "catalog.yml"))
    source = cat.project_a()
    source.discover()

    project = source.read()
    assert project is not None
    assert project.name == project_name
