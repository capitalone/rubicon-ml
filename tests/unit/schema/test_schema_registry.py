"""Testing ``schema.registry``."""

import pytest

from rubicon_ml.schema import registry


def test_available_schema():
    """Testing ``schema.registry.available_schema``."""

    for schema_name in registry.available_schema():
        assert schema_name in registry.RUBICON_SCHEMA_REGISTRY

    assert len(registry.available_schema()) == len(registry.RUBICON_SCHEMA_REGISTRY)


def test_get_schema():
    """Testing ``schema.registry.get_schema``."""

    for name, load_func in registry.RUBICON_SCHEMA_REGISTRY.items():
        schema = registry.get_schema(name)

        assert load_func() == schema


def test_get_schema_raises_error():
    """Testing ``schema.registry.get_schema`` raises an error when an invalid name is given."""

    with pytest.raises(ValueError) as err:
        registry.get_schema("InvalidSchemaClass")

    assert "'InvalidSchemaClass' is not the name of an available rubicon schema." in str(err)


def test_get_schema_name(objects_to_log):
    """Testing ``schema.registry.get_schema_name``."""

    object_to_log, _ = objects_to_log

    schema_name = registry.get_schema_name(object_to_log)

    assert schema_name == "tests___ObjectToLog"


def test_register_schema():
    """Testing ``schema.registry.register_schema``."""

    schema_name = "c1-rubicon-schema__TestRegisterSchema"
    schema = {"name": schema_name}

    registry.register_schema(schema_name, schema)

    assert schema_name in registry.RUBICON_SCHEMA_REGISTRY
    assert registry.get_schema(schema_name) == schema

    del registry.RUBICON_SCHEMA_REGISTRY[schema_name]
