.. _contribute-schema:

Contribute a schema
*******************

Consider the following schema that was created in the "Register a custom schema" section:

.. code-block:: python

    extended_schema = {
        "name": "sklearn__RandomForestClassifier__ext",
        "extends": "sklearn__RandomForestClassifier",

        "parameters": [
            {"name": "runtime_environment", "value_env": "RUNTIME_ENV"},
        ],
    }

To contribute "sklearn__RandomForestClassifier__ext" to the ``rubicon_ml.schema`` registry,
first write the dictionary out to a YAML file.

.. code-block:: python

    import yaml

    schema_filename = "sklearn__RandomForestClassifier__ext.yaml"

    with open(schema_filename, "w") as file:
        file.write(yaml.dump(extended_schema))

Once "sklearn__RandomForestClassifier__ext.yaml" is created, follow the "Developer
instructions" to fork the rubicon-ml GitHub repository and prepare to make a contribution.

From the root of the forked repository, copy the new schema into the library's schema directory:

.. code-block:: bash

    cp [PATH_TO]/sklearn__RandomForestClassifier__ext.yaml rubicon_ml/schema/schema/

Then update **rubicon_ml/schema/registry.py**, adding the new schema to the
``RUBICON_SCHEMA_REGISTRY``:

.. code-block:: python

    RUBICON_SCHEMA_REGISTRY = {
        # other schema entries...
        "sklearn__RandomForestClassifier__ext": lambda: _load_schema(
            os.path.join("schema", "sklearn__RandomForestClassifier__ext.yaml")
        ),
    }

Finally refer back to the "Contribute" section of the "Developer instructions" to push your
changes to GitHub and open a pull request. Once the pull request is merged,
"sklearn__RandomForestClassifier__ext" will be available in the next release of
``rubicon_ml``.

Schema naming conventions
=========================

When naming a schema that extends a schema already made available by ``rubicon_ml.schema``, simply
append a double-underscore and a unique identifier. The "sklearn__RandomForestClassifier__ext"
above is named following this convention.

When naming a schema that represents an object that is not yet present in schema,
leverage the ``registry.get_schema_name`` function to generate a name. For example, if
you are making a schema for an object ``my_obj`` of class ``Model`` from a module ``my_model``,
``registry.get_schema_name(my_obj)`` will return the name "my_model__Model".
