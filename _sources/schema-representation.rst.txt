.. _schema-representation:

Representing model metadata with a schema
*****************************************

A rubicon-ml schema is a YAML file defining how attributes of a Python object, generally
representing a model, will be logged to a rubicon-ml experiment. Schema can
be used to automatically instrument and standardize the rubicon-ml logging of commonly
used model objects.

Schema are used to log experiments to an existing rubicon-ml project.
Experiments consist of features, parameters, metrics, artifacts, and dataframes. More info
on each of these can be found in rubicon-ml's glossary.

A simple schema
===============

Consider the following objects from a module called ``my_model``:

.. code-block:: python

    import pandas as pd
    
    class Optimizer:
        def optimize(X, y, target):
            self.optimized_ = True
    
            return "optimized"
    
    class Model:
        def __init__(self, alpha=1e-3, gamma=1e-3):
            self.alpha = alpha
            self.gamma = gamma
    
        def fit(self, X, y):
            self.optimizer = Optimizer()
            self.target = "y"
    
            self.feature_names_in_ = X.columns
            self.feature_importances_ = [1.0 / len(X.columns)] * len(X.columns)
    
            self.learned_attribute_ = optimizer.optimize(X, y, target)
    
            return self
    
        def score(self, X):
            self.score_ = 1.0
            self.summary_ = pd.DataFrame(
                [[self.alpha, self.gamma, self.learned_attribute_, self.score_]],
                columns=["alpha", "gamma", "learned_attribute", "score"],
            )
            
            return self.score_

The following is a complete YAML representation of the ``Model`` object's schema:

.. code-block:: yaml

    name: my_model__Model
    verison: 1.0.0
    
    compatibility:
      pandas:
        max_version:
        min_version: 1.0.5
    docs_url: https://my-docs.com/my-model/Model.html
    
    artifacts:
      - self
      - name: optimizer
        data_object_attr: optimizer
    dataframes:
      - name: summary
        df_attr: summary_
    features:
      - names_attr: feature_names_in_
        importances_attr: feature_importances_
        optional: true
      - name_attr: target
    metrics:
      - name: learned_attribute
        value_attr: learned_attribute_
        optional: true
      - name: score
        value_attr: score_
      - name: env_metric
        value_env: METRIC
    parameters:
      - name: alpha
        value_attr: alpha
      - name: gamma
        value_attr: gamma
      - name: env_param
        value_env: PARAMETER

Schema metadata
---------------

The first section of the schema defines metadata about the schema itself,
like the name and version. **The name of a schema should be the name of the
library the class it represents comes from and the name of the Python class itself separated
by a double underscore.**

.. code-block:: yaml

    name: my_model__Model
    verison: 1.0.0

The next section defines any dependencies the model object has on external Python libraries.
Generally, this will be at least the library the object is imported from. Reference documentation
for the object to be logged can also be included in this section.

.. code-block:: yaml

    compatibility:
      pandas:
        max_version:
        min_version: 1.0.5
    docs_url: https://my-docs.com/my-model/Model.html

The remaining sections define how the attributes of the object will be logged to the
``rubicon-ml`` experiment. In general, each section is a list of attributes to log to
``rubicon-ml`` with a name for the logged metadata and the name of the attribute
containing the value to log.

Artifacts
---------

Define a :ref:`rubicon_ml.Artifact<library-reference-artifact>` 
for logging by providing a ``name`` for the logged artifact and the attribute ``data_object_attr``
containing the object to log. The special keyword ``self`` will log the full object the schema
represents as an artifact with the same name as the object's class.

.. code-block:: yaml

    artifacts:
      - self             # logs this Model as an artifact named "Model"
      - name: optimizer  # logs Optimizer in `optimizer` attribute as an artifact named "optimizer"
        data_object_attr: optimizer

Dataframes
----------

Define a :ref:`rubicon_ml.Dataframe<library-reference-dataframe>`
for logging by providing a ``name`` for the logged dataframe and the attribute ``df_attr``
containing the DataFrame to log.

.. code-block:: yaml

    dataframes:
      - name: summary  # logs DataFrame in `summary_` attribute as a dataframe named "summary"
        df_attr: summary_

Features
--------

Define a single :ref:`rubicon_ml.Feature<library-reference-feature>`
for logging by providing the attribute ``name_attr`` containing the name of the feature to log
and optionally the attribute ``importance_attr`` containing the feature's importance.

Lists of features can be defined for logging with the attributes ``names_attr`` containing a
list of feature names to log and optionally ``importances_attr`` containing the corresponding
importances.

.. code-block:: yaml

    features:
      - names_attr: feature_names_in_  # for each value in the `feature_names_in_` attribute, logs a feature named that
                                       # value with the corresponding importance in the `feature_importances_` attribute
        importances_attr: feature_importances_
        optional: true
      - name_attr: target              # logs a feature named the value of the `target` attribute

Metrics
-------

Define a :ref:`rubicon_ml.Metric<library-reference-metric>`
for logging by providing a ``name`` for the logged metric and the attribute ``value_attr``
containing the metric value to log.

Metric values can also be extracted from the runtime environment. Replace ``value_attr`` with ``value_env`` to
leverage ``os.environ`` to read the metric value from the available environment variables.

.. code-block:: yaml

    metrics:
      - name: learned_attribute  # logs value in `learned_attribute_` attribute as a metric named "learned_attribute"
        value_attr: learned_attribute_
        optional: true
      - name: score              # logs value in `score_` attribute as a metric named "score"
        value_attr: score_
      - name: env_metric         # logs value in `METRIC` environment varibale as a metric named "env_metric"
        value_env: METRIC

Parameters
----------

Define a :ref:`rubicon_ml.Parameter<library-reference-parameter>`
for logging by providing a ``name`` for the logged parameter and the attribute ``value_attr``
containing the parameter value to log.

Parameter values can also be extracted from the runtime environment. Replace ``value_attr`` with ``value_env`` to
leverage ``os.environ`` to read the parameter value from the available environment variables.

.. code-block:: yaml

    parameters:
      - name: alpha      # logs value in `alpha` attribute as a parameter named "alpha"
        value_attr: alpha
      - name: gamma      # logs value in `gamma` attribute as a parameter named "gamma"
        value_attr: gamma
      - name: env_param  # logs value in `PARAMETER` environment varibale as a parameter named "env_param"
        value_env: PARAMETER

Optional attributes
===================

In some cases, the attribute containing the value to log may not always be set on the underlying object. A model
may have been trained on a dataset with no feature names, or perhaps some learned attributes are only learned
if certain parameters have certain values while fitting.

By default, schema logging will raise an exception if the attribute to be logged is not set. To suppress the errors
and simply move on, items in the ``artifacts``, ``dataframes``, ``features``, ``metrics``, ``parameters`` and
``schema`` lists may optionally contain a key ``optional`` with a **true** value.

The ``feature_names_in_`` and ``learned_attribute_`` attributes are both marked optional in the example schema
above to handle cases where no feature names were present in the training data and ``learned_attribute_`` was
not learned:

.. code-block:: yaml

    features:
      - names_attr: feature_names_in_
        importances_attr: feature_importances_
        optional: true     # will not error if `feature_importances_` attribute is not set
      - name_attr: target  # **will** error if `target` attribute is not set
    metrics:
      - name: learned_attribute
        value_attr: learned_attribute_
        optional: true     # will not error if `learned_attribute_` attribute is not set

**Note:** Optional items in ``artifacts``, ``dataframes``, ``features``, and ``schema`` will omit the associated
entity from logging entirely if an optional attribute is not set. Optional items in ``metrics`` and ``parameters``
will log the associated entity with the given name and a value of **None** if an optional attribute is not set.

Nested schema
=============

The following is a complete YAML representation of the ``Optimizer`` object's schema:

.. code-block:: yaml

    name: my_model__Optimizer
    verison: 1.0.0

    metrics:
      - name: optimized
        value_attr: optimized_

To apply another schema to one of the attributes of the original object, provide the schema ``name``
to be retrieved via ``registry.get_schema`` and the attribute ``attr`` containing the
object to apply the schema to.

.. code-block:: yaml

    schema:
      - name: my_model__Optimizer  # logs a metric according to the above schema using the object in `optimizer`
      - attr: optimizer

**Note:** Nested schema will add the logged entities to the original experiment created by the parent schema,
not a new experiment. Nested schema cannot have names that conflict with the entites logged by the parent
schema.

The complete schema now looks like this and will log an additional metric ``optimized`` as defined by the
``Optimizer`` schema to the original experiment:

.. code-block:: yaml

    name: my_model__Model
    verison: 1.0.0
    
    compatibility:
      pandas:
        max_version:
        min_version: 1.0.5
    docs_url: https://my-docs.com/my-model/Model.html
    
    artifacts:
      - self
      - name: optimizer
        data_object_attr: optimizer
    dataframes:
      - name: summary
        df_attr: summary_
    features:
      - names_attr: feature_names_in_
        importances_attr: feature_importances_
        optional: true
      - name_attr: target
    metrics:
      - name: learned_attribute
        value_attr: learned_attribute_
        optional: true
      - name: score
        value_attr: score_
      - name: env_metric
        value_env: METRIC
    parameters:
      - name: alpha
        value_attr: alpha
      - name: gamma
        value_attr: gamma
      - name: env_param
        value_env: PARAMETER
    schema:
      - name: my_model__Optimizer
      - attr: optimizer

Hierarchical schema
===================

Some objects may contain a list of other objects that are already represented by a scehma, like
a feature eliminator or hyperparameter optimizer that trained multiple iterations of an underlying model
object.

The ``children`` key can be provided to log each of these underlying objects to a **new experiment**. This
means that a single call to ``project.log_with_schema`` will log **1+n** experiments to ``project`` where
**n** is the number of objects in the list specified by ``children``.

Within the ``children`` key, provide the schema ``name`` for the children objects to be retrieved via
``registry.get_schema`` and the attribute ``attr`` containing the list of child objects.

.. code-block:: yaml

    children:
      - name: my_model__Optimizer  # defines the children's schema
      - attr: optimizers           # logs an experiment according to the schema for each object in `optimizers`

If we replace the nested schema from the previous example with a list of children that adhere to the same
``Optimizer`` schema, the complete schema now looks like this. It will log a single experiment for ``Model``
containing all the information in the original ``Model`` schema, as well as an additional experiment as
defined by the ``Optimizer`` schema for each of the objects in ``Model``'s ``optimizers`` list.

.. code-block:: yaml

    name: my_model__Model
    verison: 1.0.0
    
    compatibility:
      pandas:
        max_version:
        min_version: 1.0.5
    docs_url: https://my-docs.com/my-model/Model.html
    
    artifacts:
      - self
      - name: optimizer
        data_object_attr: optimizer
    children:
      - name: my_model__Optimizer
      - attr: optimizers
    dataframes:
      - name: summary
        df_attr: summary_
    features:
      - names_attr: feature_names_in_
        importances_attr: feature_importances_
        optional: true
      - name_attr: target
    metrics:
      - name: learned_attribute
        value_attr: learned_attribute_
        optional: true
      - name: score
        value_attr: score_
      - name: env_metric
        value_env: METRIC
    parameters:
      - name: alpha
        value_attr: alpha
      - name: gamma
        value_attr: gamma
      - name: env_param
        value_env: PARAMETER

Extending a schema
==================

Consider an extension of ``Model`` named ``NewModel``:

.. code-block:: python

    class NewModel(Model):
        def __init__(self, alpha=1e-3, gamma=1e-3, delta=1e-3):
            super().__init__(alpha=alpha, gamma=gamma)
    
            self.delta = delta
    
        def fit(self, X, y):
            super().fit(X, y)
    
            self.other_learned_attribute_ = self.delta * self.learned_attribute_
    
            return self

To extend an existing schema, provide the name of the schema to extend as the
``extends`` key's value after the new schema's name. This new schema will log everything
in the schema represented by ``extends`` plus any additional values.

.. code-block:: yaml

    name: my_model__NewModel
    extends: my_model__Model
    verison: 1.0.0

The following is a complete YAML representation of the ``NewModel`` object's schema.
This schema will log everything that the ``Model`` schema would with the addition of the
``other_learned_attribute`` metric and ``delta`` parameter from ``NewModel``.

.. code-block:: yaml

    name: my_model__NewModel
    extends: my_model__Model
    verison: 1.0.0
    
    compatibility:
      pandas:
        max_version:
        min_version: 1.0.5
    docs_url: https://my-docs.com/my-model/NewModel.html
    
    metrics:
      - name: other_learned_attribute
        value_attr: other_learned_attribute_
    parameters:
      - name: delta
        value_attr: delta

To see an extended schema in action, check out the "Register a custom
schema" section.
