.. _api-reference:

API Reference
*************

Rubicon
=======
.. autoclass:: rubicon_ml.Rubicon
   :members:
   :inherited-members:

.. _library-reference-project:

Project
=======
.. autoclass:: rubicon_ml.Project
   :members:
   :inherited-members:

.. _library-reference-experiment:

Experiment
==========
.. autoclass:: rubicon_ml.Experiment
   :members:
   :inherited-members:

.. _library-reference-parameter:

Parameter
=========
.. autoclass:: rubicon_ml.Parameter
   :members:
   :inherited-members:

.. _library-reference-feature:

Feature
=======
.. autoclass:: rubicon_ml.Feature
   :members:
   :inherited-members:

.. _library-reference-metric:

Metric
======
.. autoclass:: rubicon_ml.Metric
   :members:
   :inherited-members:

.. _library-reference-dataframe:

Dataframe
=========
.. autoclass:: rubicon_ml.Dataframe
   :members:
   :inherited-members:

.. _library-reference-artifact:

Artifact
========
.. autoclass:: rubicon_ml.Artifact
   :members:
   :inherited-members:

.. _library-reference-publish:

exception_handling
==================

.. autofunction:: rubicon_ml.set_failure_mode

publish
=======
``rubicon_ml`` leverages ``intake`` to easily share sets of experiments.

.. autofunction:: rubicon_ml.publish

.. _library-reference-rubiconJSON:

RubiconJSON
===========
.. autoclass:: rubicon_ml.RubiconJSON
   :members: search

.. _library-reference-sklearn:

schema
======

.. automodule:: rubicon_ml.schema.logger
   :members:

.. automodule:: rubicon_ml.schema.registry
   :members:

sklearn
=======
``rubicon_ml`` offers direct integration with **Scikit-learn** via our
own pipeline object.

.. autoclass:: rubicon_ml.sklearn.RubiconPipeline
   :members:

.. autoclass:: rubicon_ml.sklearn.FilterEstimatorLogger
   :members:

.. autofunction:: rubicon_ml.sklearn.pipeline.make_pipeline

.. _library-reference-viz:

viz
===
``rubicon_ml`` offers visualization leveraging **Dash** and **Plotly**.
Each of the following classes are standalone widgets.

.. autoclass:: rubicon_ml.viz.DataframePlot
   :members: serve, show

.. autoclass:: rubicon_ml.viz.ExperimentsTable
   :members: serve, show

.. autoclass:: rubicon_ml.viz.MetricCorrelationPlot
   :members: serve, show

.. autoclass:: rubicon_ml.viz.MetricListsComparison
   :members: serve, show

Widgets can be combined into an interactive dashboard.

.. autoclass:: rubicon_ml.viz.Dashboard
   :members: serve, show

.. _library-reference-workflow:

workflow.prefect
================
``rubicon_ml`` contains wrappers for the workflow management engine
**Prefect**. These **tasks** represent a Prefect-ified ``rubicon_ml``
client.

.. automodule:: rubicon_ml.workflow.prefect
.. autofunction:: rubicon_ml.workflow.prefect.create_experiment_task
.. autofunction:: rubicon_ml.workflow.prefect.get_or_create_project_task
.. autofunction:: rubicon_ml.workflow.prefect.log_artifact_task
.. autofunction:: rubicon_ml.workflow.prefect.log_dataframe_task
.. autofunction:: rubicon_ml.workflow.prefect.log_feature_task
.. autofunction:: rubicon_ml.workflow.prefect.log_metric_task
.. autofunction:: rubicon_ml.workflow.prefect.log_parameter_task
