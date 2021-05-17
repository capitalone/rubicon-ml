.. _library-reference:

Library Reference
*****************

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

.. _library-reference-async:

asynchronous
============
``rubicon_ml`` also exposes an asynchronous client that itself exposes
all the same functions as the standard client detailed above. The only
differences are that the asycnhronous client is **for S3 logging only**
and each function **returns a coroutine** rather than its standard
return value.

.. autoclass:: rubicon_ml.client.asynchronous.Rubicon

.. _library-reference-ui:

ui
==
The ``rubicon_ml`` dashboard can be launched directly from Python code
or the CLI.

.. autoclass:: rubicon_ml.ui.Dashboard
.. autofunction:: rubicon_ml.ui.Dashboard.run_server

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
