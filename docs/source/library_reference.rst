.. _library-reference:

Library Reference
*****************

Rubicon
=======
.. autoclass:: rubicon.Rubicon
   :members:
   :inherited-members:

.. _library-reference-project:

Project
=======
.. autoclass:: rubicon.Project
   :members:
   :inherited-members:

.. _library-reference-experiment:

Experiment
==========
.. autoclass:: rubicon.Experiment
   :members:
   :inherited-members:

.. _library-reference-parameter:

Parameter
=========
.. autoclass:: rubicon.Parameter
   :members:
   :inherited-members:

.. _library-reference-feature:

Feature
=======
.. autoclass:: rubicon.Feature
   :members:
   :inherited-members:

.. _library-reference-metric:

Metric
======
.. autoclass:: rubicon.Metric
   :members:
   :inherited-members:

.. _library-reference-dataframe:

Dataframe
=========
.. autoclass:: rubicon.Dataframe
   :members:
   :inherited-members:

.. _library-reference-artifact:

Artifact
========
.. autoclass:: rubicon.Artifact
   :members:
   :inherited-members:

.. _library-reference-async:

asynchronous.Rubicon
====================
``rubicon`` also exposes an asynchronous client that itself exposes
all the same functions as the standard client detailed above. The only
differences are that the asycnhronous client is **for S3 logging only**
and each function **returns a coroutine** rather than its standard
return value.

.. autoclass:: rubicon.client.asynchronous.Rubicon

.. _library-reference-ui:

ui.Dashboard
============
The ``rubicon`` dashboard can be launched directly from python code
in addition to the CLI.

.. autoclass:: rubicon.ui.Dashboard
.. autofunction:: rubicon.ui.Dashboard.run_server

.. _library-reference-workflow:

workflow.prefect
================
``rubicon`` contains wrappers for the workflow management engine
**Prefect**. These **tasks** represent a Prefect-ified ``rubicon``
client.

.. automodule:: rubicon.workflow.prefect
.. autofunction:: rubicon.workflow.prefect.create_experiment_task
.. autofunction:: rubicon.workflow.prefect.get_or_create_project_task
.. autofunction:: rubicon.workflow.prefect.log_artifact_task
.. autofunction:: rubicon.workflow.prefect.log_dataframe_task
.. autofunction:: rubicon.workflow.prefect.log_feature_task
.. autofunction:: rubicon.workflow.prefect.log_metric_task
.. autofunction:: rubicon.workflow.prefect.log_parameter_task
