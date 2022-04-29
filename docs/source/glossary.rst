.. _glossary:

The Building Blocks
*******************

Project (:ref:`rubicon_ml.Project<library-reference-project>`)
============================================================================

A **project** is a collection of **experiments**, **dataframes**, and **artifacts**
identified by a unique name.

.. code-block:: python

  from rubicon_ml import Rubicon

  rubicon = Rubicon(persistence="memory")
  project = rubicon.create_project(name="Building Blocks")

Now that we have a project, we can begin logging important information to our project. 

Experiment (:ref:`rubicon_ml.Experiment<library-reference-experiment>`)
=======================================================================

An **experiment** represents a model run and is identified by its ``created_at`` time.
It can have **metrics**, **parameters**, **features**, **dataframes**, and **artifacts**
logged to it.

An **experiment** is logged to a **project**.

.. code-block:: python

  experiment = project.log_experiment(tags=["glossary"])

Parameter (:ref:`rubicon_ml.Parameter<library-reference-parameter>`)
====================================================================

A **parameter** is an input to an **experiment** (model run) that depends on the type
of model being used. It affects the model's predictions.

For example, if you were using a random forest classifier, ``n_estimators`` (the number
of trees in the forest) could be a **parameter**.


.. code-block:: python

  experiment.log_parameter("n_estimators", 20)

Feature (:ref:`rubicon_ml.Feature<library-reference-feature>`)
==============================================================

A **feature** is an input to an **experiment** (model run) that's an independent,
measurable property of a phenomenon being observed. It affects the model's predictions.

For example, consider a model that predicts how likely a customer is to pay back a loan.
Possible **features** could be ``year`` or ``credit score``.

A **feature** is logged to an **experiment**.

.. code-block:: python

  experiment.log_feature("year", importance=0.125)
  experiment.log_feature("credit score", importance=0.250)

Metric (:ref:`rubicon_ml.Metric<library-reference-metric>`)
===========================================================

A **metric** is a single-value output of an **experiment** that helps evaluate the
quality of the model's predictions.
    
It can be either a ``score`` (value to maximize) or a ``loss`` (value to minimize).

A **metric** is logged to an **experiment**.

.. code-block:: python

  experiment.log_metric("accuracy", 0.933, directionality="score")

Dataframe (:ref:`rubicon_ml.Dataframe<library-reference-dataframe>`)
====================================================================

A **dataframe** is a two-dimensional, tabular dataset with labeled axes (rows and
columns) that provides value to the model developer and/or reviewer when visualized. 

For example, confusion matrices, feature importance tables and marginal residuals can
all be logged as a **dataframe**.

A **dataframe** is logged to a **project** or an **experiment**.

.. code-block:: python

  import pandas as pd

  confusion_matrix = pd.DataFrame(
      [[5, 0, 0], [0, 5, 1], [0, 0, 4]],
      columns=["x", "y", "z"],
  )
  dataframe=experiment.log_dataframe(confusion_matrix)

Artifact (:ref:`rubicon_ml.Artifact<library-reference-artifact>`)
=================================================================

An **artifact** is a catch-all for any other type of data that can be logged to a file.

For example, a snapshot of a trained model (.pkl) can be logged to the **experiment**
created during its run. Or, a base model for the model in development can be logged to
a **project** when leveraging transfer learning.

An **artifact** is logged to a **project** or an **experiment**.

.. code-block:: python

  plot = dataframe.plot("bar")
  plot=plot.to_image(format="png")
  experiment.log_artifact(name="bar",data_bytes=plot)

Conclusion
==========
Congrats! You have now completed the building blocks of logging with **Rubicon_ml**. First we created a project to **project** to collect all our information. Then we made an **experiment** to log our current model run infomation. Then we logged **parameter**, **feature**, **metric**, **dataframe** and **artifact** information from our current model run to that **experiment**. Now that we've taken a look at the building blocks of logging, please take a look at the logging examples up next. 
