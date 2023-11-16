.. _rubicon:

Welcome to the rubicon-ml Docs!
*******************************

rubicon-ml is a data science tool that captures and stores model training and
execution information, like parameters and outcomes, in a repeatable and
searchable way. Its ``git`` integration associates these inputs and outputs
directly with the model code that produced them to ensure full auditability and
reproducibility for both developers and stakeholders alike. And while experimenting,
the dashboard makes it easy to explore, filter, visualize, and share
recorded work.

Visit the :ref:`glossary<glossary>` to explore rubicon-ml's terminology or get
started with the first example in our `quick look`_!

Components
==========

rubicon-ml's core functionality is broken down into three parts...

* `Logging`_: organize, store, and retrieve model inputs and outputs with various
  backend storage options - powered by fsspec_
* `Sharing`_: share a selected subset of logged data with collaborators or reviewers
  - powered by intake_
* `Visualizing`_: explore and compare logged model metadata with the dashboard and
  other widgets - powered by dash_

Workflow
========

Use ``rubicon_ml`` to capture model inputs and outputs over time. 
It easily integrates into existing Python models or pipelines and supports both
concurrent logging (so multiple experiments can be logged in parallel) and
asynchronous communication with S3 (so network reads and writes won’t block).

Meanwhile, periodically review the logged data within the dashboard to
steer the model tweaking process in the right direction. The dashboard lets you
quickly spot trends by exploring and filtering your logged results and
visualizes how the model inputs impacted the model outputs.

When the model is ready for review, rubicon-ml makes it easy to share specific
subsets of the data with model reviewers and stakeholders, giving them the
context necessary for a complete model review and approval.

.. _install:

Install
=======

rubicon-ml is available to install via ``conda`` and ``pip``. When using ``conda``,
make sure to set the channel to ``conda-forge``. You should only need to do this once:

.. code-block:: console

    conda config --add channels conda-forge

then...

.. code-block:: console

    conda install rubicon-ml

Alternatively:

.. code-block:: console

    pip install rubicon-ml

.. warning::
    rubicon-ml version 0.3.0+ requires Python version 3.8+

Extras
------

rubicon-ml has a few optional extras if you're installing with ``pip`` (these extras are all
installed by default when using ``conda``).

The ``s3`` extra installs ``s3fs`` to enable logging to Amazon S3.

.. code-block:: console

    pip install rubicon-ml[s3]

The ``viz`` extra installs the requirements necessary for using the visualization tools.
For a preview, take a look at the :ref:`Visualizations<Visualizations>` section of the docs.

.. code-block:: console

    pip install rubicon-ml[viz]

The ``prefect`` extra installs the requirements necessary for using the `Prefect <https://prefect.io>`_
tasks in the ``rubicon_ml.workflow`` module. Take a look at the `Prefect integration`_
to see the library integrated into a simple Prefect flow.

.. code-block:: console

    pip install rubicon-ml[prefect]

To install all extra modules, use the ``all`` extra.

.. code-block:: console

    pip install rubicon-ml[all]

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Quick Look

   quick-look/logging-experiments
   quick-look/sharing-experiments
   quick-look/visualizing-experiments

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Tutorials

   tutorials/failure-modes
   logging-examples/logging-training-metadata
   logging-examples/logging-plots
   logging-examples/logging-concurrently
   logging-examples/log-with-schema
   logging-examples/tagging
   logging-examples/rubiconJSON-querying
   visualizations.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: How to...

   logging-examples/logging-experiment-failures
   integrations/integration-git
   integrations/integration-prefect-workflows
   integrations/integration-sklearn
   logging-examples/logging-feature-plots
   logging-examples/multiple-backend
   logging-examples/manage-experiment-relationships
   logging-examples/register-custom-schema
   logging-examples/set-schema
   logging-examples/visualizing-logged-dataframes

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Explanation

   glossary.rst
   faqs.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Reference

   api_reference.rst
   schema-representation.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Community

   contributing.rst
   contribute-schema.rst
   Changelog<https://github.com/capitalone/rubicon-ml/releases>
   Feedback<https://github.com/capitalone/rubicon-ml/issues/new/choose>
   GitHub<https://github.com/capitalone/rubicon-ml>

.. _fsspec: https://filesystem-spec.readthedocs.io/en/latest/?badge=latest
.. _dash: https://dash.plotly.com/
.. _intake: https://intake.readthedocs.io/en/latest/
.. _quick look: ./quick-look/logging-experiments.html
.. _Logging: ./quick-look/logging-experiments.html
.. _Sharing: ./quick-look/sharing-experiments.html
.. _Visualizing: ./quick-look/visualizing-experiments.html
.. _Prefect integration: ./integrations/integration-prefect-workflows.html
