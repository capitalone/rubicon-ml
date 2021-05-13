.. _rubicon:

Purpose
=======

rubicon-ml is a data science tool that captures and stores model training and
execution information, like parameters and outcomes, in a repeatable and
searchable way. Its ``git`` integration associates these inputs and outputs
directly with the model code that produced them to ensure full auditability and
reproducibility for both developers and stakeholders alike. And while experimenting,
the dashboard makes it easy to explore, filter, visualize, and share
recorded work.

Visit the :ref:`glossary<glossary>` to explore the terminology.

Components
==========

rubicon-ml is composed of three parts:

* A Python library for storing and retrieving model inputs, outputs, and
  analyses to filesystems that’s powered by
  fsspec_
* A dashboard for exploring, comparing, and visualizing logged data built with
  dash_
* And a process for sharing a selected subset of logged data with collaborators
  or reviewers that leverages intake_

To see each of these parts in action, visit the `Quick Look`_!

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

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Getting Started:

   quick-look
   glossary.rst
   install.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: User Guide:

   library_reference.rst
   examples.rst
   integrations.rst
   dashboard.rst
   faqs.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Community:

   contributing.rst
   Changelog<https://github.com/capitalone/rubicon/releases>
   Feedback<https://github.com/capitalone/rubicon/issues/new/choose>
   GitHub<https://github.com/capitalone/rubicon>

.. _fsspec: https://filesystem-spec.readthedocs.io/en/latest/?badge=latest
.. _dash: https://dash.plotly.com/
.. _intake: https://intake.readthedocs.io/en/latest/
.. _Quick Look: ./quick-look.html
