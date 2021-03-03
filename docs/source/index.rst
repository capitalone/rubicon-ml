.. _rubicon:

=======
Rubicon
=======

Purpose
=======

Rubicon is a data science tool that captures and stores model training and
execution information, like parameters and outcomes, in a repeatable and
searchable way. Rubicon's git integration associates these inputs and outputs
directly with the model code that produced them to ensure full auditability and
reproducibility for both developers and stakeholders alike. And the Rubicon
dashboard makes it easy to explore, filter, visualize, and share recorded work.

Components
==========

Rubicon is composed of three parts:

* A Python library for storing and retrieving model inputs, outputs, and
  analyses to filesystems that’s powered by
  fsspec_
* A dashboard for exploring, comparing, and visualizing logged data built with
  dash_
* And a process for sharing a selected subset of logged data with collaborators
  or reviewers that leverages intake_

Workflow
========

Use the Rubicon library to capture model inputs and outputs overtime. It can be
easily integrated into existing Python models or pipelines and supports both
concurrent logging (so multiple experiments can be logged in parallel) and
asynchronous communication with S3 (so network reads and writes won’t block).

Meanwhile, periodically review the logged data within the Rubicon dashboard to
steer the model tweaking process in the right direction. The dashboard lets you
quickly spot trends by exploring and filtering your logged results and
visualizes how the model inputs impacted the model outputs.

After the model has met its goals, use the collobaration process to share the
relevant subsets of logged data with model reviewers or stakeholders, giving
everyone the context necessary for a peace of mind model review and approval.

---

Visit the :ref:`glossary<glossary>` to explore Rubicon's terminology. And to see Rubicon
in action, visit the :ref:`quick look<quick-look>`!

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Getting Started:

   quick_look.rst
   glossary.rst
   install.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: User Guide:

   library_reference.rst
   examples.rst
   dashboard.rst
   faqs.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Community:

   contributing.rst
   Feedback<https://github.com/capitalone/rubicon/issues/new/choose>
   GitHub<https://github.com/capitalone/rubicon>
   Changelog<https://github.com/capitalone/rubicon/releases>

.. _fsspec: https://filesystem-spec.readthedocs.io/en/latest/?badge=latest
.. _dash: https://dash.plotly.com/
.. _intake: https://intake.readthedocs.io/en/latest/