.. _rubicon:

=======
Rubicon
=======

Rubicon is a model development and governance library that can integrate directly into your Python code. It's
designed for both model developers and model reviewers and offers the following features:

    * a Python library for storing and retrieving model inputs, ouputs, and analyses to filesystems (local, S3)
    * a dashboard for exploring, comparing, and visualizing logged data
    * a process for sharing a selected subset of logged data with collaborators and reviewers

Rubicon is designed to enforce best practices, like automatically linking your logged experiments to
the corresponding model code through Rubicon's git integration.

Rubicon is also designed to be lightweight and performant. It supports concurrent logging, so you can log
multiple experiments at a time and it also supports asynchronous communication with S3, so your network
reads and writes won't block.

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
