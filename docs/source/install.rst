.. _install:

Install
*******

``rubicon`` is available to install via ``conda`` and ``pip``. When using ``conda``,
make sure to set the channel to ``conda-forge``. You should only need to do this once:

.. code-block:: console

    conda config --add channels conda-forge

then...

.. code-block:: console

    conda install rubicon-ml

Alternatively:

.. code-block:: console

    pip install rubicon-ml

Extras
======

``rubicon`` has a few optional extras if you're installing with ``pip`` (these extras are all
installed by default when using ``conda``).

|

The ``ui`` extra installs the requirements necessary for using ``rubicon``'s visualization tools.
For a preview, take a look at the **dashboard** section of the :ref:`quick look<quick-look>`.

.. code-block:: console

    pip install rubicon-ml[ui]

|

The ``prefect`` extra installs the requirements necessary for using the `Prefect <https://prefect.io>`_ 
tasks in the ``rubicon.workflow`` module. Take a look at the **Prefect integration** :ref:`example<examples>` 
to see ``rubicon`` integrated into a simple Prefect flow.

.. code-block:: console

    pip install rubicon-ml[prefect]

|

To install all extra modules, use the ``all`` extra.

.. code-block:: console

    pip install rubicon-ml[all]
