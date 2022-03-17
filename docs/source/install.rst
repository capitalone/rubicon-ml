.. _install:

Install
*******

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
======

rubicon-ml has a few optional extras if you're installing with ``pip`` (these extras are all
installed by default when using ``conda``).

The ``ui`` extra installs the requirements necessary for using the visualization tools.
For a preview, take a look at the :ref:`Visualizations<Visualizations>` section of the docs.

.. code-block:: console

    pip install rubicon-ml[ui]

The ``prefect`` extra installs the requirements necessary for using the `Prefect <https://prefect.io>`_ 
tasks in the ``rubicon_ml.workflow`` module. Take a look at the Prefect integration :ref:`example<integrations>`
to see the library integrated into a simple Prefect flow.

.. code-block:: console

    pip install rubicon-ml[prefect]

To install all extra modules, use the ``all`` extra.

.. code-block:: console

    pip install rubicon-ml[all]
