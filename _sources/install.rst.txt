.. _install:

Install
=======

``rubicon`` is available to install via ``conda`` and ``pip``:

.. code-block:: console

    conda install rubicon-ml

.. code-block:: console

    pip install rubicon-ml

**Extras**

``rubicon`` has a few optional extras:

The ``ui`` extra installs the requirements necessary for using ``rubicon``'s visualization tools.
For a preview, take a look at the **dashboard** section of the :ref:`quick look<quick-look>`.

.. code-block:: console

    conda install rubicon-ml[ui]

.. code-block:: console

    pip install rubicon-ml[ui]

The ``prefect`` extra installs the requirements necessary for using the `Prefect <https://prefect.io>`_ 
tasks in the ``rubicon.workflow`` module. Take a look at the **Prefect integration** :ref:`example<examples>` 
to see ``rubicon`` integrated into a simple Prefect flow.

.. code-block:: console

    conda install rubicon-ml[prefect]

.. code-block:: console

    pip install rubicon-ml[prefect]

To install all extra modules, use the ``all`` extra.

.. code-block:: console

    conda install rubicon-ml[all]

.. code-block:: console

    pip install rubicon-ml[all]
