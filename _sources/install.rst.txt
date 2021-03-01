.. _install:

Install
=======

``rubicon`` is available to install via ``pip``:

.. code-block:: console

    pip install rubicon-ml

**Extras**

``rubicon`` has a few optional extras:

The ``ui`` extra installs the requirements necessary for using ``rubicon``'s visualization tools.
For a preview, take a look at the **dashboard** section of the :ref:`quick look<quick-look>`.

.. code-block:: console

    pip install rubicon-ml[ui]

The ``prefect`` extra installs the requirements necessary for using the `Prefect <https://prefect.io>`_ 
tasks in the ``rubicon.workflow`` module. Take a look at the **Prefect integration** :ref:`example<examples>` 
to see ``rubicon`` integrated into a simple Prefect flow.

.. code-block:: console

    pip install rubicon-ml[prefect]

To install all extra modules, use the ``all`` extra.

.. code-block:: console

    pip install rubicon-ml[all]
