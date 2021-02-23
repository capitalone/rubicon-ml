.. _install_os:

Install OS
==========

``rubicon`` is available to install via ``conda`` and ``pip``:

.. code-block:: console

    conda install rubicon

.. code-block:: console

    pip install rubicon

**Extras**

``rubicon`` has a few optional extras:

The ``ui`` extra installs the requirements necessary for using ``rubicon``'s visualization tools.
For a preview, take a look at the **dashboard** section of the :ref:`quick look<quick-look>`.

.. code-block:: console

    conda install rubicon[ui]

.. code-block:: console

    pip install rubicon[ui]

The ``prefect`` extra installs the requirements necessary for using the `Prefect <https://prefect.io>`_ 
tasks in the ``rubicon.workflow`` module. Take a look at the **Prefect integration** :ref:`example<examples>` 
to see ``rubicon`` integrated into a simple Prefect flow.

.. code-block:: console

    conda install rubicon[prefect]

.. code-block:: console

    pip install rubicon[prefect]

To install all extra modules, use the ``all`` extra.

.. code-block:: console

    conda install rubicon[all]

.. code-block:: console

    pip install rubicon[all]
