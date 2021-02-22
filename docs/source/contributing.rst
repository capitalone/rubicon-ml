Contributing
************

Thanks for helping us build Rubicon!

Cloning the Repository
======================

Make a fork of the ``rubicon`` repo and clone the fork:

.. code-block:: shell

   git clone https://github.com/<your-github-username>/rubicon
   cd rubicon

You may want to add ``https://github.com/capitalone/rubicon/``
as an upstream remote:

.. code-block:: shell

   git remote add upstream https://github.com/capitalone/rubicon

Creating a Development Environment
==================================

We have a ``conda`` environment YAML file with all the necessary dependencies
in the ``ci`` directory.

.. code-block:: shell

   conda env create -f ci/environment.yml

Building ``rubicon``
====================

After you've cloned the repository, use ``pip`` to install ``rubicon`` locally:

.. code-block:: shell

   python -m pip install -e .

Style
=====

``rubicon`` uses `black <http://black.readthedocs.io/en/stable/>`_ for formatting,
`flake8 <http://flake8.pycqa.org/en/latest/>`_ for linting, and
`isort <https://pycqa.github.io/isort/>`_ for standardizing imports. If you installed
``rubicon`` using ``conda``, these tools will already be in your environment.

.. code-block:: shell

    black rubicon tests
    flake8 rubicon tests
    isort -rc rubicon tests

Install and configure `pre-commit <https://pre-commit.com/>`_ to automatically run
each of these tools before committing. Once installed, run ``pre-commit install``
to set up ``rubicon``'s git hooks.

Running Tests
=============

``rubicon`` uses `pytest <https://docs.pytest.org/en/latest/>`_ for testing.
Run the tests from the root ``rubicon`` directory as follows:

.. code-block:: shell

    python -m pytest

Documentation
=============

We follow `numpydoc <http://numpydoc.readthedocs.io/en/latest/format.html>`_
formatting for our docstrings. This lets us use ``sphinx`` to automatically
generate a library reference.

To build the docs locally, use the environment YAML file in the ``docs``
directory to create a new ``conda`` environment with all necessary tools:

.. code-block:: shell

   conda env create -f docs/docs-environment.yml

Activate the new environment and use the ``build-docs.sh`` script in the
``docs`` directory to build the documentation locally. The newly built
documentation will open in a browser window.

.. code-block:: shell

   conda activate rubicon-docs
   sh build-docs.sh

Never commit built documentation code directly to Rubicon. Our CICD handles
deploying documentation.
