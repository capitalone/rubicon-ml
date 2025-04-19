Contributing
************

Thanks for helping us build Rubicon!

Cloning the Repository
======================

Make a fork of the rubicon-ml repo and clone the fork:

.. code-block:: shell

   git clone https://github.com/<your-github-username>/rubicon-ml
   cd rubicon-ml

You may want to add ``https://github.com/capitalone/rubicon-ml/``
as an upstream remote:

.. code-block:: shell

   git remote add upstream https://github.com/capitalone/rubicon-ml

Creating a Development Environment
==================================

We have a ``conda`` environment YAML file with all the necessary dependencies
at the root of the repository.

.. code-block:: shell

   conda create -n rubicon-ml-dev python=3.11
   conda activate rubicon-ml-dev

Building ``rubicon_ml``
=======================

After you've cloned the repository, use ``pip`` to install ``rubicon_ml`` locally:

.. code-block:: shell

   python -m pip install -e './[dev]'

Style
=====

``rubicon_ml`` uses `black <http://black.readthedocs.io/en/stable/>`_ for formatting,
`flake8 <http://flake8.pycqa.org/en/latest/>`_ for linting, and
`isort <https://pycqa.github.io/isort/>`_ for standardizing imports. If you installed
``rubicon_ml`` using ``conda``, these tools will already be in your environment.

.. code-block:: shell

    black rubicon_ml tests
    flake8 rubicon_ml tests
    isort -rc rubicon_ml tests

Install and configure `pre-commit <https://pre-commit.com/>`_ to automatically run
each of these tools before committing. Once installed, run ``pre-commit install``
to set up the git hooks.

Running Tests
=============

``rubicon_ml`` uses `pytest <https://docs.pytest.org/en/latest/>`_ for testing.
Run the tests from the root ``rubicon_ml`` directory as follows:

.. code-block:: shell

    python -m pytest

Documentation
=============

We follow `numpydoc <http://numpydoc.readthedocs.io/en/latest/format.html>`_
formatting for our docstrings. This lets us use ``sphinx`` to automatically
generate a library reference.

We also use `nbsphinx <https://nbsphinx.readthedocs.io/>`_ to render our
example notebooks directly from our repo. To reduce the complexity of our
documentation builds, we only commit executed notebooks to the repo so
``nbsphinx`` doesn't have to spend time executing them itself.

To build the documentation locally, use the provided Makefile:

.. code-block:: shell

   make clean html

The newly built documentation can be opened in a browser.

.. code-block:: shell

   open ./build/html/index.html

Never commit built documentation code directly, only the source.
Our ``.gitignore`` should handle keeping built docs out of the repo, and
our CICD handles deploying newly committed documentation.
