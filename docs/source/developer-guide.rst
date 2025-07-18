Developer Guide
***************

Thanks for helping us build rubicon-ml! This guide will help you get set up to start
contributing to the project.

Clone the Repository
====================

Make a `fork <https://github.com/capitalone/rubicon-ml/fork>`_ of the rubicon-ml repo
and clone the fork:

.. code-block:: shell

   git clone https://github.com/<your-github-username>/rubicon-ml
   cd rubicon-ml

You may want to add ``https://github.com/capitalone/rubicon-ml/`` as an upstream
remote:

.. code-block:: shell

   git remote add upstream https://github.com/capitalone/rubicon-ml

Create a Development Environment
================================

We recommended setting up a development environment with
[``uv``](https://github.com/astral-sh/uv):

.. code-block:: shell

   uv venv --python 3.13

After you've cloned the repository and created a virtual environment, use ``uv`` to
install rubicon-ml and its developer dependencies locally:

.. code-block:: shell

   uv sync --extra dev

All future `uv` invocations from the rubicon-ml repository will leverage the new
environment.

Run the Tests
=============

``rubicon_ml`` uses `pytest <https://docs.pytest.org/en/latest/>`_ for testing. Run
the tests from the root ``rubicon_ml`` directory as follows:

.. code-block:: shell

    uv run pytest

Build the Documentation
=======================

We follow `numpydoc <http://numpydoc.readthedocs.io/en/latest/format.html>`_
formatting for our docstrings. This lets us use ``sphinx`` to automatically
generate a library reference.

We also use `nbsphinx <https://nbsphinx.readthedocs.io/>`_ to render our example
notebooks directly from our repo. To reduce the complexity of our documentation
builds, we only commit executed notebooks to the repo so ``nbsphinx`` doesn't have
to spend time executing them itself.

To build the documentation locally, use the provided Makefile:

.. code-block:: shell

   cd docs/
   uv run make clean html

The newly built documentation can be opened in a browser locally.

.. code-block:: shell

   open ./build/html/index.html

Never commit built documentation code directly, only the source.
Our ``.gitignore`` should handle keeping built docs out of the repo, and
our CICD handles deploying newly committed documentation.

Check the Style
===============

``rubicon_ml`` uses `ruff <https://docs.astral.sh/ruff/>`_ for linting and formatting.
To check and update the code style, run:

.. code-block:: shell

    uv run ruff check --fix
    uv run ruff format

Install and configure `pre-commit <https://pre-commit.com/>`_ to automatically run
each of these tools before committing. Once installed, run ``uv run pre-commit install``
to set up the git hooks and ``uv run pre-commit run --all-files`` to run the checks.
To skip these checks, run ``git commit --no-verify``.

Cut a Release
=============

To release a new version of rubicon-ml, follow these steps to open a release PR. Then
rubicon-ml's CICD will handle the rest.

First, update the project's version number with ``bumpver``:

.. code-block:: shell

   uv run bumpver update --patch

Other valid options are ``--minor`` and ``--major``. This will update the version
accordingly, following the ``MAJOR.MINOR.PATCH`` pattern.

Next, commit and push your changes. Then open a PR using the release pull request
template. Follow the instructions in the template to populate the PR body. This will
become the contents of the release notes for the new version, so make sure it is
accurate. Once the PR is approved and merged, the CICD will automatically publish the
new version of the package and create a new release on GitHub with the appropriate tag.
