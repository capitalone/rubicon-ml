.. _backend-repositories:

Available Backend Repositories
******************************

rubicon-ml uses pluggable storage backends powered by fsspec_. The backend is
selected via the ``persistence`` and ``root_dir`` keyword arguments when
instantiating the :class:`~rubicon_ml.Rubicon` client. All backends expose the
same logging API, so switching between them requires only changing how you
create the client — the rest of your code stays the same.

.. list-table:: Backend summary
   :header-rows: 1
   :widths: 20 20 25 35

   * - Backend
     - ``persistence``
     - ``root_dir``
     - Install
   * - Local filesystem
     - ``"filesystem"``
     - ``"/path/to/dir"``
     - *(included)*
   * - Amazon S3
     - ``"filesystem"``
     - ``"s3://bucket/prefix"``
     - ``pip install s3fs``
   * - In-memory
     - ``"memory"``
     - *(optional)*
     - *(included)*
   * - Weights & Biases
     - ``"wandb"``
     - *(unused)*
     - ``pip install wandb``


Local Filesystem
================

The local filesystem backend persists rubicon-ml data to a directory on the
machine where your code is running. It is the default backend and requires no
extra dependencies.

.. code-block:: python

    from rubicon_ml import Rubicon

    rubicon = Rubicon(persistence="filesystem", root_dir="/path/to/rubicon-root")

``root_dir`` must be an absolute or relative path to a directory. The directory
will be created if it does not exist. Data is written as JSON metadata files and
Parquet dataframes in a nested directory structure under ``root_dir``.

Any additional keyword arguments are passed through to the underlying
``fsspec.filesystem("file", ...)`` call via ``**storage_options``.

Amazon S3
=========

The S3 backend persists rubicon-ml data to a remote S3 bucket. It uses the same
``persistence="filesystem"`` setting as the local backend — rubicon-ml detects
the ``s3://`` prefix in ``root_dir`` and selects the S3 repository
automatically.

Install ``s3fs`` extra to use the S3 backend:

.. code-block:: console

    pip install s3fs

Then create a client pointing at your bucket:

.. code-block:: python

    from rubicon_ml import Rubicon

    rubicon = Rubicon(
        persistence="filesystem",
        root_dir="s3://my-bucket/rubicon-root",
    )

AWS credentials are resolved in the standard order: environment variables
(``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``), the shared credentials
file (``~/.aws/credentials``), or an instance profile. You can also pass
credentials explicitly via ``storage_options``:

.. code-block:: python

    rubicon = Rubicon(
        persistence="filesystem",
        root_dir="s3://my-bucket/rubicon-root",
        profile="my-aws-profile",
    )

All extra keyword arguments are forwarded to ``s3fs.S3FileSystem``.

In-Memory
=========

The in-memory backend stores data in a virtual filesystem that lives entirely in
the current process's memory. It is intended for **testing and development** —
data will not survive between Python sessions.

.. code-block:: python

    from rubicon_ml import Rubicon

    rubicon = Rubicon(persistence="memory")

``root_dir`` is only required if you are interacting with a previously-created
in-memory filesystem.

.. _wandb-backend:

Weights & Biases
================

.. warning::

    The W&B backend is **experimental** and may contain breaking changes in
    future versions. If you encounter any bugs or missing features, please
    open an issue.

The W&B backend maps rubicon-ml concepts onto native Weights & Biases
primitives, so your experiment data is visible in both the rubicon-ml API and
the W&B web UI.

Setup
-----

Install the ``wandb`` package and authenticate:

.. code-block:: console

    pip install wandb

Set your API key as an environment variable:

.. code-block:: console

    export WANDB_API_KEY="your-api-key"

Alternatively, you can run ``wandb login`` which stores the key in
``~/.netrc``.

Basic usage
-----------

.. code-block:: python

    from rubicon_ml import Rubicon

    rubicon = Rubicon(persistence="wandb")

    project = rubicon.get_or_create_project("My Project")
    experiment = project.log_experiment()

    experiment.log_parameter("alpha", 0.1)
    experiment.log_metric("accuracy", 0.95)

The ``root_dir`` parameter is unused by the W&B backend.

Configuration
-------------

You can pass additional configuration when creating the client:

``entity``
    The W&B entity (username or team name) to read from and write to. When
    omitted, W&B uses the default entity from your local configuration.

``wandb_init_kwargs``
    A dictionary of additional keyword arguments forwarded to every
    ``wandb.init()`` call (e.g. ``{"mode": "offline"}``).

Pass these as ``storage_options``:

.. code-block:: python

    rubicon = Rubicon(
        persistence="wandb",
        entity="my-team",
        wandb_init_kwargs={"mode": "offline"},
    )

Concept mapping
---------------

rubicon-ml objects map to W&B primitives as follows:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - rubicon-ml
     - Weights & Biases
   * - Project
     - W&B Project
   * - Experiment
     - W&B Run
   * - Metric (value)
     - W&B Metric (``wandb.log``)
   * - Parameter (value)
     - W&B Config entry
   * - Feature (importance)
     - W&B Metric (``wandb.log``)
   * - Artifact
     - W&B Artifact (type ``"model"``)
   * - Dataframe
     - W&B Artifact (type ``"dataset"``) + W&B Table

In addition to these native representations, every entity's complete rubicon-ml
metadata is stored in the run's W&B Config under a private ``_rubicon_*`` key so
that it can be fully reconstructed when reading back through rubicon-ml.

Limitations
-----------

The W&B backend does not currently support every operation available in the
filesystem backends:

- **No project-level artifacts or dataframes.** These must be logged to an
  experiment (W&B run).
- **No** ``get_projects()`` **listing.** Use ``get_project(name)`` 
  or ``get_or_create_project(name)`` with a specific project name instead.

Transitioning from a filesystem backend
----------------------------------------

If you have been using a local or S3 filesystem backend and want to switch to
W&B, the change is straightforward — update the ``persistence`` argument:

.. code-block:: python

    # Before
    rubicon = Rubicon(persistence="filesystem", root_dir="./rubicon-root")

    # After
    rubicon = Rubicon(persistence="wandb")

Because every backend exposes the same logging API, the rest of your code does
not need to change. New experiments will be logged to W&B going forward.

Using Multiple Backends
=======================

rubicon-ml supports writing to multiple backends simultaneously via the
``composite_config`` parameter. When a composite config is set, **every write
operation fans out to all configured backends**, while **read operations return
from the first backend that succeeds**.

.. code-block:: python

    from rubicon_ml import Rubicon

    rubicon = Rubicon(
        composite_config=[
            {"persistence": "filesystem", "root_dir": "s3://my-bucket/rubicon-root"},
            {"persistence": "wandb"},
        ],
    )

    project = rubicon.get_or_create_project("My Project")
    experiment = project.log_experiment(name="dual-write")
    experiment.log_metric("accuracy", 0.95)
    # metric is now persisted to both S3 and W&B

This is useful when you want redundant storage or are migrating between backends
and want a period of dual-writing.

For a complete walkthrough, see the
:doc:`multiple backend notebook <logging-examples/multiple-backend>`.

.. _fsspec: https://filesystem-spec.readthedocs.io/en/latest/
