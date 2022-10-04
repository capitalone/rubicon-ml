# rubicon-ml

[![Test Package](https://github.com/capitalone/rubicon-ml/actions/workflows/test-package.yml/badge.svg)](https://github.com/capitalone/rubicon-ml/actions/workflows/test-package.yml)
[![Publish Package](https://github.com/capitalone/rubicon-ml/actions/workflows/publish-package.yml/badge.svg)](https://github.com/capitalone/rubicon-ml/actions/workflows/publish-package.yml)
[![Publish Docs](https://github.com/capitalone/rubicon-ml/actions/workflows/publish-docs.yml/badge.svg)](https://github.com/capitalone/rubicon-ml/actions/workflows/publish-docs.yml)
[![edgetest](https://github.com/capitalone/rubicon-ml/actions/workflows/edgetest.yml/badge.svg)](https://github.com/capitalone/rubicon-ml/actions/workflows/edgetest.yml)

[![Conda Version](https://img.shields.io/conda/vn/conda-forge/rubicon-ml.svg)](https://anaconda.org/conda-forge/rubicon-ml)
[![PyPi Version](https://img.shields.io/pypi/v/rubicon_ml.svg)](https://pypi.org/project/rubicon-ml/)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/capitalone/rubicon-ml/main?labpath=binder%2Fwelcome.ipynb)


## Purpose

rubicon-ml is a data science tool that captures and stores model training and
execution information, like parameters and outcomes, in a repeatable and
searchable way. Its `git` integration associates these inputs and outputs
directly with the model code that produced them to ensure full auditability and
reproducibility for both developers and stakeholders alike. While experimenting,
the dashboard makes it easy to explore, filter, visualize, and share
recorded work.

p.s. If you're looking for Rubicon, the Java/ObjC Python bridge, visit
[this](https://pypi.org/project/rubicon/) instead.

---

## Components

rubicon-ml is composed of three parts:

* A Python library for storing and retrieving model inputs, outputs, and
  analyses to filesystems that’s powered by
  [`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest)
* A dashboard for exploring, comparing, and visualizing logged data built with
  [`dash`](https://dash.plotly.com/)
* And a process for sharing a selected subset of logged data with collaborators
  or reviewers that leverages [`intake`](https://intake.readthedocs.io/en/latest/)

## Workflow

Use `rubicon_ml` to capture model inputs and outputs over time. It can be
easily integrated into existing Python models or pipelines and supports both
concurrent logging (so multiple experiments can be logged in parallel) and
asynchronous communication with S3 (so network reads and writes won’t block).

Meanwhile, periodically review the logged data within the Rubicon dashboard to
steer the model tweaking process in the right direction. The dashboard lets you
quickly spot trends by exploring and filtering your logged results and
visualizes how the model inputs impacted the model outputs.

When the model is ready for review, Rubicon makes it easy to share specific
subsets of the data with model reviewers and stakeholders, giving them the
context necessary for a complete model review and approval.

## Use

Check out the [interactive notebooks in this Binder](https://mybinder.org/v2/gh/capitalone/rubicon-ml/main?labpath=binder%2Fwelcome.ipynb)
to try `rubicon_ml` for yourself.

Here's a simple example:

```python
from rubicon_ml import Rubicon

rubicon = Rubicon(
    persistence="filesystem", root_dir="/rubicon-root", auto_git_enabled=True
)

project = rubicon.create_project(
    "Hello World", description="Using rubicon to track model results over time."
)

experiment = project.log_experiment(
    training_metadata=[SklearnTrainingMetadata("sklearn.datasets", "my-data-set")],
    model_name="My Model Name",
    tags=["my_model_name"],
)

experiment.log_parameter("n_estimators", n_estimators)
experiment.log_parameter("n_features", n_features)
experiment.log_parameter("random_state", random_state)

accuracy = rfc.score(X_test, y_test)
experiment.log_metric("accuracy", accuracy)
```

Then explore the project by running the dashboard:

```
rubicon_ml ui --root-dir /rubicon-root
```

## Documentation

For a full overview, visit the [docs](https://capitalone.github.io/rubicon-ml/). If
you have suggestions or find a bug, [please open an
issue](https://github.com/capitalone/rubicon-ml/issues/new/choose).

## Install

The Python library is available on Conda Forge via `conda` and PyPi via `pip`.

```
conda config --add channels conda-forge
conda install rubicon-ml
```

or

```
pip install rubicon-ml
```

## Develop

The project uses conda to manage environments. First, install
[conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).
Then use conda to setup a development environment:

```bash
conda env create -f environment.yml
conda activate rubicon-ml-dev
```

Finally, install `rubicon_ml` locally into the newly created environment.

```bash
pip install -e ".[all]"
```

## Testing

The tests are separated into unit and integration tests. They can be run
directly in the activated dev environment via `pytest tests/unit` or `pytest
tests/integration`. Or by simply running `pytest` to execute all of them.

**Note**: some integration tests are intentionally `marked` to control when they
are run (i.e. not during CICD). These tests include:

* Integration tests that write to physical filesystems - local and S3. Local
  files will be written to `./test-rubicon` relative to where the tests are run.
  An S3 path must also be provided to run these tests. By default, these
  tests are disabled. To enable them, run:

    ```
    pytest -m "write_files" --s3-path "s3://my-bucket/my-key"
    ```

* Integration tests that run Jupyter notebooks. These tests are a bit slower
  than the rest of the tests in the suite as they need to launch Jupyter servers.
  By default, they are enabled. To disable them, run:

    ```
    pytest -m "not run_notebooks and not write_files"
    ```

    **Note**: When simply running `pytest`, `-m "not write_files"` is the
    default. So, we need to also apply it when disabling notebook tests.

## Code Formatting

Install and configure pre-commit to automatically run `black`, `flake8`, and
`isort` during commits:
* [install pre-commit](https://pre-commit.com/#installation)
* run `pre-commit install` to set up the git hook scripts

Now `pre-commit` will run automatically on git commit and will ensure consistent
code format throughout the project. You can format without committing via
`pre-commit run` or skip these checks with `git commit --no-verify`.
