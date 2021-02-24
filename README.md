# Rubicon

`rubicon` is a data science tool for capturing all information related to a model during its development. With minimal effort, Rubicon's Python library can integrate directly into your Python models:

```python
from rubicon import Rubicon

# Configure client object, automatically track git details
rubicon = Rubicon(persistence="filesystem", root_dir="/rubicon-root", auto_git_enabled=True)

# Create a project to hold a collection of experiments
project = rubicon.create_project("Hello World", description="Using rubicon to track model results over time.")

# Log experiment data
experiment = project.log_experiment(
    training_metadata=[SklearnTrainingMetadata("sklearn.datasets", "my-data-set")],
    model_name="My Model Name",
    tags=["model"],
)

experiment.log_parameter("n_estimators", n_estimators)
experiment.log_parameter("n_features", n_features)
experiment.log_parameter("random_state", random_state)

accuracy = rfc.score(X_test, y_test)
experiment.log_metric("accuracy", accuracy)

# Tag the data so it's easily filterable
if accuracy >= .94:
    experiment.add_tags(["success"])
```

Explore and visualize Rubicon projects stored locally or in S3 with the CLI:

```
rubicon ui --root-dir /rubicon-root
```

---

## Purpose

Rubicon is a data science tool for capturing all information related to a model
during its development. It allows data scientists to store model results
over time and ensures full audibility and reproducibility.

It offers the following features:

* a Python library for storing and retrieving model inputs, ouputs, and analyses
  to filesystems (local, S3)

* a dashboard for exploring, comparing, and visualizing logged data

* a process for sharing a selected subset of logged data with collaborators

Rubicon is designed to enforce best practices, like automatically linking
logged experiments (results) to their corresponding model code. And it supports
concurrent logging, so multiple experiments can be logged in parallel and also
asynchronous communication with S3, so network reads and writes don’t block.

## Documentation

For a full overview, visit the [docs](https://capitalone.github.io/rubicon/). If
you have suggestions or find a bug, [please open an
issue](https://github.com/capitalone/rubicon/issues/new/choose).

## Install

```
pip install rubicon-ml
```

or

```
conda install -c conda-forge rubicon-ml
```

## Develop

`rubicon` uses conda to manage environments. First, install
[conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).
Then use conda to setup a development environment:

```bash
conda env create -f ci/environment.yml
conda activate rubicon-dev
```

## Testing

The tests are separated into unit and integration tests. They can be run
directly in the activated dev environment via `pytest tests/unit` or `pytest
tests/integration`. Or by simply running `pytest` to execute all of them.

**Note**: some integration tests are intentionally `marked` to control when they
are run (i.e. not during cicd). These tests include:

* Integration tests that connect to physical filesystems (local, S3). You'll
  want to configure the `root_dir` appropriately for these tests
  (tests/integration/test_async_rubicon.py, tests/integration/test_rubicon.py).
  And they can be run with:

    ```
    pytest -m "physical_filesystem_test"
    ```

* Integration tests for the dashboard. To run these integration tests locally,
  you'll need to install one of the WebDrivers. To do so, follow the `Install`
  instructions in the [Dash Testing Docs](https://dash.plotly.com/testing) or
  install via brew with `brew cask install chromedriver`. You may have to update
  your permissions in Security & Privacy to install with brew.

    ```
    pytest -m "dashboard_test"
    ```

    **Note**: The `--headless` flag can be added to run the dashboard tests in
    headless mode.

## Code Formatting

Install and configure pre-commit to automatically run `black`, `flake8`, and
`isort` during commits:
* [install pre-commit](https://pre-commit.com/#installation)
* run `pre-commit install` to set up the git hook scripts

Now `pre-commit` will run automatically on git commit and will ensure consistent
code format throughout the project. You can format without committing via
`pre-commit run` or skip these checks with `git commit --no-verify`.

## Contributors

<table>
  <tr>
    <td align="center"><a href="https://github.com/mmccarty"><img src="https://avatars.githubusercontent.com/u/625946?v=4"
    width="100px;" alt=""/><br /><sub><b>Mike McCarty</b></sub></a><br /></td>
    <td align="center"><a href="https://github.com/srilatharanganathan"><img src="https://avatars.githubusercontent.com/u/31327886?v=4"
    width="100px;" alt=""/><br /><sub><b>Sri Ranganathan</b></sub></a><br /></td>
    <td align="center"><a href="https://github.com/joe-wolfe21"><img src="https://avatars.githubusercontent.com/u/10947704?v=4" width="100px;" alt=""/><br /><sub><b>Joe Wolfe</b></sub></a><br /></td>
    <td align="center"><a href="https://github.com/RyanSoley"><img src="https://avatars.githubusercontent.com/u/53409969?v=4" width="100px;" alt=""/><br /><sub><b>Ryan Soley</b></sub></a><br /></td>
    <td align="center"><a href="https://github.com/dianelee217"><img src="https://avatars.githubusercontent.com/u/67274829?v=4" width="100px;" alt=""/><br /><sub><b>Diane Lee</b></sub></a><br /></td>
  </tr>
</table>
