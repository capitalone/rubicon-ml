# Rubicon

`rubicon` is a model development and governance library that can integrate
directly into your python models. `rubicon` stores model inputs, analyses and
results for quick, simple model iteration and reproducibility.

## Developers

Install the dependencies:

```bash
conda env create -f ci/environment.yml
conda activate positron-dev
```

### Testing

Run the tests as Jenkins would:

```bash
python -m pytest
```

Run unit or integration tests:

```bash
python -m pytest tests/unit
python -m pytest tests/integration
```

**Note**: some tests are intentionally `marked` to control when they are run (i.e. not during cicd). These tests include:

* Integration tests that connect to physical filesystems (local, S3)

    ```
    python -m pytest -m "physical_filesystem_test"
    ```

* Dashboard Integration Tests

    ```
    python -m pytest -m "dashboard_test"
    ```

    **Note**: To run these integration tests locally, you'll need to install one of the WebDrivers.
    To do so, follow the `Install` instructions in the [Dash Testing Docs](https://dash.plotly.com/testing)
    or install via brew with `brew cask install chromedriver`. You may have to update your permissions
    in Security & Privacy to install with brew.

    **Note**: The `--headless` flag can be added to run the dashboard tests in headless mode.

    We're currently excluding these from cicd, but may move towards running them nightly or on
    master merges.

Install locally:

```bash
cd positron
pip install -e .
```

Optionally, install and configure [pre-commit](https://pre-commit.com/) to
automatically run black, flake8, and isort during commits:
* [install pre-commit](https://pre-commit.com/#installation)
* run `pre-commit install` to set up the git hook scripts

Now `pre-commit` will run automatically on git commit!
