# Documentation

`rubicon`'s documentation is hosted on the `gh-pages` branch within this
repository.

### Development

Create and activate the `conda` environment in the `rubicon/docs`
directory and install a local copy of `rubicon_ml`.

```
conda env create -f docs/docs-environment.yml
conda activate rubicon-ml-docs
cd docs
pip install --no-deps -e ../
```

*Note:* We're using the `--no-deps` flag because `docs-environment.yml`
already installed the `rubicon_ml` dependencies we need to build the docs.

Use the provided Makefile to build the docs locally.

```
cd docs/
make html
```

Never commit built documentation code directly to the `gh-pages` branch.
Our CICD handles building and deploying documentation.
