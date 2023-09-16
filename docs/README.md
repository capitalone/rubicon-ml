# Documentation

`rubicon`'s documentation is hosted on the `gh-pages` branch within this
repository.

### Development

Create and activate the `conda` environment in the `rubicon/docs`
directory and install a local copy of `rubicon_ml`.

```
conda env create -n rubicon-ml-docs python>=3.8
conda activate rubicon-ml-docs
pip install -e ".[docs]"
```

Use the provided Makefile to build the docs locally.

```
cd docs/
make html
```

Never commit built documentation code directly to the `gh-pages` branch.
Our CICD handles building and deploying documentation.
