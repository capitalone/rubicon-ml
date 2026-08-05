# Documentation

`rubicon`'s documentation is hosted on the `gh-pages` branch within this
repository.

### Development
#### Conda
Create and activate the `conda` environment in the `rubicon`
directory and install a local copy of `rubicon_ml`.

```
conda env create -n rubicon-ml-docs
conda activate rubicon-ml-docs
pip install .[docs]
```

Use the provided Makefile to build the docs locally.

```
cd docs/
make html
```

#### UV
```
uv venv --python 3.13
source ./venv/bin/activate
uv sync --extra docs

```

Use the provided Makefile to build the docs locally.

```
cd docs/
uv make clean html
```

Never commit built documentation code directly to the `gh-pages` branch.
Our CICD handles building and deploying documentation.
