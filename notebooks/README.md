# `rubicon` notebooks

The `rubicon` notebooks are interactive versions of the examples found in our
documentation.

### Running the notebooks

Create and activate the `conda` environment from the `rubicon/notebooks`
directory:

```
conda env create -f user-environment.yml
conda activate rubicon
```

Start the local notebook server:

```
jupyter notebook
```

### Development

To run the examples off the latest master, uninstall `rubicon` from your
environment and reinstall a local copy with `pip`:

```
conda remove rubicon
pip install -e .[all]
```

**Note**: Make sure to run the install command from the repository's root
`rubicon` directory.
