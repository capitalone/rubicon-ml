# rubicon-ml notebooks

These notebooks are interactive versions of the examples found in our
documentation. You can clone the repo and run the examples on your own, or just
take a look at their outputs here!

If you're a rubicon-ml user that wants to run the examples, check out the first
section, **Users** to get set up.

If you're a developer looking to create new examples, take a look at the second
section, **Developers**.

## Users

To ensure these examples work with your version of rubicon-ml, clone this repository
at the tag corresponding to the verison of rubicon-ml you'll be using by replacing
`X.X.X` in the command below with that version.

```
git clone https://github.com/capitalone/rubicon-ml.git --branch X.X.X --single-branch
```

Then, create and activate the "rubicon-ml" `conda` environment in the `notebooks` directory.

```
cd rubicon_ml
conda env create -f notebooks/user-environment.yml
conda activate rubicon-ml
```

The example notebooks can be viewed with either the `jupyter notebook` or `lab`
command.

```
jupyter notebook notebooks/
```

```
jupyter lab notebooks/
```

## Developers

When adding examples, make sure to commit any notebooks with their
cells executed in order. These example notebooks are rendered as-is within the
[documentation](https://capitalone.github.io/rubicon-ml/examples.html).

To develop examples off the latest on the `main` branch, use the "rubicon-ml-dev"
environment in `environment.yml` at the root of the rubicon-ml repository.

```
conda env create -f environment.yml
conda activate rubicon-ml-dev
```

You'll need to install Jupyterlab and a local copy of the library as well.

```
conda install jupyterlab
pip install -e .[all]
```
