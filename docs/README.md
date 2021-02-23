# Documentation

`rubicon`'s documentation is hosted on the `gh-pages` branch within this
repository.

### Development

Create and activate the ``conda`` environment from the ``rubicon/docs``
directory:

```
conda env create -f docs-environment.yml
conda activate rubicon-docs
```

The included `build-docs.sh` script pulls the most recent version of the
documentation from the `gh-pages` branch and builds any changes into the
`rubicon/docs/build/html` directory. Then it opens the index locally to review
your changes.

```
bash build-docs.sh
```

When actively developing, run the script with the `--no-clone` option to skip
cloning the `gh-pages` branch every build. You only need to clone the repo right
before you push your source changes to avoid and resolve any conflicts.

```
bash build-docs.sh --no-clone
```

Never commit built documentation code directly to Rubicon. Our CICD handles
deploying documentation.
