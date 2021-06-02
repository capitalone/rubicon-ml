# Minimal makefile for Sphinx documentation

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

html: pre-build build-docs post-build

# Clear any existing sources and copy over up to date ones from the root
# of the `rubicon_ml` repo.
pre-build:
	rm -rf ./source/rubicon-root

	rm -rf ./source/logging-examples
	rm -rf ./source/integrations
	rm -f ./source/*.ipynb

	mkdir ./source/logging-examples
	mkdir ./source/logging-examples/data
	mkdir ./source/integrations

	cp ../notebooks/quick-look.ipynb ./source
	cp ../notebooks/logging-examples/logging-basics.ipynb ./source/logging-examples
	cp ../notebooks/logging-examples/logging-asynchronously.ipynb ./source/logging-examples
	cp ../notebooks/logging-examples/logging-concurrently.ipynb ./source/logging-examples
	cp ../notebooks/logging-examples/logging-training-metadata.ipynb ./source/logging-examples
	cp ../notebooks/logging-examples/visualizing-logged-dataframes.ipynb ./source/logging-examples
	cp ../notebooks/integrations/integration-git.ipynb ./source/integrations
	cp ../notebooks/integrations/integration-prefect-workflows.ipynb ./source/integrations
	cp ../notebooks/integrations/integration-sklearn.ipynb ./source/integrations

	cp ../notebooks/quick-look-dashboard.png ./source
	cp ../notebooks/integrations/integration-git-dashboard.png ./source/integrations

# Build the html docs.
build-docs:
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Manually update underscore-js to deal with Whitesource vulnerability.
# I have no idea where the original dependency is coming from.
post-build:
	cp ./source/_static/underscore.js ./build/html/_static/underscore.js
	rm ./build/html/_static/underscore-1.12.0.js
