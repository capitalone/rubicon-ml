# Minimal makefile for Sphinx documentation

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

.PHONY: help clean pre-build build-docs post-build html

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Clear any existing, outdated sources.
clean:
	rm -rf ./build/html
	rm -rf ./source/logging-examples
	rm -rf ./source/integrations

# Copy over up to date sources from the root of the `rubicon_ml` repo.
pre-build:
	mkdir -p ./build/html
	mkdir -p ./source/logging-examples
	mkdir -p ./source/integrations

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

# Run the Sphinx command to build the docs as html.
build-docs:
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Manually update underscore-js to deal with Whitesource vulnerability.
# I have no idea where the original dependency is coming from.
post-build:
	rm -f ./build/html/_static/underscore*.js
	cp ./source/_static/underscore.js ./build/html/_static/underscore.js

	$(info open with 'open ./build/html/index.html')

html: clean pre-build build-docs post-build
