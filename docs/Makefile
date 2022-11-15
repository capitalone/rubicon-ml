# Minimal makefile for Sphinx documentation

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

.PHONY: help clean pre-build build-docs post-build html open

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Clear any existing, outdated sources.
clean:
	rm -rf ./build/html
	rm -rf ./source/quick-look
	rm -rf ./source/logging-examples
	rm -rf ./source/integrations
	rm -rf ./source/tutorials
	rm -rf ./source/visualizations

# Copy over up to date sources from the root of the `rubicon_ml` repo.
pre-build:
	mkdir -p ./build/html
	mkdir -p ./source/quick-look
	mkdir -p ./source/logging-examples
	mkdir -p ./source/integrations
	mkdir -p ./source/tutorials
	mkdir -p ./source/visualizations

	cp ../notebooks/quick-look/*.ipynb ./source/quick-look
	cp ../notebooks/logging-examples/*.ipynb ./source/logging-examples
	cp ../notebooks/integrations/*.ipynb ./source/integrations
	cp ../notebooks/tutorials/*.ipynb ./source/tutorials
	cp ../notebooks/viz/*.ipynb ./source/visualizations

	cp ../notebooks/quick-look/*.png ./source/quick-look
	cp ../notebooks/logging-examples/*.png ./source/logging-examples
	cp ../notebooks/integrations/*.png ./source/integrations
	cp ../notebooks/viz/*.png ./source/visualizations

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

open:
	open ./build/html/index.html
