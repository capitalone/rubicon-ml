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

html: prepare

# Clear the examples directory and then copy the advanced example 
# notebooks to the source folder so nbsphinx can build docs off of them
prepare:
	rm -rf ./source/rubicon-root ./source/*.ipynb
	rm -rf ./source/data

	cp ../notebooks/logging-examples/logging-basics.ipynb ./source
	cp ../notebooks/logging-examples/logging-asynchronously.ipynb ./source
	cp ../notebooks/logging-examples/logging-concurrently.ipynb ./source
	cp ../notebooks/logging-examples/logging-training-metadata.ipynb ./source
	cp ../notebooks/logging-examples/visualizing-logged-dataframes.ipynb ./source
	cp ../notebooks/integrations/integration-git.ipynb ./source
	cp ../notebooks/integrations/integration-prefect-workflows.ipynb ./source

	# some notebooks require sample data
	mkdir ./source/data
	cp ../notebooks/logging-examples/data/revenue_data.csv ./source/data

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
