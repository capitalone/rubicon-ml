# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

from rubicon import __version__ as version  # noqa F401

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath("."))


# -- Project information -----------------------------------------------------

project = "rubicon"
author = "rubicon developers"
copyright = "2020, rubicon developers"


# -- General configuration ---------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = "1.0"

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named "sphinx.ext.*") or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.githubpages",
    "numpydoc",
    "nbsphinx",
    "sphinx_copybutton",
]

# Don't execute the notebook cells when generating the documentation
# This can be configured on a per notebook basis as well
# See: https://nbsphinx.readthedocs.io/en/0.2.15/never-execute.html#Explicitly-Dis-/
nbsphinx_execute = "never"
nbsphinx_prolog = """
`View this notebook on GitHub <https://github.com/capitalone/rubicon/tree/main/notebooks/{{ env.doc2path(env.docname, base=None) }}>`_

----
"""

autodoc_default_flags = ["members", "inherited-members"]

numpydoc_class_members_toctree = False
numpydoc_show_class_members = False
numpydoc_attributes_as_param_list = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = [".rst", ".md"]
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.
html_theme = "furo"

html_static_path = ["_static"]

html_theme_options = {
    "light_logo": "images/rubicon_logo_black.png",
    "dark_logo": "images/rubicon_logo_white.png",
    "sidebar_hide_name": True,
}

html_css_files = [
    "custom.css",
]
