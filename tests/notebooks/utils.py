import json
import os

import fsspec
import nbformat

DEFAULT_NBFORMAT_VERSION = 4


def get_notebook_filenames(root_path):
    fs = fsspec.filesystem("file")

    notebook_glob = os.path.join(root_path, "*.ipynb")
    nested_notebook_glob = os.path.join(root_path, "**", "*.ipynb")

    notebook_filenames = fs.glob(notebook_glob) + fs.glob(nested_notebook_glob)

    return [n for n in notebook_filenames if ".ipynb_checkpoints" not in n]


def read_notebook_file(notebook_filename):
    fs = fsspec.filesystem("file")

    with fs.open(notebook_filename, "r") as notebook_file:
        notebook = notebook_file.read()
        notebook_json = json.loads(notebook)
        notebook_version = notebook_json.get("nbformat", DEFAULT_NBFORMAT_VERSION)

        notebook = nbformat.reads(notebook, as_version=notebook_version)

    return notebook
