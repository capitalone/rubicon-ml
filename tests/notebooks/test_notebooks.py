import os
from unittest import mock

import fsspec
import pytest
from nbconvert.preprocessors import ExecutePreprocessor

from tests.notebooks.utils import get_notebook_filenames, read_notebook_file

NOTEBOOK_FILENAMES = get_notebook_filenames(
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "notebooks")
)
BAD_NOTEBOOK_FILENAMES = get_notebook_filenames(
    os.path.join(os.path.dirname(__file__), "bad-notebooks")
)
BAD_NOTEBOOK_XFAIL_MARKS = [
    pytest.param(
        n,
        marks=pytest.mark.xfail(
            reason=f"`test_notebook_is_executed_in_order` for notebook {n} is expected to fail"
        ),
    )
    for n in BAD_NOTEBOOK_FILENAMES
]


@pytest.mark.run_notebooks
@pytest.mark.parametrize("notebook_filename", NOTEBOOK_FILENAMES + BAD_NOTEBOOK_XFAIL_MARKS)
def test_notebook_is_executed_in_order(notebook_filename):
    notebook = read_notebook_file(notebook_filename)
    execution_counts = [
        cell.get("execution_count") for cell in notebook.cells if cell.get("cell_type") == "code"
    ]

    is_all_cells_executed = all([ec is not None for ec in execution_counts])
    if not is_all_cells_executed:
        failure_message = "all code cells are not executed"

        is_last_cell_executed = execution_counts[-1] is not None
        if not is_last_cell_executed:
            failure_message += " - there might be an empty cell at the end"

        pytest.fail(failure_message)

    is_cell_execution_ordered = all(
        execution_counts[i] < execution_counts[i + 1] for i in range(len(execution_counts) - 1)
    )
    if not is_cell_execution_ordered:
        pytest.fail("code cells are executed out of order")


IGNORE_EXECUTE_NOTEBOOK_FILENAMES = [
    "classification.ipynb",
    "failure-modes.ipynb",
    "integration-prefect-workflows.ipynb",
    "logging-feature-plots.ipynb",
    "visualizing-experiments.ipynb",
]
EXECUTE_NOTEBOOK_FILENAMES = [
    n for n in NOTEBOOK_FILENAMES if os.path.split(n)[-1] not in IGNORE_EXECUTE_NOTEBOOK_FILENAMES
]


@mock.patch.dict(os.environ, {"RUBICON_ROOT": "test-rubicon-root"})
@pytest.mark.run_notebooks
@pytest.mark.parametrize("notebook_filename", EXECUTE_NOTEBOOK_FILENAMES)
def test_notebooks_execute_without_error(notebook_filename):
    notebook = read_notebook_file(notebook_filename)
    resources = {"metadata": {"path": os.path.dirname(notebook_filename)}}

    preprocessor = ExecutePreprocessor(kernel_name="python3", timeout=60)
    preprocessor.preprocess(notebook, resources=resources)

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    notebook_output_dir = os.path.join(repo_root, "notebooks", "test-rubicon-root")

    if "logging-experiments.ipynb" not in notebook_filename:
        fs = fsspec.filesystem("file")
        try:
            fs.rm(notebook_output_dir, recursive=True)
        except FileNotFoundError:
            pass  # some notebooks don't write output
