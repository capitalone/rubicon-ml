import os
import sys

import pandas as pd

import holoviews as hv
from rubicon import Rubicon
from rubicon.exceptions import RubiconException


def get_or_create_project(rubicon, name):
    try:
        project = rubicon.create_project(name)
    except RubiconException:
        project = rubicon.get_project(name)

    return project


def log_rubicon(path):
    project_name = "intake-rubicon unit testing"
    if os.path.exists(os.path.join(path, "projects", project_name)):
        return

    rubicon = Rubicon(persistence="filesystem", root_dir=path)
    project = get_or_create_project(rubicon, project_name)
    experiment_a = project.log_experiment(name="experiment_a", tags=["model-a", "y"])
    experiment_a.log_feature("age")
    experiment_a.log_feature("credit score")

    experiment_b = project.log_experiment(name="experiment_b", tags=["model-b", "y"])
    experiment_b.log_feature("age")
    experiment_b.log_feature("credit score")

    experiment_a.log_parameter("random state", 13243546)
    experiment_a.log_parameter("test size", "10 GB")
    experiment_a.log_parameter("n_estimators", 20)

    experiment_a.log_metric("Accuracy", "99")
    experiment_a.log_metric("AUC", "0.825")

    df = pd.DataFrame([[1, 2, 3], [2, 1, 2], [3, 2, 1]], columns=["x", "y", "z"])
    dataframe = experiment_a.log_dataframe(df, tags=["x", "y"])
    plot = dataframe.plot(kind="bar")
    plot_path = f"{path}/plot.png"
    hv.save(plot, plot_path, fmt="png")

    project.log_artifact(data_path=plot_path, description="bar plot logged with path")

    with open(plot_path, "rb") as f:
        source_data = f.read()

    project.log_artifact(
        data_bytes=source_data,
        name="plot.png",
        description="bar plot logged with bytes",
    )

    with open(plot_path, "rb") as f:
        project.log_artifact(data_file=f, name="plot.png", description="bar plot logged with file")


if __name__ == "__main__":
    here = os.path.dirname(__file__)
    path = os.path.join(here, "data")

    sys.exit(log_rubicon(path))
