import os
from pathlib import Path


def test_rubicon_with_misc_folders_at_project_level(rubicon_local_filesystem_client_with_project):
    rubicon, project = rubicon_local_filesystem_client_with_project

    os.makedirs(os.path.join(rubicon.config.root_dir, "test-project", ".ipynb_checkpoints"))
    os.makedirs(os.path.join(rubicon.config.root_dir, "test"))

    assert len(rubicon.projects()) == 1


def test_rubicon_with_misc_files_at_project_level(rubicon_local_filesystem_client_with_project):
    rubicon, project = rubicon_local_filesystem_client_with_project

    Path(os.path.join(rubicon.config.root_dir, "file.txt")).touch()

    assert len(rubicon.projects()) == 1


def test_rubicon_with_misc_folders_at_sublevel_level(rubicon_local_filesystem_client_with_project):
    rubicon, project = rubicon_local_filesystem_client_with_project

    project.log_experiment("exp1")
    project.log_experiment("exp2")

    os.makedirs(
        os.path.join(rubicon.config.root_dir, "test-project", "experiments", ".ipynb_checkpoints")
    )
    os.makedirs(os.path.join(rubicon.config.root_dir, "test-project", "experiments", "test"))

    assert len(project.experiments()) == 2


def test_rubicon_with_misc_files_at_sublevel_level(rubicon_local_filesystem_client_with_project):
    rubicon, project = rubicon_local_filesystem_client_with_project

    project.log_experiment("exp1")
    project.log_experiment("exp2")

    Path(os.path.join(rubicon.config.root_dir, "test-project", "experiments", "file.txt")).touch()

    assert len(project.experiments()) == 2


def test_rubicon_with_misc_folders_at_deeper_sublevel_level(
    rubicon_local_filesystem_client_with_project,
):
    rubicon, project = rubicon_local_filesystem_client_with_project

    exp = project.log_experiment("exp1")
    exp.log_parameter("a", 1)

    os.makedirs(
        os.path.join(
            rubicon.config.root_dir,
            "test-project",
            "experiments",
            exp.id,
            "parameters",
            ".ipynb_checkpoints",
        )
    )
    os.makedirs(
        os.path.join(
            rubicon.config.root_dir, "test-project", "experiments", exp.id, "parameters", "test"
        )
    )

    assert len(exp.parameters()) == 1
