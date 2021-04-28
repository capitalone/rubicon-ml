import os
import warnings


def test_rubicon_with_misc_folders_at_project_level(rubicon_local_filesystem_client_with_project):
    rubicon, project = rubicon_local_filesystem_client_with_project

    os.makedirs(os.path.join(rubicon.config.root_dir, ".ipynb_checkpoints"))

    with warnings.catch_warnings(record=True) as w:
        projects = rubicon.projects()

        assert len(projects) == 1
        assert "not found" in str(w[-1].message)


def test_rubicon_with_misc_folders_at_sublevel_level(rubicon_local_filesystem_client_with_project):
    rubicon, project = rubicon_local_filesystem_client_with_project

    project.log_experiment("exp1")
    project.log_experiment("exp2")

    os.makedirs(
        os.path.join(rubicon.config.root_dir, "test-project", "experiments", ".ipynb_checkpoints")
    )

    with warnings.catch_warnings(record=True) as w:
        experiments = project.experiments()

        assert len(experiments) == 2
        assert "not found" in str(w[-1].message)


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

    with warnings.catch_warnings(record=True) as w:
        parameters = exp.parameters()

        assert len(parameters) == 1
        assert "not found" in str(w[-1].message)
