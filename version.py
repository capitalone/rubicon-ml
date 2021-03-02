import os
import subprocess


def _write_version_file(version):
    """Overwrites the file at 'rubicon/_version.py' to contain the following
    function where <VERSION> is the latest version of ``rubicon``:

    def _get_version():
        return '<VERSION>'

    Parameters
    ----------
    version : str
        The latest version of ``rubicon`` to be inserted into the generated
        file.
    """
    root_dir = os.path.dirname(os.path.abspath(__file__))
    version_file_path = os.path.join(root_dir, "rubicon", "_version.py")

    version_file = f"def _get_version():\n    return '{version}'\n"

    with open(version_file_path, "w") as f:
        f.write(version_file)


def get_version(write_version_file=False):
    """Gets the latest version of ``rubicon`` by inspecting the repository's
    ``git`` tags.

    Parameters
    ----------
    write_version_file : bool
        If true, rewrite the file at 'rubicon/_version.py' with the latest
        version. Defaults to false.

    Note
    ----
    Must be run within a clone of the ``rubicon`` repository with the
    latest tags.

    Returns
    -------
    str
        The latest version of ``rubicon``.
    """
    get_latest_git_tag_command = ["git", "describe", "--tags", "--abbrev=0"]
    version = subprocess.check_output(get_latest_git_tag_command, encoding="utf-8").strip()

    if write_version_file:
        _write_version_file(version)

    return version
