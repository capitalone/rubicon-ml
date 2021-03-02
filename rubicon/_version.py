import subprocess


def _get_version():
    """Gets the latest version of ``rubicon`` by inspecting the repository's
    ``git`` tags.

    Note
    ----
    This file will be overwritten when ``rubicon`` is packaged using ``setup.py``
    such that ``_get_version`` returns the latest version of ``rubicon`` without
    using ``git``.

    Returns
    -------
    str
        The latest version of ``rubicon``.
    """
    get_latest_git_tag_command = ["git", "describe", "--tags", "--abbrev=0"]
    version = subprocess.check_output(get_latest_git_tag_command, encoding="utf-8").strip()

    return version
