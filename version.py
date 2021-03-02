import os
import subprocess


def _write_version_file(version):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    version_file_path = os.path.join(root_dir, "rubicon", "_version.py")

    version_file = f"def get_version():\n    return '{version}'\n"

    with open(version_file_path, "w") as f:
        f.write(version_file)


def get_version(write_version_file=False):
    get_latest_git_tag_command = ["git", "describe", "--tags", "--abbrev=0"]
    version = subprocess.check_output(get_latest_git_tag_command, encoding="utf-8").strip()

    if write_version_file:
        _write_version_file(version)

    return version
