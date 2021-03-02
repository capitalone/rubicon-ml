import subprocess


def get_version():
    get_latest_git_tag_command = ["git", "describe", "--tags", "--abbrev=0"]
    version = subprocess.check_output(get_latest_git_tag_command, encoding="utf-8").strip()

    return version
