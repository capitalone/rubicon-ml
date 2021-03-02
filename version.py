import subprocess


def get_version():
    get_latest_git_tag_command = ["git", "describe", "--tags", "--abbrev=0"]
    completed_subprocess = subprocess.run(get_latest_git_tag_command)

    return completed_subprocess.stdout
