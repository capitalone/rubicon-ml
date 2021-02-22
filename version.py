"""
Starting point taken from https://gist.github.com/pwithnall/7bc5f320b3bdf418265a

Gets the current version number.
If in a git repository, it is the current git tag.
To use this script, simply import it in your setup.py file
and use the results of get_version() as your package version:
    from version import *
    setup(
        ...
        version=get_version(),
        ...
    )
"""

__all__ = "get_version"

import os
import subprocess
from os.path import dirname, isdir, join

tag = os.environ.get("COMMIT_MESSAGE")


def get_version():
    # tag gets specified during the Jenkins pipeline
    if tag:
        return tag

    # if no tag, pull from the latest github tag
    d = dirname(__file__)

    if isdir(join(d, "..", "..", ".git")):
        # Get the version using "git describe".
        cmd = "git describe --tags".split()
        try:
            version = subprocess.check_output(cmd).decode().strip()
        except subprocess.CalledProcessError:
            print("Unable to find .git directory")
            # default so that jenkins doesn't break
            version = "non-master-branch"

        # PEP 386 compatibility
        if "-" in version:
            version = ".post".join(version.split("-")[:2])

    else:
        print("Unable to find .git directory")
        # default so that jenkins doesn't break
        version = "non-master-branch"

    return version


if __name__ == "__main__":
    print(get_version())
