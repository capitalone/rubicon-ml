"""Setup file for the package. For configuration information, see the ``setup.cfg``."""

from setuptools import setup

import versioneer

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
