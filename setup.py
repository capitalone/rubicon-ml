import os

from setuptools import find_packages, setup

import versioneer

pwd = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(pwd, "README.md"), encoding="utf-8") as readme:
    long_description = readme.read()

install_requires = [
    "click>=7.1",
    "dask[dataframe]>=2.12.0",
    "fsspec>=2021.4.0",
    "intake>=0.5.2",
    "pyarrow>=0.18.0",
    "pyyaml>=5.4.0",
    "s3fs>=0.4",
]

prefect_requires = ["prefect>=0.12.0"]
ui_requires = ["dash>=1.14.0", "dash-bootstrap-components>=0.10.6", "jupyter-dash"]

all_requires = prefect_requires + ui_requires

extras_require = {
    "all": all_requires,
    "prefect": prefect_requires,
    "ui": ui_requires,
}

setup(
    name="rubicon-ml",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Joe Wolfe, Ryan Soley, Diane Lee, Mike McCarty, CapitalOne",
    license="Apache License, Version 2.0",
    description="an ML library for model development and governance",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    url="https://github.com/capitalone/rubicon-ml",
    python_requires=">=3.7",
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        "console_scripts": ["rubicon_ml=rubicon_ml.cli:cli"],
        "intake.drivers": [
            "rubicon_ml_project = rubicon_ml.intake_rubicon.project:ProjectSource",
            "rubicon_ml_experiment = rubicon_ml.intake_rubicon.experiment:ExperimentSource",
        ],
    },
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 4 - Beta",
        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Build Tools",
        "Topic :: Software Development :: Documentation",
        # Pick your license as you wish (should match "license" above)
        "License :: OSI Approved :: Apache Software License",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 3 or both.
        "Programming Language :: Python :: 3",
    ],
)
