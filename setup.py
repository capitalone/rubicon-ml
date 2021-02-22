from setuptools import find_packages, setup

from version import get_version

install_requires = [
    "click>=7.1",
    "dask[dataframe]>=2.12.0",
    "fsspec>=0.8.3",
    "intake>=0.5.2",
    "pyarrow>=0.16.0,<0.18.0",
    "pyyaml>=3.12.0,<5.4.0",
    "s3fs>=0.5.1",
]

prefect_requires = ["prefect>=0.12.0"]
ui_requires = ["dash>=1.14.0", "dash-bootstrap-components>=0.10.6"]

all_requires = prefect_requires + ui_requires

extras_require = {
    "all": all_requires,
    "prefect": prefect_requires,
    "ui": ui_requires,
}

setup(
    name="rubicon",
    version=get_version(),
    author="The Rubicon Team",
    author_email="rubicon-developers@capitalone.com",
    description="an ML library for model development and governance",
    packages=find_packages(),
    include_package_data=True,
    url="https://github.com/capitalone/rubicon",
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        "console_scripts": ["rubicon=rubicon.cli:cli"],
        "intake.drivers": [
            "rubicon_project = rubicon.intake_rubicon.project:ProjectSource",
            "rubicon_experiment = rubicon.intake_rubicon.experiment:ExperimentSource",
        ],
    },
)
