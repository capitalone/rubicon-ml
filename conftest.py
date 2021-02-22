pytest_plugins = [
    "tests.fixtures",
]


def pytest_addoption(parser):
    parser.addoption("--s3-path", dest="s3-path")
