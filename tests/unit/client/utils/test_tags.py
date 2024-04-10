import pytest

from rubicon_ml.client.utils.tags import TagContainer, has_tag_requirements


def test_or_single_success():
    assert has_tag_requirements(["x", "y", "z"], ["y"], "or")


def test_or_multiple_success():
    assert has_tag_requirements(["x", "y", "z"], ["a", "y"], "or")


def test_or_single_failure():
    assert not has_tag_requirements(["x", "y", "z"], ["a"], "or")


def test_and_single_success():
    assert has_tag_requirements(["x", "y", "z"], ["y"], "and")


def test_and_multiple_success():
    assert has_tag_requirements(["x", "y", "z"], ["y", "z"], "and")


def test_and_single_failure():
    assert not has_tag_requirements(["x", "y", "z"], ["a"], "and")


def test_and_multiple_failure():
    assert not has_tag_requirements(["x", "y", "z"], ["a", "z"], "and")


def test_tag_container():
    tags = TagContainer(["a", "b:c"])

    assert tags[0] == "a"
    assert tags[1] == "b:c"
    assert tags["b"] == "c"


def test_tag_container_errors():
    tags = TagContainer([])

    with pytest.raises(KeyError) as error:
        tags["missing"]

    assert "KeyError('missing')" in str(error)
