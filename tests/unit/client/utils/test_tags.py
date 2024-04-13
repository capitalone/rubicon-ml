import pytest

from rubicon_ml.client.utils.tags import TagContainer, has_tag_requirements


@pytest.mark.parametrize(
    ["required_tags", "qtype"],
    [
        (["pre_y_post"], "or"),
        (["missing", "pre_y_post"], "or"),
        (["pre_y_post"], "and"),
        (["pre_y_post", "pre_z"], "and"),
        (["x_post", "pre_y_post"], "and"),
        (["*y*"], "or"),
        (["missing", "*y*"], "or"),
        (["*y*"], "and"),
        (["*y*", "*z"], "and"),
        (["x*", "*y*"], "and"),
        (["*_*_*", "*_*"], "and"),
    ],
)
def test_has_tags(required_tags, qtype):
    tags = ["x_post", "pre_y_post", "pre_z"]

    assert has_tag_requirements(tags, required_tags, qtype)


@pytest.mark.parametrize(
    ["required_tags", "qtype"],
    [
        (["missing"], "or"),
        (["missing"], "and"),
        (["missing", "pre_z"], "and"),
        (["*y"], "or"),
        (["y*"], "or"),
        (["*y"], "and"),
        (["y*"], "and"),
        (["missing", "*z"], "and"),
    ],
)
def test_not_has_tags(required_tags, qtype):
    tags = ["x_post", "pre_y_post", "pre_z"]

    assert not has_tag_requirements(tags, required_tags, qtype)


def test_tag_container():
    tags = TagContainer(["a", "b:c", "d:e", "d:f"])

    assert tags[0] == "a"
    assert tags[1] == "b:c"
    assert tags[2] == "d:e"
    assert tags[3] == "d:f"
    assert tags["b"] == "c"
    assert tags["d"] == ["e", "f"]


def test_tag_container_nested():
    tags = TagContainer(["a", "b:c", "d:e", "d:f"])

    assert tags[1:] == ["b:c", "d:e", "d:f"]
    assert tags[1:2]["b"] == "c"
    assert tags[2:4]["d"] == ["e", "f"]


def test_tag_container_errors():
    tags = TagContainer([])

    with pytest.raises(KeyError) as error:
        tags["missing"]

    assert "KeyError('missing')" in str(error)
