from rubicon.domain.mixin import TagMixin


class Taggable(TagMixin):
    def __init__(self, tags=[]):
        self.tags = tags


def test_add_tags():
    taggable = Taggable()
    taggable.add_tags(["x"])

    assert taggable.tags == ["x"]


def test_add_duplicate_tags():
    taggable = Taggable(tags=["x"])
    taggable.add_tags(["x"])

    assert taggable.tags == ["x"]


def test_remove_tags():
    taggable = Taggable(tags=["x", "y"])
    taggable.remove_tags(["x"])

    assert taggable.tags == ["y"]
