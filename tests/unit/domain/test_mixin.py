from rubicon_ml.domain.mixin import CommentMixin, TagMixin


class Taggable(TagMixin, CommentMixin):
    def __init__(self, tags=[], comments=[]):
        self.tags = tags
        self.comments = comments


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


def test_add_comments():
    taggable = Taggable()
    taggable.add_comments(["x"])

    assert taggable.comments == ["x"]


def test_remove_comments():
    taggable = Taggable(comments=["comment 1", "comment 2"])
    taggable.remove_comments(["comment 1"])

    assert taggable.comments == ["comment 2"]
