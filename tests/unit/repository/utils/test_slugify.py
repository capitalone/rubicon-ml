from rubicon_ml.repository.utils import slugify


def test_can_slugify():
    un_slugified = " TESTing...    SlUgIfY?    "
    slugified = "testing-slugify"
    assert slugify(un_slugified) == slugified
