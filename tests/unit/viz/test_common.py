import pytest

from rubicon_ml.viz.common.colors import get_rubicon_colorscale
from rubicon_ml.viz.common.dropdown_header import dropdown_header


@pytest.mark.parametrize("num_colors,expected", [(1, 2), (4, 4)])
def test_get_rubicon_colorscale(num_colors, expected):
    colors = get_rubicon_colorscale(num_colors)

    assert len(colors) == expected


def test_dropdown_header():
    layout = dropdown_header(["A", "B", "C"], "A", "test left", "test right", "test-id")

    assert layout.id.startswith("test-id")

    assert layout.children[1].children.label == "A"
    assert [child.children for child in layout.children[1].children.children] == ["A", "B", "C"]

    assert layout.children[0].children.children == "test left"
    assert layout.children[2].children.children == "test right"
