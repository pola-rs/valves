import pytest
from valves import item_item_counts, sessionize, bayes_average


@pytest.mark.parametrize("func", [item_item_counts, sessionize])
def test_dataf_error_occurs(func):
    """These functions dont have required arugments."""
    with pytest.raises(ValueError):
        func(1)


def test_dataf_error_bayes_average():
    """bayes_average has required arugments."""
    with pytest.raises(ValueError):
        bayes_average(1, group_cols="a", target_col="b", C=10)
