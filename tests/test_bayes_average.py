import pytest
import polars as pl
import pandas as pd

from valves.polars import bayes_average as bayes_av_pl
from valves.pandas import bayes_average as bayes_av_pd

data_two_items = [
    {"item": 1, "rating": 1},
    {"item": 1, "rating": 2},
    {"item": 2, "rating": 1},
    {"item": 2, "rating": 2},
]


def test_bayes_average_polars_no_smoothing():
    """Polars implementation works without any smoothing."""
    dataf = pl.DataFrame(data_two_items).pipe(
        bayes_av_pl, group_cols=["item"], target_col="rating", C=0
    )
    assert list(dataf["bayes_avg"]) == [1.5, 1.5, 1.5, 1.5]


def test_bayes_average_polars_heavy_smoothing():
    """Polars implementation works with heavy smoothing."""
    dataf = pl.DataFrame(data_two_items).pipe(
        bayes_av_pl, group_cols=["item"], target_col="rating", C=10000, prior_mean=10
    )
    for rating in list(dataf["bayes_avg"]):
        assert pytest.approx(rating, 0.1) == 10


def test_bayes_average_pandas_no_smoothing():
    """pandas implementation works without any smoothing."""
    dataf = pd.DataFrame(data_two_items).pipe(
        bayes_av_pd, group_cols=["item"], target_col="rating", C=0
    )
    assert list(dataf["bayes_avg"]) == [1.5, 1.5, 1.5, 1.5]


def test_bayes_average_pandas_heavy_smoothing():
    """pandas implementation works with heavy smoothing."""
    dataf = pd.DataFrame(data_two_items).pipe(
        bayes_av_pd, group_cols=["item"], target_col="rating", C=10000, prior_mean=10
    )
    for rating in list(dataf["bayes_avg"]):
        assert pytest.approx(rating, 0.1) == 10
