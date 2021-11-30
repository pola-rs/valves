import pytest
import pandas as pd
import polars as pl
import dask.dataframe as dd

from valves.polars import sessionize as sess_pl
from valves.pandas import sessionize as sess_pd
from valves.dask import sessionize as sess_dd
from valves import sessionize

data_three_sessions = [
    {"user": 1, "timestamp": "2020-01-01 00:00:00"},
    {"user": 1, "timestamp": "2020-01-01 00:10:00"},
    {"user": 2, "timestamp": "2020-01-01 00:00:00"},
    {"user": 2, "timestamp": "2020-01-01 00:10:00"},
]

data_four_sessions = [
    {"user": 1, "timestamp": "2020-01-01 00:00:00"},
    {"user": 1, "timestamp": "2020-01-01 00:10:00"},
    {"user": 1, "timestamp": "2020-01-01 00:50:00"},
    {"user": 1, "timestamp": "2020-01-01 10:50:00"},
    {"user": 2, "timestamp": "2020-01-01 00:00:00"},
]


@pytest.mark.parametrize(
    "data, result",
    [
        (data_three_sessions, [1, 1, 2, 2]),
        (data_four_sessions, [1, 1, 2, 3, 4]),
    ],
)
def test_pandas_sessionize(data, result):
    """It needs to work on some simple pandas datasets"""
    dataf = pd.DataFrame(data).assign(
        timestamp=lambda d: pd.to_datetime(d["timestamp"])
    )
    # Ensure the direct function call works
    assert dataf.pipe(sess_pd)["session"].tolist() == result
    # Ensure the multiple dispatch function call works
    assert dataf.pipe(sessionize)["session"].tolist() == result


@pytest.mark.parametrize(
    "data, result",
    [
        (data_three_sessions, [1, 1, 2, 2]),
        (data_four_sessions, [1, 1, 2, 3, 4]),
    ],
)
def test_polars_sessionize(data, result):
    """It needs to work on some simple polars datasets"""
    dataf = pl.DataFrame(data).with_column(
        pl.col("timestamp").str.strptime(pl.Datetime)
    )
    # Ensure the direct function call works
    assert list(dataf.pipe(sess_pl)["session"]) == result
    # Ensure the multiple dispatch function call works
    assert list(dataf.pipe(sessionize)["session"]) == result


@pytest.mark.parametrize(
    "data, result",
    [
        (data_three_sessions, [1, 1, 2, 2]),
        (data_four_sessions, [1, 1, 2, 3, 4]),
    ],
)
def test_dask_sessionize(data, result):
    """It needs to work on some simple dask datasets"""
    dataf_pd = pd.DataFrame(data).assign(
        timestamp=lambda d: pd.to_datetime(d["timestamp"])
    )
    # Ensure the direct function call works
    dataf = dd.from_pandas(dataf_pd, npartitions=1).pipe(sess_dd)
    assert list(dataf["session"]) == result

    # Ensure the multiple dispatch function call works
    dataf = dd.from_pandas(dataf_pd, npartitions=1).pipe(sessionize)
    assert list(dataf["session"]) == result
