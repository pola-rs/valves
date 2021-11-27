import pytest
import pandas as pd
import polars as pl

from valve._pandas import sessionize as sess_pd
from valve._polars import sessionize as sess_pl

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
    dataf = (
        pd.DataFrame(data)
        .assign(timestamp=lambda d: pd.to_datetime(d["timestamp"]))
        .pipe(sess_pd)
    )
    assert dataf.session.tolist() == result


@pytest.mark.parametrize(
    "data, result",
    [
        (data_three_sessions, [1, 1, 2, 2]),
        (data_four_sessions, [1, 1, 2, 3, 4]),
    ],
)
def test_polars_sessionize(data, result):
    """It needs to work on some simple polars datasets"""
    dataf = (
        pl.DataFrame(data)
        .with_column(pl.col("timestamp").str.strptime(pl.Datetime))
        .pipe(sess_pl)
    )
    assert list(dataf["session"]) == result
