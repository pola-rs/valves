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

def test_pandas_sessionize1():
    dataf = (pd.DataFrame(data_three_sessions)
                .assign(timestamp=lambda d: pd.to_datetime(d['timestamp']))
                .pipe(sess_pd))
    assert dataf.session.tolist() == [1, 1, 2, 2]


def test_pandas_sessionize2():
    dataf = (pd.DataFrame(data_four_sessions)
                .assign(timestamp=lambda d: pd.to_datetime(d['timestamp']))
                .pipe(sess_pd))
    assert dataf.session.tolist() == [1, 1, 2, 3, 4]


def test_polars_sessionize1():
    dataf = (pl.DataFrame(data_three_sessions)
              .with_column(pl.col("timestamp").str.strptime(pl.Datetime))
              .pipe(sess_pl))
    assert list(dataf['session']) == [1, 1, 2, 2]


def test_polars_sessionize2():
    dataf = (pl.DataFrame(data_four_sessions)
              .with_column(pl.col("timestamp").str.strptime(pl.Datetime))
              .pipe(sess_pl))
    assert list(dataf['session']) == [1, 1, 2, 3, 4]
