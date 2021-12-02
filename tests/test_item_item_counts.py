import pandas as pd
import polars as pl
import dask.dataframe as dd

from valves.polars import item_item_counts as ii_pl
from valves.pandas import item_item_counts as ii_pd
from valves.dask import item_item_counts as ii_dd
from valves import item_item_counts

going_in = [
    {"user": 1, "item": 1},
    {"user": 1, "item": 2},
    {"user": 1, "item": 3},
    {"user": 2, "item": 2},
    {"user": 2, "item": 3},
    {"user": 2, "item": 4},
]

going_out = [
    {"item": 1, "item_rec": 2, "n_item": 1, "n_item_rec": 2, "n_both": 1},
    {"item": 1, "item_rec": 3, "n_item": 1, "n_item_rec": 2, "n_both": 1},
    {"item": 2, "item_rec": 1, "n_item": 2, "n_item_rec": 1, "n_both": 1},
    {"item": 2, "item_rec": 3, "n_item": 2, "n_item_rec": 2, "n_both": 2},
    {"item": 3, "item_rec": 1, "n_item": 2, "n_item_rec": 1, "n_both": 1},
    {"item": 3, "item_rec": 2, "n_item": 2, "n_item_rec": 2, "n_both": 2},
    {"item": 2, "item_rec": 4, "n_item": 2, "n_item_rec": 1, "n_both": 1},
    {"item": 3, "item_rec": 4, "n_item": 2, "n_item_rec": 1, "n_both": 1},
    {"item": 4, "item_rec": 2, "n_item": 1, "n_item_rec": 2, "n_both": 1},
    {"item": 4, "item_rec": 3, "n_item": 1, "n_item_rec": 2, "n_both": 1},
]


def test_pandas_item_item_counts():
    """It needs to work on some simple pandas datasets"""
    result = pd.DataFrame(going_in).pipe(ii_pd).to_dict(orient="records")
    assert result == going_out
    result = pd.DataFrame(going_in).pipe(item_item_counts).to_dict(orient="records")
    assert result == going_out


def test_dask_item_item_counts():
    """It needs to work on some simple dask datasets"""
    dask_df = dd.from_pandas(pd.DataFrame(going_in), npartitions=1)
    result = dask_df.pipe(ii_dd).compute().to_dict(orient="records")
    assert result == going_out
    result = dask_df.pipe(item_item_counts).compute().to_dict(orient="records")
    assert result == going_out


def test_polars_item_item_counts():
    """It needs to work on some simple polars datasets"""
    result = pl.DataFrame(going_in).pipe(ii_pl).to_pandas().to_dict(orient="records")
    assert result == going_out
    result = (
        pl.DataFrame(going_in)
        .pipe(item_item_counts)
        .to_pandas()
        .to_dict(orient="records")
    )
    assert result == going_out
