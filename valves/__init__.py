try:
    import polars as pl

    _POLARS_AVAILABLE = True
except ImportError:
    _POLARS_AVAILABLE = False
else:
    from .polars import sessionize as sess_pl
    from .polars import bayes_average as bayes_average_pl

try:
    import pandas as pd

    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False
else:
    from .pandas import sessionize as sess_pd
    from .pandas import bayes_average as bayes_average_pd

try:
    import dask.dataframe as dd

    _DASK_AVAILABLE = True
except ImportError:
    _DASK_AVAILABLE = False
else:
    from .dask import sessionize as sess_dd
    from .dask import bayes_average as bayes_average_dd


def _raise_dataf_error(dataf):
    """Raises error based on availability of dataframe package."""
    installed_packages = []
    if _DASK_AVAILABLE:
        installed_packages.append("dask")
    if _PANDAS_AVAILABLE:
        installed_packages.append("pandas")
    if _POLARS_AVAILABLE:
        installed_packages.append("polars")
    raise ValueError(
        f"Recieved {type(dataf)} dataframe. Are you sure this is installed? We can confirm {installed_packages} are installed."
    )


def sessionize(dataf, user_col="user", ts_col="timestamp", threshold=20 * 60):
    """
    Adds a session to the dataset, using multiple dispatch to proxy the correct backend.

    This function is meant to be used in a `.pipe()`-line.

    Arguments:
        - dataf: pandas, polars or dask dataframe
        - user_col: name of the column containing the user id
        - ts_col: name of the column containing the timestamp
        - threshold: time in seconds to consider a user inactive
    """
    if _DASK_AVAILABLE and isinstance(dataf, dd.DataFrame):
        return sess_dd(dataf, user_col, ts_col, threshold)
    if _PANDAS_AVAILABLE and isinstance(dataf, pd.DataFrame):
        return sess_pd(dataf, user_col, ts_col, threshold)
    if _POLARS_AVAILABLE and isinstance(dataf, pl.DataFrame):
        return sess_pl(dataf, user_col, ts_col, threshold)
    _raise_dataf_error(dataf)


def bayes_average(
    dataf, group_cols, target_col, C, prior_mean=None, out_col="bayes_avg"
):
    r"""
    Computes the Bayes average for a target column.

    The average is calculated per formula found on [wikipedia](https://en.wikipedia.org/wiki/Bayesian_average).

    $$
    \bar{x}=\frac{C m+\sum_{i=1}^{n} x_{i}}{C+n}
    $$

    This function is meant to be used in a `.pipe()`-line.

    Arguments:
        - dataf: dask dataframe
        - group_cols: list of columns to group by
        - target_col: name of the column containing the target value, typically a rating
        - C: smoothing parameter
        - prior_mean: optional, a prior mean to use instead of the mean of the target column
        - out_col: name of the column to output
    """
    if _DASK_AVAILABLE and isinstance(dataf, dd.DataFrame):
        return bayes_average_dd(dataf, group_cols, target_col, C, prior_mean, out_col)
    if _PANDAS_AVAILABLE and isinstance(dataf, pd.DataFrame):
        return bayes_average_pd(dataf, group_cols, target_col, C, prior_mean, out_col)
    if _POLARS_AVAILABLE and isinstance(dataf, pl.DataFrame):
        return bayes_average_pl(dataf, group_cols, target_col, C, prior_mean, out_col)
    _raise_dataf_error(dataf)
