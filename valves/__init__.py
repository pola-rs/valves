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
    if "dask" in str(dataf.__class__):
        # Import dask-related stuff only if dask is detected
        from .dask import sessionize as sess_dd

        return sess_dd(dataf, user_col, ts_col, threshold)
    if "pandas" in str(dataf.__class__):
        # Import pandas-related stuff only if pandas is detected
        from .pandas import sessionize as sess_pd

        return sess_pd(dataf, user_col, ts_col, threshold)
    if "polars" in str(dataf.__class__):
        # Import polars-related stuff only if polars is detected
        from .polars import sessionize as sess_pl

        return sess_pl(dataf, user_col, ts_col, threshold)


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
    if "dask" in str(dataf.__class__):
        # Import dask-related stuff only if dask is detected
        from .dask import bayes_average as bayes_dd

        return bayes_dd(dataf, group_cols, target_col, C, prior_mean, out_col)
    if "pandas" in str(dataf.__class__):
        # Import pandas-related stuff only if pandas is detected
        from .pandas import bayes_average as bayes_pd

        return bayes_pd(dataf, group_cols, target_col, C, prior_mean, out_col)
    if "polars" in str(dataf.__class__):
        # Import polars-related stuff only if polars is detected
        from .polars import sessionize as bayes_pl

        return bayes_pl(dataf, group_cols, target_col, C, prior_mean, out_col)
