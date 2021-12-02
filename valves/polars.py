import polars as pl


def sessionize(dataf, user_col="user", ts_col="timestamp", threshold=20 * 60):
    """
    Adds a session to the dataset.

    This function is meant to be used in a `.pipe()`-line.

    Arguments:
        - dataf: polars dataframe
        - user_col: name of the column containing the user id
        - ts_col: name of the column containing the timestamp
        - threshold: time in seconds to consider a user inactive
    """
    threshold = threshold * 1000
    return (
        dataf.sort([user_col, ts_col])
        .with_columns(
            [
                (pl.col("timestamp").diff().cast(pl.Int64) > threshold)
                .fill_null(True)
                .alias("ts_diff"),
                (pl.col(user_col).diff() != 0).fill_null(True).alias("user_diff"),
            ]
        )
        .with_column(
            (pl.col("ts_diff") | pl.col("user_diff")).alias("new_session_mark")
        )
        .with_column(pl.col("new_session_mark").cumsum().alias("session"))
        .drop(["user_diff", "ts_diff", "new_session_mark"])
    )


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
        - dataf: polars dataframe
        - group_cols: list of columns to group by
        - target_col: name of the column containing the target value, typically a rating
        - C: smoothing parameter
        - prior_mean: optional, a prior mean to use instead of the mean of the target column
        - out_col: name of the column to output
    """
    if prior_mean is None:
        prior_mean = dataf[target_col].mean()
    return dataf.with_column(
        (
            ((prior_mean * C) + pl.col(target_col).sum().over(group_cols))
            / (C + pl.col(target_col).count().over(group_cols))
        ).alias(out_col)
    )


def item_item_counts(dataf, user_col="user", item_col="item"):
    """
    Computers item-item overlap counts from user-item interactions, useful for recommendations.

    This function is meant to be used in a `.pipe()`-line.

    Arguments:
        - dataf: polars dataframe
        - user_col: name of the column containing the user id
        - item_col: name of the column containing the item id
    """
    return (
        dataf.with_columns(
            [
                pl.col(item_col).list().over("user").alias(f"{item_col}_rec"),
            ]
        )
        .explode(f"{item_col}_rec")
        .filter(pl.col(item_col) != pl.col(f"{item_col}_rec"))
        .with_columns(
            [
                pl.col(user_col)
                .n_unique()
                .over(pl.col(item_col))
                .alias(f"n_{item_col}"),
                pl.col(user_col)
                .n_unique()
                .over(f"{item_col}_rec")
                .alias(f"n_{item_col}_rec"),
                pl.col(user_col)
                .n_unique()
                .over([pl.col(item_col), f"{item_col}_rec"])
                .alias("n_both"),
            ]
        )
        .select(
            [
                f"{item_col}",
                f"{item_col}_rec",
                f"n_{item_col}",
                f"n_{item_col}_rec",
                "n_both",
            ]
        )
        .drop_duplicates()
    )
