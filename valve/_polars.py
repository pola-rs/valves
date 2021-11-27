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
    return (dataf
             .sort([user_col, ts_col])
             .with_columns([
                 (pl.col("timestamp").diff().cast(pl.Int64) > threshold).fill_null(True).alias("ts_diff"),
                 (pl.col(user_col).diff() != 0).fill_null(True).alias("user_diff"),
             ])
             .with_column((pl.col("ts_diff") | pl.col("user_diff")).alias("new_session_mark"))
             .with_column(pl.col("new_session_mark").cumsum().alias("session"))
             .drop(["user_diff", "ts_diff", "new_session_mark"]))
