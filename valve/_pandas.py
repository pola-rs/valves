import pandas as pd 


def sessionize(dataf, user_col="user", ts_col="timestamp", threshold=20 * 60):
    """
    Adds a session to the dataset.

    This function is meant to be used in a `.pipe()`-line. 

    Arguments:
        - dataf: pandas dataframe 
        - user_col: name of the column containing the user id
        - ts_col: name of the column containing the timestamp
        - threshold: time in seconds to consider a user inactive
    """
    return (dataf
             .sort_values([user_col, ts_col])
             .assign(ts_diff=lambda d: (d[ts_col] - d[ts_col].shift()).dt.seconds > threshold,
                     char_diff=lambda d: (d[user_col].diff() != 0),
                     new_session_mark=lambda d: d['ts_diff'] | d['char_diff'],
                     session=lambda d: d['new_session_mark'].fillna(0).cumsum())
             .drop(columns=['char_diff', 'ts_diff', 'new_session_mark']))
