import pandas as pd

from utils.metrics import (
    has_invalid_extra,
    count_name_occurrences,
    sum_sessions,
    total_channel_watch_time,
    compute_top_titles,
)


def process(df: pd.DataFrame) -> dict[str, pd.DataFrame]:

    if df.empty:
        return {
            "channels_by_time": pd.DataFrame(),
            "channels_by_users": pd.DataFrame(),
            "top_titles": pd.DataFrame(),
        }

    mask = df["extra"].map(has_invalid_extra)

    df = df.loc[~mask]  # type: ignore

    sum_sess = sum_sessions(df)

    channels_by_time = total_channel_watch_time(sum_sess)

    channels_by_users = count_name_occurrences(sum_sess)

    top_titles = compute_top_titles(df)

    return {
        "channels_by_time": channels_by_time,
        "channels_by_users": channels_by_users,
        "top_titles": top_titles,
    }
