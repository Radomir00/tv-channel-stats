import pandas as pd

from utils.metrics import (
    has_invalid_extra,
    count_name_occurrences,
    sum_sessions,
    total_channel_watch_time,
    compute_top_titles,
)


def process(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "channels_by_time": pd.DataFrame(),
            "channels_by_users": pd.DataFrame(),
            "top_titles": pd.DataFrame(),
        }

    df = df.copy()

    mask = df["extra"].apply(has_invalid_extra)
    df = df[~mask]
    df = df[df["duration"] <= 400000]

    # TOP kanali po vremenu
    channels_by_time = total_channel_watch_time(df).head(100)

    # TOP kanali po korisnicima
    sum_sess = sum_sessions(df)
    channels_by_users = count_name_occurrences(sum_sess).head(100)

    # TOP titles
    top_titles = compute_top_titles(df).head(100)

    return {
        "channels_by_time": channels_by_time,
        "channels_by_users": channels_by_users,
        "top_titles": top_titles,
    }
