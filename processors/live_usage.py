import pandas as pd

from utils.metrics import (
    build_iptv_sessions,
    sum_sessions,
    process_top_name_watch_time,
    compute_top_titles,
    compute_watch_ranges,
)

SESSIONS_PATH = "/opt/airflow/output/liveusage_iptv_sessions.parquet"


def process(
    df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:

    if df.empty:
        return {
            "top_name_watch_time": pd.DataFrame(),
            "top_titles": pd.DataFrame(),
            "watch_range": pd.DataFrame(),
        }

    sum_sess = sum_sessions(df, dur="total_duration")

    top_name_watch_time = process_top_name_watch_time(sum_sess)

    top_titles = compute_top_titles(df)

    watch_range = compute_watch_ranges(df)

    return {
        "top_name_watch_time": top_name_watch_time,
        "top_titles": top_titles,
        "watch_range": watch_range,
    }
