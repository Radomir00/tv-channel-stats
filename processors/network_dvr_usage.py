import pandas as pd
from utils.metrics import sum_sessions, total_channel_watch_time


def process(df: pd.DataFrame) -> pd.DataFrame:
    df_device = sum_sessions(df)

    result = total_channel_watch_time(df_device)
    return result
