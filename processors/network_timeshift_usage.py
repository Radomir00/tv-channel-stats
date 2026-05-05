import pandas as pd
from utils.metrics import sum_sessions, count_name_occurrences


def process(df: pd.DataFrame) -> pd.DataFrame:
    result = count_name_occurrences(df, 0)
    return result
