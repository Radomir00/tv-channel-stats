from utils.metrics import count_name_occurrences
import pandas as pd


def process(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if df.empty:
        return {
            "top name": pd.DataFrame(),
        }
    top_name = count_name_occurrences(df, min_duration=0)
    return {"top_name": top_name}
