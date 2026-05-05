from utils.metrics import count_name_occurrences
import pandas as pd


def process(df: pd.DataFrame) -> pd.DataFrame:
    return count_name_occurrences(df)
