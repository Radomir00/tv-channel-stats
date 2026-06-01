import pandas as pd
from utils.metrics import (
    count_name_occurrences,
    sum_sessions,
)


def process(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if df.empty:
        return {
            "result": pd.DataFrame(),
        }
    df = df.copy()

    df = df.reset_index(drop=True)

    sum_sess = sum_sessions(df)

    result = count_name_occurrences(sum_sess, 0)
    return {"result": result}
