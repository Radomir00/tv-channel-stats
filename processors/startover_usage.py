import pandas as pd
from utils.metrics import (
    count_name_occurrences,
    sum_sessions,
    extract_columns_from_extra,
)


def process(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if df.empty:
        return {
            "result": pd.DataFrame(),
        }

    df = extract_columns_from_extra(
        df,
        column="extra",
        keys=["title", "programId"],
    )

    df = df.reset_index(drop=True)

    sum_sess = sum_sessions(df, ["programId", "title"])

    result = count_name_occurrences(sum_sess, 0, ["name", "title", "programId"])
    return {"result": result}
