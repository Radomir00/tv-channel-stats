import pandas as pd
from utils.metrics import (
    count_name_occurrences,
    sum_sessions,
    has_invalid_extra,
    extract_columns_from_extra,
)


def process(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if df.empty:
        return {
            "result": pd.DataFrame(),
        }

    df = df.copy()
    mask = df["extra"].apply(has_invalid_extra)
    df = df[~mask]
    df = df[df["duration"] <= 400000]

    df = extract_columns_from_extra(
        df,
        column="extra",
        keys=["programId"],
    )

    df = df.reset_index(drop=True)

    sum_sess = sum_sessions(df, ["programId"])

    result = count_name_occurrences(sum_sess, 0, ["name", "programId"])

    return {"result": result}
