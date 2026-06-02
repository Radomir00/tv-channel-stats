import pandas as pd

from utils.metrics import (
    count_name_occurrences,
    sum_sessions,
    extract_columns_from_extra,
    merge_name_with_fallback,
)


def process(
    df: pd.DataFrame,
    sessions_path: str,
) -> dict[str, pd.DataFrame]:

    if df.empty:
        return {
            "result": pd.DataFrame(),
        }

    df = extract_columns_from_extra(
        df,
        column="extra",
        keys=["programId", "title"],
    )

    df.drop(columns=["name"], inplace=True)

    df_live = pd.read_parquet(
        sessions_path,
        columns=["programId", "title", "name"],
    )

    df_live = df_live.drop_duplicates(["programId", "title"])

    df = merge_name_with_fallback(df, df_live)

    sum_sess = sum_sessions(df, ["programId", "title"])

    result = count_name_occurrences(
        sum_sess,
        0,
        ["name", "title", "programId"],
    )

    return {"result": result}
