import pandas as pd
import json


def extract_from_json(
    df: pd.DataFrame, column: str, new_column: str, key: str
) -> pd.DataFrame:
    """
    Izvlači vrijednost iz JSON kolone.
    """

    def _extract(value):
        if pd.isna(value):
            return None
        try:
            return json.loads(value).get(key)
        except (json.JSONDecodeError, TypeError):
            return None

    df = df.copy()
    df[new_column] = df[column].apply(_extract)
    return df


def extract_columns_from_extra(
    df: pd.DataFrame,
    column: str,
    keys: list[str],
    drop_source: bool = True,
) -> pd.DataFrame:
    """
    Koristi extract_from_json za više kolona iz JSON polja.

    keys: lista JSON ključeva koji će postati kolone istog imena
    """

    df = df.copy()

    for key in keys:
        df = extract_from_json(
            df=df,
            column=column,
            new_column=key,
            key=key,
        )

    if drop_source:
        df = df.drop(columns=[column])

    return df


def compute_top_titles(
    df: pd.DataFrame, min_watch_percentage: float = 0.75
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["programId", "name", "title", "viewer_count"])

    df = df.copy()

    # -------------------------
    # 1. Rename session duration
    # -------------------------
    df = df.rename(columns={"duration": "session_duration"})

    # -------------------------
    # 2. Extract iz extra
    # -------------------------
    df = extract_columns_from_extra(
        df,
        column="extra",
        keys=["title", "programId", "duration"],
    )

    df = df.reset_index(drop=True)

    # -------------------------
    # 3. Watch time po useru
    # -------------------------
    df_user = df.groupby(
        ["devRef", "programId", "name", "title", "duration"],
        as_index=False,
    ).agg(user_watch_time=("session_duration", "sum"))

    # -------------------------
    # 4. Filter (>= 75%)
    # -------------------------
    df_user = df_user[
        df_user["user_watch_time"] >= min_watch_percentage * df_user["duration"]
    ]

    # -------------------------
    # 5. Top titles
    # -------------------------
    top_titles = (
        df_user.groupby(["programId", "name", "title"])["devRef"]
        .count()
        .reset_index(name="viewer_count")
        .sort_values(by="viewer_count", ascending=False)
    )

    top_titles = top_titles.reset_index(drop=True)
    return top_titles


def has_invalid_extra(x):
    """
    Provjerava da li extra JSON ima validne vrijednosti.
    """
    try:
        data = json.loads(x) if x else {}

        for k in ["title", "genre", "startTime", "programId", "duration"]:
            if k not in data:
                return True

            val = data[k]

            if isinstance(val, str):
                val = val.strip().lower()

            if val in [None, "", "other", "null", "no title", "0", "-1"]:
                return True

        return False

    except Exception:
        return True


def sum_sessions(df: pd.DataFrame, extra=None) -> pd.DataFrame:
    group_cols = ["devRef", "name"]

    if extra:
        if isinstance(extra, list):
            group_cols.extend(extra)
        else:
            group_cols.append(extra)

    return df.groupby(group_cols, as_index=False).agg(duration=("duration", "sum"))


def count_name_occurrences(
    df: pd.DataFrame,
    min_duration: int = 30000,
    group_cols: str | list[str] = "name",
) -> pd.DataFrame:

    if df.empty:
        return pd.DataFrame()

    if "duration" not in df.columns:
        raise ValueError("Column 'duration' does not exist in DataFrame")

    # osiguraj da je lista
    if isinstance(group_cols, str):
        group_cols = [group_cols]

    # provjera da kolone postoje
    missing = [col for col in group_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}")

    filtered = df[(df["duration"].isna()) | (df["duration"] >= min_duration)]

    return (
        filtered.groupby(group_cols, as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values(by="count", ascending=False)
    )


def total_channel_watch_time(df: pd.DataFrame) -> pd.DataFrame:
    df = sum_sessions(df)
    return (
        df.groupby("name", as_index=False)
        .agg(total_watch_time=("duration", "sum"))
        .sort_values(by="total_watch_time", ascending=False)
    )
