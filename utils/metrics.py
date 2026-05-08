import pandas as pd
import orjson

INVALID_VALUES = {
    None,
    "",
    "other",
    "null",
    "no title",
    "0",
    "-1",
}

REQUIRED_KEYS = (
    "title",
    "genre",
    "startTime",
    "programId",
    "duration",
)


def has_invalid_extra(x):

    data = orjson.loads(x)

    for key in REQUIRED_KEYS:
        val = data.get(key)

        if isinstance(val, str):
            val = val.strip().lower()

        if val in INVALID_VALUES:
            return True

    return False


def extract_columns_from_extra(
    df: pd.DataFrame,
    column: str,
    keys: list[str],
    drop_source: bool = True,
) -> pd.DataFrame:

    parsed = df[column].map(
        lambda x: orjson.loads(x) if x else {}  # type: ignore
    )

    for key in keys:
        df[key] = parsed.str.get(key)

        if df[key].dtype == "object":
            df[key] = df[key].astype("category")

    if drop_source:
        df.drop(columns=[column], inplace=True)

    del parsed

    return df


def compute_top_titles(
    df: pd.DataFrame,
    min_watch_percentage: float = 0.75,
) -> pd.DataFrame:

    if df.empty:
        return pd.DataFrame(columns=["programId", "name", "title", "viewer_count"])

    # Rename session duration
    df = df.rename(columns={"duration": "session_duration"})

    # Izvlačenje podataka iz extra kolone
    df = extract_columns_from_extra(
        df,
        column="extra",
        keys=["title", "programId", "duration"],
    )

    df["duration"] = pd.to_numeric(df["duration"], errors="coerce")

    # Ukupno vreme gledanja po korisniku
    df_user = df.groupby(
        ["devRef", "programId", "name", "title", "duration"],
        observed=True,
        as_index=False,
    ).agg(user_watch_time=("session_duration", "sum"))

    # Filter: korisnik mora odgledati dovoljno
    threshold = min_watch_percentage * df_user["duration"]

    df_user = df_user[df_user["user_watch_time"] >= threshold]

    # Brojanje jedinstvenih gledalaca
    top_titles = (
        df_user.groupby(
            ["programId", "name", "title"],
            observed=True,
        )["devRef"]
        .nunique()
        .reset_index(name="viewer_count")
        .sort_values(
            by="viewer_count",
            ascending=False,
            ignore_index=True,
        )
    )

    return top_titles


def sum_sessions(df: pd.DataFrame, extra=None) -> pd.DataFrame:
    group_cols = ["devRef", "name"]

    if extra:
        if isinstance(extra, list):
            group_cols.extend(extra)
        else:
            group_cols.append(extra)

    return df.groupby(
        group_cols,
        as_index=False,
        observed=True,
    ).agg(duration=("duration", "sum"))


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
        filtered.groupby(
            group_cols,
            as_index=False,
            observed=True,
        )
        .size()
        .rename(columns={"size": "count"})  # type: ignore
        .sort_values(by="count", ascending=False)
    )


def total_channel_watch_time(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("name", as_index=False, observed=True)
        .agg(total_watch_time=("duration", "sum"))
        .sort_values(by="total_watch_time", ascending=False)
    )
