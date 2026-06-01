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
) -> pd.DataFrame:

    if df.empty:
        return pd.DataFrame(columns=["programId", "name", "title", "viewer_count"])

    # Brojanje jedinstvenih gledalaca
    top_titles = (
        df.groupby(
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


def sum_sessions(df: pd.DataFrame, extra=None, dur: str = "duration") -> pd.DataFrame:
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
    ).agg(duration=(dur, "sum"))


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
    df = sum_sessions(df)
    return (
        df.groupby("name", as_index=False, observed=True)
        .agg(total_watch_time=("duration", "sum"))
        .sort_values(by="total_watch_time", ascending=False)
    )


def process_top_name_watch_time(
    df: pd.DataFrame,
) -> pd.DataFrame:

    top_name = count_name_occurrences(df).rename(
        columns={
            "count": "viewer_count",
        }
    )

    watch_time = total_channel_watch_time(df)

    result = top_name.merge(
        watch_time,
        on="name",
        how="outer",
    ).fillna(
        {
            "viewer_count": 0,
            "total_watch_time": 0,
        }
    )

    result["viewer_count"] = result["viewer_count"].astype("int64")

    result["total_watch_time"] = result["total_watch_time"].astype("int64")

    result = result.sort_values(
        by=[
            "viewer_count",
            "total_watch_time",
        ],
        ascending=False,
        ignore_index=True,
    )

    return result


def build_sessions(
    df: pd.DataFrame,
    max_gap_ms: int = 400000,
    heartbeat_ms: int = 300000,
) -> pd.DataFrame:
    group_cols = ["devRef", "name", "extra"]

    df = df.copy()
    df["insertedTS"] = pd.to_datetime(df["insertedTS"])

    df["event_start_ts"] = df["insertedTS"] + pd.to_timedelta(
        heartbeat_ms - df["duration"],
        unit="ms",
    )

    df["event_end_ts"] = df["insertedTS"] + pd.to_timedelta(
        df["duration"],
        unit="ms",
    )

    df = df.sort_values(group_cols + ["event_start_ts"])

    df["prev_end_ts"] = df.groupby(
        group_cols,
        observed=True,
    )["event_end_ts"].shift()

    df["gap"] = (df["event_start_ts"] - df["prev_end_ts"]).dt.total_seconds() * 1000

    df["new_session"] = df["gap"].isna() | (df["gap"] > max_gap_ms)

    df["session_id"] = df.groupby(
        group_cols,
        observed=True,
    )["new_session"].cumsum()

    sessions = df.groupby(
        group_cols + ["session_id"],
        as_index=False,
        observed=True,
    ).agg(
        start_ts=("event_start_ts", "min"),
        end_ts=("event_end_ts", "max"),
        timezone=("timeZone", "first"),
        event_count=("insertedTS", "count"),
        duration=("duration", "sum"),
    )

    invalid_mask = sessions["end_ts"] < sessions["start_ts"]

    start_ts = pd.to_datetime(sessions.loc[invalid_mask, "start_ts"])

    duration_td = pd.to_timedelta(
        sessions.loc[invalid_mask, "duration"],
        unit="ms",
    )

    sessions.loc[invalid_mask, "end_ts"] = start_ts + duration_td

    sessions["total_duration"] = (
        sessions["end_ts"] - sessions["start_ts"]
    ).dt.total_seconds() * 1000

    sessions["start_ts"] = sessions["start_ts"].dt.floor("s")
    sessions["end_ts"] = sessions["end_ts"].dt.floor("s")

    return sessions.drop(columns=["session_id"])


def build_iptv_sessions(
    df: pd.DataFrame,
    max_gap_ms: int = 400000,
    heartbeat_ms: int = 300000,
) -> pd.DataFrame:
    df_sessions = build_sessions(
        df,
        max_gap_ms=max_gap_ms,
        heartbeat_ms=heartbeat_ms,
    )

    df_sessions = extract_columns_from_extra(
        df_sessions,
        "extra",
        ["title", "genre", "startTime", "programId", "duration", "location"],
    )

    df_sessions = df_sessions[df_sessions["duration"] > 0]

    df_sessions["program_start"] = pd.to_datetime(
        df_sessions["startTime"],
        unit="ms",
        utc=True,
    )

    df_sessions.drop(columns=["startTime"], inplace=True)

    df_sessions["offset_hours"] = (
        df_sessions["timezone"].str.extract(r"([+-]\d{2})", expand=False).astype(int)
    )

    df_sessions["program_start_local"] = df_sessions["program_start"].dt.tz_localize(
        None
    ) + pd.to_timedelta(df_sessions["offset_hours"], unit="h")

    df_sessions.drop(
        columns=["timezone", "program_start", "offset_hours"],
        inplace=True,
    )

    columns = [
        "devRef",
        "name",
        "title",
        "genre",
        "programId",
        "location",
        "program_start_local",
        "start_ts",
        "end_ts",
        "duration",
        "total_duration",
    ]

    result: pd.DataFrame = pd.DataFrame(df_sessions.loc[:, columns]).copy()

    return result


def compute_watch_ranges(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df["total_duration"] = pd.to_numeric(
        df["total_duration"],
        errors="coerce",
    )

    df["duration"] = pd.to_numeric(
        df["duration"],
        errors="coerce",
    )

    df["watch_ratio"] = df["total_duration"] / df["duration"]

    conditions = [
        (df["watch_ratio"] >= 0.75),
        ((df["watch_ratio"] >= 0.50) & (df["watch_ratio"] < 0.75)),
        ((df["watch_ratio"] >= 0.25) & (df["watch_ratio"] < 0.50)),
        (df["watch_ratio"] < 0.25),
    ]

    choices = [
        "75-100%",
        "50-75%",
        "25-50%",
        "0-25%",
    ]

    df["watch_group"] = pd.NA

    for cond, value in zip(
        conditions,
        choices,
    ):
        df.loc[
            cond,
            "watch_group",
        ] = value

    grouped = (
        df.groupby(
            [
                "name",
                "title",
                "programId",
                "watch_group",
            ],
            observed=True,
        )["devRef"]
        .nunique()
        .unstack(fill_value=0)
        .reset_index()
    )

    # osiguraj da sve kolone postoje
    for col in choices:
        if col not in grouped.columns:
            grouped[col] = 0

    result = grouped[
        [
            "name",
            "title",
            "programId",
            "75-100%",
            "50-75%",
            "25-50%",
            "0-25%",
        ]
    ]

    result = result.sort_values(
        by=[
            "75-100%",
            "50-75%",
            "25-50%",
            "0-25%",
        ],
        ascending=False,
    )  # type: ignore

    return result


def merge_name_with_fallback(
    df: pd.DataFrame,
    df_live: pd.DataFrame,
) -> pd.DataFrame:

    strict_map = df_live.drop_duplicates(["programId", "title"]).set_index(
        ["programId", "title"]
    )["name"]

    df["name"] = df.set_index(["programId", "title"]).index.map(strict_map)  # type: ignore

    title_map = df_live.drop_duplicates(["title"]).set_index("title")["name"]

    missing = df["name"].isna()
    df.loc[missing, "name"] = df.loc[missing, "title"].map(title_map)  # type: ignore

    return df
