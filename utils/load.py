import pandas as pd
from sqlalchemy import create_engine, text

DB_URI = "mysql+pymysql://statsuser:statspass@172.17.0.3:3306/statsdb?charset=utf8mb4"


engine = create_engine(
    DB_URI,
    pool_pre_ping=True,
    connect_args={
        "charset": "utf8mb4",
        "use_unicode": True,
    },
)


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    category_cols = [
        "type",
        "name",
        "devRef",
        "timeZone",
    ]

    for col in category_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    integer_cols = [
        "duration",
    ]

    for col in integer_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col],
                downcast="integer",
            )

    return df


def load_event_types() -> list[str]:
    query = text("""
        SELECT DISTINCT type
        FROM statistic
    """)

    df = pd.read_sql(query, engine)

    return list(df["type"].dropna().astype(str))  # type: ignore


def load_data(chunksize=10_000):
    query = text("""
        SELECT
            devRef,
            name,
            type,
            duration,
            extra,
            insertedTS,
            timeZone
        FROM statistic_2026_04_21
        WHERE duration <= 400000
          AND JSON_VALID(extra)
    """)

    return pd.read_sql_query(
        query, engine.execution_options(stream_results=True), chunksize=chunksize
    )


def load_event_data(event_type: str) -> pd.DataFrame:
    if event_type == "LiveUsage":
        query = text("""
            SELECT
                devRef,
                name,
                duration,
                extra,
                insertedTS,
                timeZone
            FROM statistic
            WHERE type = :event_type
              AND duration <= 400000
              AND JSON_VALID(extra)
        """)
    else:
        query = text("""
            SELECT
                devRef,
                name,
                duration,
                extra
            FROM statistic
            WHERE type = :event_type
        """)

    df = pd.read_sql(
        query,
        engine,
        params={"event_type": event_type},
    )

    df = optimize_dataframe(df)

    return df
