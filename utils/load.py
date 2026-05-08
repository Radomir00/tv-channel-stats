import pandas as pd
from sqlalchemy import create_engine, text

DB_URI = "mysql+pymysql://statsuser:statspass@host.docker.internal:3306/statsdb"

engine = create_engine(DB_URI)


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    category_cols = [
        "type",
        "name",
        "devRef",
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


def load_event_data(event_type: str) -> pd.DataFrame:
    if event_type == "LiveUsage":
        query = text("""
            SELECT
                devRef,
                name,
                duration,
                extra
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
