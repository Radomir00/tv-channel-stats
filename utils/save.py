import pandas as pd
from sqlalchemy import create_engine, text, inspect

engine = create_engine(
    "mysql+pymysql://statsuser:statspass@host.docker.internal:3306/statsdb?charset=utf8mb4",
    pool_pre_ping=True,
    connect_args={
        "charset": "utf8mb4",
    },
)


def delete_existing_data(
    table_name: str,
    event_date: str,
    event_type: str,
) -> None:

    query = text(f"""
        DELETE FROM {table_name}
        WHERE event_date = :event_date
        AND event_type = :event_type
    """)

    with engine.begin() as conn:  # type: ignore
        conn.execute(
            query,
            {
                "event_date": event_date,
                "event_type": event_type,
            },
        )

    print(
        f"[OK] Deleted existing rows from {table_name} for {event_date} / {event_type}"
    )


# =========================================================
# SAVE RESULT
# =========================================================


def save_result(
    df: pd.DataFrame,
    table_name: str,
    event_type: str,
) -> None:

    if df is None or df.empty:
        print(f"[WARN] Empty dataframe for {table_name}")
        return

    try:
        # -------------------------------------------------
        # DELETE EXISTING DATA
        # -------------------------------------------------
        inspector = inspect(engine)

        if inspector.has_table(table_name):  # type: ignore
            if "event_date" in df.columns:
                unique_dates = df["event_date"].astype(str).unique()

                for event_date in unique_dates:
                    delete_existing_data(
                        table_name=table_name,
                        event_date=event_date,
                        event_type=event_type,
                    )

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

        print(f"[OK] Inserted {len(df)} rows into {table_name}")

    except Exception as e:
        print(f"[ERROR] Failed inserting into {table_name}")

        print(str(e))

        raise
