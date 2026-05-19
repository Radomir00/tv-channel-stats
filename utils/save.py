import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

engine: Engine = create_engine(
    "mysql+pymysql://statsuser:statspass@host.docker.internal:3306/statsdb?charset=utf8mb4",
    pool_pre_ping=True,
    connect_args={
        "charset": "utf8mb4",
    },
)


def delete_existing_data(
    table_name: str,
) -> None:

    query = text(f"""
        DELETE FROM {table_name}
    """)

    try:
        with engine.begin() as conn:
            conn.execute(query)

        print(f"[OK] Deleted existing rows from {table_name}")

    except Exception as e:
        # tabela jos ne postoji
        if "doesn't exist" in str(e):
            print(f"[INFO] Table {table_name} does not exist yet")
            return

        raise


# =========================================================
# SAVE RESULT
# =========================================================


def save_result(
    df: pd.DataFrame,
    table_name: str,
) -> None:

    if df is None or df.empty:
        print(f"[WARN] Empty dataframe for {table_name}")
        return

    try:
        # -------------------------------------------------
        # DELETE EXISTING DATA
        # -------------------------------------------------
        delete_existing_data(
            table_name=table_name,
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
