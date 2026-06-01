import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

engine: Engine = create_engine(
    "mysql+pymysql://statsuser:statspass@209.38.253.92:3306/statsdb",
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=1,
    max_overflow=0,
    connect_args={
        "charset": "utf8mb4",
        "connect_timeout": 60,
        "read_timeout": 600,
        "write_timeout": 600,
    },
)


def prepare_for_mysql(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.select_dtypes(include=["category"]).columns:
        df[col] = df[col].astype(str)

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)

        elif df[col].dtype == "object":
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("\x00", "", regex=False)
                .str.slice(0, 1000)
            )

    df = df.where(pd.notnull(df), None)

    return df


def make_safe_records(
    records: list[dict],
    columns: list[str],
) -> tuple[list[dict], dict[str, str]]:
    param_map = {col: f"p{i}" for i, col in enumerate(columns)}

    safe_records = []

    for row in records:
        safe_row = {param_map[col]: row[col] for col in columns}

        safe_records.append(safe_row)

    return safe_records, param_map


def save_result(df: pd.DataFrame, table_name: str) -> None:
    if df is None or df.empty:
        print(f"[WARN] Empty dataframe for {table_name}")
        return

    try:
        df = prepare_for_mysql(df)

        print(f"[INFO] Saving {len(df)} rows into {table_name}")
        print(
            "Memory MB:",
            round(
                df.memory_usage(deep=True).sum() / 1024 / 1024,
                2,
            ),
        )

        df.head(0).to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )

        print("Table created")

        columns = list(df.columns)

        records = df.to_dict(orient="records")

        safe_records, param_map = make_safe_records(
            records=records,
            columns=columns,
        )

        cols_sql = ", ".join(f"`{c}`" for c in columns)
        vals_sql = ", ".join(f":{param_map[c]}" for c in columns)

        insert_sql = text(
            f"""
            INSERT INTO `{table_name}`
            ({cols_sql})
            VALUES ({vals_sql})
            """
        )

        chunk_size = 500

        for start in range(0, len(safe_records), chunk_size):
            end = min(start + chunk_size, len(safe_records))
            batch = safe_records[start:end]

            try:
                with engine.begin() as conn:
                    conn.execute(insert_sql, batch)

            except Exception:
                print(f"[WARN] Batch failed {start}-{end}, testing row by row")

                original_batch = records[start:end]

                for offset, row in enumerate(batch):
                    i = start + offset

                    print(f"Testing row {i}")

                    try:
                        with engine.begin() as conn:
                            conn.execute(insert_sql, [row])

                    except Exception:
                        print("=" * 80)
                        print(f"FAILED ROW: {i}")
                        print(original_batch[offset])
                        print("=" * 80)
                        raise

        print(f"[OK] Finished inserting {len(df)} rows")

    except Exception as e:
        print(f"[ERROR] Failed inserting into {table_name}")
        print(str(e))
        raise
