import os
import sys
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any

from datetime import datetime

import pandas as pd

from airflow.decorators import dag, task

sys.path.append("/opt/airflow")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.load import load_data, optimize_dataframe
from utils.save import save_result
from utils.metrics import has_invalid_extra, build_iptv_sessions

from processors import get_processor


OUTPUT_DIR = "/opt/airflow/output"

EVENT_TYPES = [
    "LiveUsage",
    "RESTARTUsage",
    "STARTOVERUsage",
]


@dag(
    dag_id="usage_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    tags=["usage"],
)
def usage_pipeline():

    @task
    def clean_data() -> str:
        print("Loading data in chunks...")

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        total_before = 0
        total_after = 0

        writer = None
        parquet_path = None
        first_date = None

        for i, chunk in enumerate(load_data(chunksize=10_000)):
            chunk = optimize_dataframe(chunk)

            total_before += len(chunk)

            mask = chunk["extra"].map(has_invalid_extra)
            chunk = chunk.loc[~mask].copy()

            if chunk.empty:
                del chunk, mask
                continue

            chunk["insertedTS"] = pd.to_datetime(chunk["insertedTS"])

            if first_date is None:
                first_date = chunk["insertedTS"].dt.date.min()
                parquet_path = f"{OUTPUT_DIR}/iptv_{first_date}.parquet"

            total_after += len(chunk)

            table = pa.Table.from_pandas(
                chunk,
                preserve_index=False,
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    parquet_path,
                    table.schema,
                )

            writer.write_table(table)

            print(f"Chunk {i}: rows={len(chunk)}")

            del chunk, mask, table

        if writer is not None:
            writer.close()

        if parquet_path is None:
            raise ValueError("No valid rows after cleaning")

        print(f"Rows before cleaning: {total_before}")
        print(f"Rows after cleaning: {total_after}")
        print(f"Saved parquet file: {parquet_path}")

        # parquet_path = f"{OUTPUT_DIR}/iptv_2026-05-20.parquet"

        return parquet_path

    @task
    def build_liveusage_sessions(parquet_path: Any) -> str:

        file_name = os.path.basename(parquet_path)
        event_date = file_name.replace("iptv_", "").replace(".parquet", "")

        sessions_path = f"{OUTPUT_DIR}/liveusage_iptv_sessions_{event_date}.parquet"

        df_clean = pd.read_parquet(
            parquet_path,
            filters=[("type", "==", "LiveUsage")],
        )

        if df_clean.empty:
            raise ValueError("No LiveUsage data found")

        sessions = build_iptv_sessions(df_clean)

        if sessions.empty:
            raise ValueError("No LiveUsage sessions created")

        sessions.to_parquet(
            sessions_path,
            index=False,
        )

        print(f"Saved sessions to: {sessions_path}")
        print(f"Rows: {len(sessions)}")

        # sessions_path = f"{OUTPUT_DIR}/liveusage_iptv_sessions_2026-05-20.parquet"

        return sessions_path

    @task
    def process_event_type(
        event_type: str,
        cleaned_path: Any,
        sessions_path: Any,
    ) -> None:
        print(f"Processing event type: {event_type}")

        df_clean = pd.read_parquet(
            cleaned_path,
            filters=[("type", "==", event_type)],
        )

        if df_clean.empty:
            print(f"No data for {event_type}")
            return

        processor = get_processor(event_type)

        if event_type == "LiveUsage":
            df_sessions = pd.read_parquet(sessions_path)

            if df_sessions.empty:
                print("No sessions for LiveUsage")
                return

            tables = processor(df_sessions)

        elif event_type == "RESTARTUsage":
            tables = processor(df_clean, sessions_path)

        else:
            tables = processor(df_clean)

        event_date = df_clean["insertedTS"].dt.date.min()

        for table_name, result_df in tables.items():
            if result_df.empty:
                print(f"Skipping empty table: {table_name}")
                continue

            result_df["event_date"] = event_date

            full_name = f"{event_type.lower()}_{table_name}"

            save_result(
                result_df,
                full_name,
            )

            print(f"Saved table: {full_name}")

    cleaned_path = clean_data()

    sessions_path = build_liveusage_sessions(cleaned_path)

    process_event_type.partial(
        cleaned_path=cleaned_path,
        sessions_path=sessions_path,
    ).expand(
        event_type=EVENT_TYPES,
    )


dag = usage_pipeline()
