import os
import sys
import pyarrow as pa
import pyarrow.parquet as pq
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
CLEAN_PATH = f"{OUTPUT_DIR}/iptv_2026-05-20.parquet"
SESSIONS_PATH = f"{OUTPUT_DIR}/liveusage_iptv_sessions.parquet"

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
    def clean_data():

        print("Loading data in chunks...")

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        writers = {}

        total_before = 0
        total_after = 0

        for i, chunk in enumerate(load_data(chunksize=10_000)):
            chunk = optimize_dataframe(chunk)

            total_before += len(chunk)

            mask = chunk["extra"].map(has_invalid_extra)
            chunk = chunk.loc[~mask].copy()

            if chunk.empty:
                del chunk, mask
                continue

            total_after += len(chunk)

            chunk["insertedTS"] = pd.to_datetime(chunk["insertedTS"])
            chunk["event_date"] = chunk["insertedTS"].dt.date.astype(str)

            for event_date, df_date in chunk.groupby("event_date"):
                file_path = f"{OUTPUT_DIR}/iptv_{event_date}.parquet"

                df_date = df_date.drop(columns=["event_date"])

                table = pa.Table.from_pandas(
                    df_date,
                    preserve_index=False,
                )

                if event_date not in writers:
                    writers[event_date] = pq.ParquetWriter(
                        file_path,
                        table.schema,
                    )

                writers[event_date].write_table(table)

                print(f"Chunk {i}, date={event_date}: rows={len(df_date)}")

                del df_date, table

            del chunk, mask

        for writer in writers.values():
            writer.close()

        if not writers:
            raise ValueError("No valid rows after cleaning")

        print(f"Rows before cleaning: {total_before}")
        print(f"Rows after cleaning: {total_after}")
        print(f"Saved parquet files to: {OUTPUT_DIR}")

        return CLEAN_PATH

    @task
    def build_liveusage_sessions(parquet_path: str):
        df_clean = pd.read_parquet(parquet_path, filters=[("type", "==", "LiveUsage")])

        sessions = build_iptv_sessions(df_clean)

        if sessions.empty:
            raise ValueError("No LiveUsage sessions created")

        sessions.to_parquet(
            SESSIONS_PATH,
            index=False,
        )

        print(f"Rows: {len(sessions)}")

        return SESSIONS_PATH

    @task
    def process_event_type(event_type: str, cleaned_path: str, sessions_path: str):

        print(f"Processing event type: {event_type}")

        df_clean = pd.read_parquet(cleaned_path, filters=[("type", "==", event_type)])

        df_sessions = pd.read_parquet(sessions_path)

        if df_clean.empty:
            print(f"No data for {event_type}")
            return

        processor = get_processor(event_type)

        if event_type == "LiveUsage":
            tables = processor(df_sessions)  # type: ignore
        else:
            tables = processor(df_clean)  # type: ignore

        for table_name, result_df in tables.items():
            if result_df.empty:
                print(f"Skipping empty table: {table_name}")
                continue

            result_df["event_date"] = df_clean["insertedTS"].dt.date.max()

            full_name = f"{event_type.lower()}_{table_name}"

            save_result(
                result_df,
                full_name,
            )

            print(f"Saved table: {full_name}")

    cleaned_path = clean_data()
    sessions_path = build_liveusage_sessions(cleaned_path)  # type: ignore

    process_event_type.partial(
        cleaned_path=cleaned_path, sessions_path=sessions_path
    ).expand(
        event_type=EVENT_TYPES,
    )


dag = usage_pipeline()
