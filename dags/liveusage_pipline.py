import os
import sys

from datetime import datetime, date

import pandas as pd

from airflow.decorators import dag, task

sys.path.append("/opt/airflow")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.load import load_event_data
from utils.save import save_result

from utils.metrics import (
    build_iptv_sessions,
    compute_top_titles,
    compute_watch_ranges,
    has_invalid_extra,
    process_top_name_watch_time,
)

EVENT_TYPE = "LiveUsage"

OUTPUT_DIR = "/opt/airflow/output"

CLEAN_PATH = f"{OUTPUT_DIR}/liveusage_clean.parquet"

SESSIONS_PATH = f"{OUTPUT_DIR}/liveusage_iptv_sessions.parquet"


def get_metric_function(name):

    mapping = {
        "top_name_watch_time": process_top_name_watch_time,
        "top_titles": compute_top_titles,
        "watch_range": compute_watch_ranges,
    }

    return mapping[name]


@dag(
    dag_id="liveusage_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=3,
    tags=["liveusage"],
)
def liveusage_pipeline():

    @task
    def metric_names():

        # 1. Povuce metric names

        return [
            "top_name_watch_time",
            "top_titles",
            "watch_range",
        ]

    @task
    def clean_data():

        # # 2. Ucita LiveUsage
        # # 3. Provjeri invalid_extra

        # print("Loading LiveUsage data...")

        # df = load_event_data(EVENT_TYPE)

        # if df.empty:
        #     raise ValueError("No LiveUsage data found")

        # print(f"Rows before cleaning: {len(df)}")

        # mask = df["extra"].map(has_invalid_extra)

        # df = df.loc[~mask].copy()

        # print(f"Rows after cleaning: {len(df)}")

        # if df.empty:
        #     raise ValueError("No valid rows after cleaning")

        # os.makedirs(
        #     OUTPUT_DIR,
        #     exist_ok=True,
        # )

        # df.to_parquet(
        #     CLEAN_PATH,
        #     index=False,
        # )

        return CLEAN_PATH

    @task
    def build_sessions(
        parquet_path: str,
    ):

        # # 4. build_iptv_session

        # print("Building IPTV sessions...")

        # df = pd.read_parquet(parquet_path)

        # sessions = build_iptv_sessions(df)

        # if sessions.empty:
        #     raise ValueError("No IPTV sessions built")

        # print(f"Built IPTV sessions: {len(sessions)}")

        # sessions.to_parquet(
        #     SESSIONS_PATH,
        #     index=False,
        # )

        return SESSIONS_PATH

    @task
    def process_metric(
        metric_name: str,
        sessions_path: str,
    ):

        print(f"Processing metric: {metric_name}")

        sessions = pd.read_parquet(sessions_path)

        if sessions.empty:
            print("No sessions found")
            return

        metric_func = get_metric_function(metric_name)

        result = metric_func(sessions)

        if result.empty:
            print(f"Empty result for {metric_name}")
            return

        result["event_date"] = date.today()

        save_result(
            result,
            f"liveusage_{metric_name}",
        )

        print(f"Saved liveusage_{metric_name}")

    cleaned_path = clean_data()

    sessions_path = build_sessions(cleaned_path)  # type: ignore

    process_metric.partial(
        sessions_path=sessions_path,
    ).expand(metric_name=metric_names())


dag = liveusage_pipeline()
