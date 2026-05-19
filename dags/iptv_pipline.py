import sys
import os

from datetime import datetime

from airflow.decorators import dag, task

sys.path.append("/opt/airflow")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.load import load_event_data, load_event_types
from utils.save import save_result
from processors import get_processor
from datetime import date


def process_event_type(event_type: str):

    print(f"Processing event type: {event_type}")

    processor = get_processor(event_type)

    df = load_event_data(event_type)

    if df.empty:
        print(f"No data for {event_type}")
        return

    event_date = date.today()

    tables = processor(df)

    for table_name, result_df in tables.items():
        if result_df.empty:
            print(f"Skipping empty table: {table_name}")
            continue

        if len(result_df.columns) < 2:
            print(f"Skipping invalid table: {table_name}")
            continue

        result_df["event_date"] = event_date

        result_df["event_type"] = event_type

        full_name = f"{event_type.lower()}_{table_name}"
        save_result(result_df, full_name)

        print(f"Saved table: {table_name}")


@dag(
    dag_id="event_processing_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    tags=["events"],
)
def event_pipeline():

    @task
    def get_types():
        return load_event_types()

    @task
    def process(event_type: str):
        process_event_type(event_type)

    event_types = get_types()

    process.expand(event_type=event_types)


dag = event_pipeline()
