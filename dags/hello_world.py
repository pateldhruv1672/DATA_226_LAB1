from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="example_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["example"],
) as dag:

    @task
    def extract():
        # pretend we fetched something
        print("Extact Process Initiated")
        return {"a": 1, "b": 2, "c": 3}

    @task
    def transform(data: dict):
        # simple transform
        return {k: v * 10 for k, v in data.items()}

    @task
    def load(rows: dict):
        print("Loading rows:", rows)

    load(transform(extract()))
