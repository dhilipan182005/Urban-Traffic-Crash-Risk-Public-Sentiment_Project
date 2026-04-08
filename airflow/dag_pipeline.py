from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="traffic_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command="python ingestion/chicago_api.py"
    )

    silver = BashOperator(
        task_id="silver",
        bash_command="python processing/silver_chicago.py"
    )

    gold = BashOperator(
        task_id="gold",
        bash_command="python processing/gold_aggregation.py"
    )

    ingest >> silver >> gold