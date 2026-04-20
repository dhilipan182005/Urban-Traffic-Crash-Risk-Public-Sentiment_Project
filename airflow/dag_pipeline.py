from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="traffic_pipeline",
    description="Urban Traffic & YouTube Sentiment Pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ingestion", "silver", "gold", "hive", "traffic", "youtube"],
) as dag:

    youtube_ingest = BashOperator(
        task_id="youtube_ingest",
        bash_command="/workspace/run.sh ingestion/youtube_api.py",
    )

    chicago_ingest = BashOperator(
        task_id="chicago_ingest",
        bash_command="/workspace/run.sh ingestion/chicago_api.py",
    )

    silver_youtube = BashOperator(
        task_id="silver_youtube",
        bash_command="/workspace/run.sh processing/silver_youtube.py",
    )

    silver_chicago = BashOperator(
        task_id="silver_chicago",
        bash_command="/workspace/run.sh processing/silver_chicago.py",
    )

    gold_layer = BashOperator(
        task_id="gold_layer",
        bash_command="/workspace/run.sh processing/gold_layer.py",
    )

    create_hive_tables = BashOperator(
        task_id="create_hive_tables",
        bash_command="hive -f /workspace/sql/hive_tables.sql",
    )

    validation = BashOperator(
        task_id="validation",
        bash_command="/workspace/run.sh processing/validation.py",
    )

    pipeline_summary = BashOperator(
        task_id="pipeline_summary",
        bash_command=(
            "echo '[INFO] $(date +\"%Y-%m-%d %H:%M:%S\") - Pipeline execution summary' && "
            "echo '[SUCCESS] $(date +\"%Y-%m-%d %H:%M:%S\") - Ingestion completed' && "
            "echo '[SUCCESS] $(date +\"%Y-%m-%d %H:%M:%S\") - Silver layer completed' && "
            "echo '[SUCCESS] $(date +\"%Y-%m-%d %H:%M:%S\") - Gold layer completed' && "
            "echo '[SUCCESS] $(date +\"%Y-%m-%d %H:%M:%S\") - All steps executed successfully'"
        ),
    )

    upload_bronze_hdfs = BashOperator(
        task_id="upload_bronze_hdfs",
        bash_command="bash /workspace/scripts/upload_bronze_hdfs.sh",
    )

    youtube_ingest >> upload_bronze_hdfs
    chicago_ingest >> upload_bronze_hdfs
    upload_bronze_hdfs >> silver_youtube
    upload_bronze_hdfs >> silver_chicago
    [silver_youtube, silver_chicago] >> gold_layer
    gold_layer >> create_hive_tables >> validation >> pipeline_summary
