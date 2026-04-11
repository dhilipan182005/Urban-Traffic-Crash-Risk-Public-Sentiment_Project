from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ============================================================
# Default arguments applied to every task in this DAG
# ============================================================
default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================
# DAG definition — ingestion + silver + gold
# ============================================================
with DAG(
    dag_id="traffic_pipeline",
    description="Urban Traffic & YouTube Sentiment Pipeline — Ingestion + Silver + Gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ingestion", "silver", "gold", "traffic", "youtube"],
) as dag:

    # ----------------------------------------------------------
    # STEP 1a — Ingest YouTube data into bronze layer
    # ----------------------------------------------------------
    youtube_ingest = BashOperator(
        task_id="youtube_ingest",
        bash_command="export PYTHONPATH=/workspace && python -u /workspace/ingestion/youtube_api.py",
    )

    # ----------------------------------------------------------
    # STEP 1b — Ingest Chicago crash data into bronze layer
    # ----------------------------------------------------------
    chicago_ingest = BashOperator(
        task_id="chicago_ingest",
        bash_command="export PYTHONPATH=/workspace && python -u /workspace/ingestion/chicago_api.py",
    )

    # ----------------------------------------------------------
    # STEP 2a — Clean and transform YouTube bronze → silver
    # ----------------------------------------------------------
    silver_youtube = BashOperator(
        task_id="silver_youtube",
        bash_command="export PYTHONPATH=/workspace && python -u /workspace/processing/silver_youtube.py",
    )

    # ----------------------------------------------------------
    # STEP 2b — Clean and transform Chicago bronze → silver
    # ----------------------------------------------------------
    silver_chicago = BashOperator(
        task_id="silver_chicago",
        bash_command="export PYTHONPATH=/workspace && python -u /workspace/processing/silver_chicago.py",
    )

    # ----------------------------------------------------------
    # STEP 3a — Aggregate YouTube silver → gold business tables
    # ----------------------------------------------------------
    gold_youtube = BashOperator(
        task_id="gold_youtube",
        bash_command="export PYTHONPATH=/workspace && python -u /workspace/processing/gold_youtube.py",
    )

    # ----------------------------------------------------------
    # STEP 3b — Aggregate Chicago silver → gold business tables
    # ----------------------------------------------------------
    gold_chicago = BashOperator(
        task_id="gold_chicago",
        bash_command="export PYTHONPATH=/workspace && python -u /workspace/processing/gold_chicago.py",
    )

    # ----------------------------------------------------------
    # Dependencies — ingestion → silver → gold
    # ----------------------------------------------------------
    youtube_ingest >> silver_youtube >> gold_youtube
    chicago_ingest >> silver_chicago >> gold_chicago