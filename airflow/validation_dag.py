from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta
import os

# ============================================================
# Alert Callbacks
# ============================================================
def failure_alert(context):
    """
    Placeholder for Slack/Email alerting mechanism.
    Executes if any validation task fails.
    """
    task_instance = context.get('task_instance')
    print(f"[ALERT] Task Failed: {task_instance.task_id}")
    print(f"[ALERT] DAG Run ID: {context.get('run_id')}")

# ============================================================
# DAG Configuration
# ============================================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False, # Set true to send emails
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': failure_alert
}

with DAG(
    dag_id='data_quality_validation',
    description='Validation & Monitoring DAG without breaking ingestion logic',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['validation', 'dq', 'monitoring']
) as dag:

    # ============================================================
    # 1. HDFS / File Presence Sensors
    # ============================================================
    # Ensures the pipeline generated files on disk before we analyze them.
    # We use FileSensor as a proxy for HdfsSensor in a local Docker setup,
    # but in a pure Hadoop cluster, use airflow.providers.apache.hdfs.sensors.hdfs
    
    check_silver_chicago_files = FileSensor(
        task_id='check_silver_chicago_files',
        filepath='/workspace/data/silver/chicago/_SUCCESS',
        poke_interval=60,
        timeout=600,
        mode='reschedule'
    )

    check_silver_youtube_files = FileSensor(
        task_id='check_silver_youtube_files',
        filepath='/workspace/data/silver/youtube/_SUCCESS',
        poke_interval=60,
        timeout=600,
        mode='reschedule'
    )

    # ============================================================
    # 2. Hive Data Quality Validations
    # ============================================================
    
    # Refresh partitions in the new optimized tables
    refresh_chicago_partitions = HiveOperator(
        task_id='refresh_chicago_partitions',
        hql='MSCK REPAIR TABLE data_pipeline.silver_chicago_optimized;',
        hive_cli_conn_id='hive_default'
    )

    # Row Count Check & Freshness
    validate_chicago_counts = HiveOperator(
        task_id='validate_chicago_counts',
        hql="""
            SELECT 
                COUNT(*) as total_rows 
            FROM data_pipeline.silver_chicago_optimized;
            -- Should throw an alert if rows = 0, can be parsed by an operator
        """,
        hive_cli_conn_id='hive_default'
    )

    # Null Check on critical columns
    validate_youtube_nulls = HiveOperator(
        task_id='validate_youtube_nulls',
        hql="""
            SELECT 
                SUM(CASE WHEN video_id IS NULL THEN 1 ELSE 0 END) as null_video_ids,
                SUM(CASE WHEN engagement_score IS NULL THEN 1 ELSE 0 END) as null_scores
            FROM data_pipeline.silver_youtube_optimized;
        """,
        hive_cli_conn_id='hive_default'
    )

    # ============================================================
    # 3. Schema Drift & Custom Python Checks
    # ============================================================
    def expected_schema_check():
        """
        Validate that the generated Parquet files match expected schema signatures.
        Logs an anomaly if fields are missing.
        """
        import pyarrow.parquet as pq
        
        # Chicago check
        try:
            chicago_q = pq.ParquetDataset("/workspace/data/silver/chicago")
            schema = chicago_q.schema
            names = [sch.name for sch in schema]
            if "crash_record_id" not in names:
                raise ValueError("Schema Drift Detected: 'crash_record_id' missing from Chicago Silver!")
            print("[OK] Chicago Silver Schema is intact.")
        except Exception as e:
            print(f"[WARNING] {e}")
            
    schema_drift_check = PythonOperator(
        task_id='schema_drift_check',
        python_callable=expected_schema_check
    )

    # ============================================================
    # DAG Dependencies
    # ============================================================
    
    # 1. Wait for success files to confirm pipeline actually wrote data
    [check_silver_chicago_files, check_silver_youtube_files] >> refresh_chicago_partitions
    
    # 2. Once partitions map properly, run our SQL queries to check for anomalies
    refresh_chicago_partitions >> [validate_chicago_counts, validate_youtube_nulls]
    
    # 3. Simultaneously, check for Schema drifting directly against the flat files
    [check_silver_chicago_files, check_silver_youtube_files] >> schema_drift_check
