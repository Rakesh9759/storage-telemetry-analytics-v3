"""
Airflow DAG for Storage Telemetry Analytics Pipeline
Orchestrates data generation, Spark transformation, anomaly detection, and dashboard validation.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'storage_telemetry_dag',
    default_args=default_args,
    description='Storage Telemetry Analytics Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    generate_raw_data = BashOperator(
        task_id='generate_raw_data',
        bash_command='python scripts/generate_sample_data.py',
    )

    load_raw_to_postgres = BashOperator(
        task_id='load_raw_to_postgres',
        bash_command='python pipelines/load_raw_to_postgres.py',
    )

    spark_transform = BashOperator(
        task_id='spark_transform',
        bash_command='spark-submit pipelines/spark_transform.py',
    )

    run_anomaly_detection = BashOperator(
        task_id='run_anomaly_detection',
        bash_command='python pipelines/anomaly_detection.py',
    )

    build_dashboard_marts = BashOperator(
        task_id='build_dashboard_marts',
        bash_command='python pipelines/build_marts.py',
    )

    validate_dashboard_datasets = BashOperator(
        task_id='validate_dashboard_datasets',
        bash_command='python pipelines/validate_dashboard_datasets.py',
    )

    generate_raw_data >> load_raw_to_postgres >> spark_transform >> run_anomaly_detection >> build_dashboard_marts >> validate_dashboard_datasets
