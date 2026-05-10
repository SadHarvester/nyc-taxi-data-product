"""
Optional Airflow DAG.

The project can be run with Dagster by default. This DAG is included only to show
how the same streaming ingestion flow could be triggered from Airflow if required.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="nyc_taxi_streaming_medallion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["big-data", "streaming", "medallion"],
) as dag:
    streaming_ingestion = BashOperator(
        task_id="streaming_ingestion",
        bash_command="python scripts/run_streaming_ingestion.py",
    )

    medallion_transformations = BashOperator(
        task_id="medallion_transformations",
        bash_command="python scripts/run_pipeline.py",
    )

    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command="python scripts/run_data_quality_checks.py",
    )

    streaming_ingestion >> medallion_transformations >> data_quality_checks
