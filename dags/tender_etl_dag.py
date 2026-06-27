"""
Airflow DAG: Tender ETL Monitoring Pipeline

Place this file inside your Airflow DAGs folder.
This DAG orchestrates the Python/Pandas tender monitoring pipeline.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task


# Project root is one level above dags/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from scripts.tender_pipeline import (  # noqa: E402
    extract_raw_data,
    validate_tender_data,
    transform_tender_data,
    load_to_database,
    generate_reports,
    archive_processed_file,
)


default_args = {
    "owner": "shivam",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="airflow_tender_etl_monitoring_pipeline",
    description="ETL pipeline for tender SLA monitoring, SQL load, MIS reporting, and exception reporting.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="30 9 * * *",
    catchup=False,
    tags=["python", "pandas", "etl", "tender", "monitoring"],
)
def tender_etl_monitoring_pipeline():

    @task(task_id="extract_raw_data")
    def extract_task():
        return extract_raw_data()

    @task(task_id="validate_tender_data")
    def validate_task():
        return validate_tender_data()

    @task(task_id="transform_tender_data")
    def transform_task():
        return transform_tender_data()

    @task(task_id="load_to_database")
    def load_task():
        return load_to_database()

    @task(task_id="generate_mis_reports")
    def report_task():
        return generate_reports()

    @task(task_id="archive_processed_file")
    def archive_task():
        return archive_processed_file()

    extracted = extract_task()
    validated = validate_task()
    transformed = transform_task()
    loaded = load_task()
    reports = report_task()
    archived = archive_task()

    extracted >> validated >> transformed >> loaded >> reports >> archived


tender_etl_monitoring_pipeline()
