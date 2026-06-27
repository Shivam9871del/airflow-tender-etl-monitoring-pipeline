# Airflow-Based Tender ETL Monitoring Pipeline

## Project Overview

This project automates tender monitoring using Python, Pandas, SQL, and Apache Airflow.

The pipeline reads an Excel-based tender tracker, validates data quality, transforms tender lifecycle data, loads clean records into SQLite, and generates MIS and exception reports.

## Business Problem

Tender tracking is often done in Excel, which makes it difficult to monitor delays, pending stages, SLA breaches, officer workload, and data-quality issues. This project solves that by converting an Excel tracker into an automated ETL monitoring pipeline.

## Tech Stack

- Python
- Pandas
- Apache Airflow
- SQLite
- Linux / WSL
- Git / GitHub
- Excel input and CSV reporting

## Pipeline Flow

```text
Tender Excel File
   ↓
Extract Raw Data
   ↓
Validate Tender Data
   ↓
Transform Status, SLA, Delay, Current Stage
   ↓
Load into SQLite Database
   ↓
Generate MIS Reports
   ↓
Archive Processed File
```

## Airflow DAG

DAG name:

```text
airflow_tender_etl_monitoring_pipeline
```

Airflow tasks:

```text
extract_raw_data
validate_tender_data
transform_tender_data
load_to_database
generate_mis_reports
archive_processed_file
```

## Input File

```text
data/raw/tender_input_master_final.xlsx
```

Main input sheet:

```text
tender_master
```

## Output Files

```text
output/exception_report.csv
output/mis_status_summary.csv
output/officer_workload_summary.csv
output/high_delay_tenders.csv
database/tender_monitoring.db
```

## Validation Rules

The pipeline checks:

- Missing mandatory fields
- Duplicate Tender IDs
- Invalid tender status
- CBA exists but TBA is missing
- PBO exists but CBA is missing
- Price Comparison exists but PBO is missing
- Order Issued exists but Award Approval is missing
- Cancelled tender without cancellation reason

## Transformation Logic

The pipeline calculates:

- Current tender stage
- Pending owner/team
- Total cycle days
- Delay days based on 60-day SLA
- SLA status:
  - Completed Within SLA
  - Completed Delayed
  - Running Within SLA
  - Running Delayed
  - Cancelled

## Run Without Airflow

Use this first to confirm the Python pipeline is working:

```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas openpyxl
python3 run_pipeline.py
```

## Run With Airflow

For local practice/testing, Apache Airflow supports a standalone mode that initializes the database, creates a user, and starts required components.

Basic steps:

```bash
export AIRFLOW_HOME=~/airflow
pip install apache-airflow pandas openpyxl
mkdir -p ~/airflow/dags
cp dags/tender_etl_dag.py ~/airflow/dags/
cp -r scripts ~/airflow/
cp -r data ~/airflow/
airflow standalone
```

Open Airflow UI:

```text
http://localhost:8080
```

Trigger this DAG manually:

```text
airflow_tender_etl_monitoring_pipeline
```

## Business Use Case

Tender monitoring usually involves multiple stages such as file receipt, tender floating, technical evaluation, commercial evaluation, approvals, order issue, and closure. Manual tracking through Excel can lead to delays, missing approvals, incorrect stage updates, and inconsistent reporting.

This project automates the tender monitoring workflow using an Airflow-based ETL pipeline. It validates tender records, identifies business exceptions, calculates SLA status, loads clean data into SQLite, and generates MIS reports for management review.

## Key Features

- Automated Airflow DAG for tender data extraction, validation, transformation, database loading, reporting, and archiving.
- Business exception report for date sequence issues, missing dependencies, remarks-date mismatch, and data quality problems.
- MIS reports for tender status summary, officer workload, and high-delay tenders.
- SQLite database load for structured storage and future SQL-based analysis.
- Airflow UI monitoring for task status, retries, logs, run history, and execution tracking.
- Archive logic to move processed input files after successful pipeline completion.

## Future Enhancements

- Add email or Slack alerts for failed DAG runs and high exception counts.
- Use Airflow FileSensor to trigger the pipeline when a new input file arrives.
- Deploy the pipeline on EC2/Linux server for scheduled production execution.
- Store processed data in PostgreSQL instead of SQLite for production use.
