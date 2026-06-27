# Interview Notes: Airflow Tender ETL Project

## 1. What is Airflow?

Apache Airflow is used to schedule, orchestrate, and monitor data pipelines. In this project, Airflow controls the order of ETL tasks and shows logs, success, failure, and retry status.

## 2. What is a DAG?

A DAG is the complete workflow or pipeline. My DAG name is:

airflow_tender_etl_monitoring_pipeline

## 3. What are tasks in your DAG?

The tasks are:

1. extract_raw_data
2. validate_tender_data
3. transform_tender_data
4. load_to_database
5. generate_mis_reports
6. archive_processed_file

## 4. Why did you use Airflow?

I used Airflow because a tender monitoring process has multiple dependent steps. Data should be extracted first, then validated, then transformed, then loaded into SQL, and finally reports should be generated. Airflow helped manage this sequence clearly.

## 5. What Python logic did you write?

I used Python and Pandas for:

- Reading Excel data
- Validating mandatory fields
- Checking duplicate Tender IDs
- Checking dependency errors
- Calculating SLA delay
- Finding current tender stage
- Creating MIS and exception reports

## 6. What SQL/database did you use?

I used SQLite for local project implementation. The processed tender data is loaded into a table called tender_monitoring. I also created summary tables for SLA status and officer workload.

## 7. How will this help business users?

This helps business users monitor:

- Delayed tenders
- Running tenders
- Pending approval stages
- Officer workload
- SLA breaches
- Data quality issues

## 8. How will you explain this in 1 minute?

I built an Airflow-based Tender ETL Monitoring Pipeline. The source file was an Excel tender tracker. The pipeline extracts data, validates missing and incorrect records, transforms the data using Python and Pandas, calculates SLA delays and current stages, loads the clean data into SQLite, and generates MIS and exception reports. Airflow is used to orchestrate these steps as separate tasks with dependencies, retries, and logs.
