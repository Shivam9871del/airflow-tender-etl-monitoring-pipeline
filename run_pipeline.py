"""
Run this file to test the project without Airflow.

Command:
python3 run_pipeline.py
"""

from scripts.tender_pipeline import run_full_pipeline

if __name__ == "__main__":
    run_full_pipeline()
