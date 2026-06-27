"""
Airflow-Based Tender ETL Monitoring Pipeline
Author: Shivam Paliwal

This script contains reusable Python functions used by the Airflow DAG.
The same functions can also be tested without Airflow by running run_pipeline.py.
"""

from pathlib import Path
from datetime import date
import shutil
import sqlite3

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "tender_input_master_final.xlsx"
STAGING_FILE = PROJECT_ROOT / "data" / "staging" / "tender_extracted.csv"
PROCESSED_FILE = PROJECT_ROOT / "data" / "processed" / "tender_cleaned.csv"
EXCEPTION_FILE = PROJECT_ROOT / "output" / "exception_report.csv"
MIS_STATUS_FILE = PROJECT_ROOT / "output" / "mis_status_summary.csv"
OFFICER_WORKLOAD_FILE = PROJECT_ROOT / "output" / "officer_workload_summary.csv"
HIGH_DELAY_FILE = PROJECT_ROOT / "output" / "high_delay_tenders.csv"
DATABASE_FILE = PROJECT_ROOT / "database" / "tender_monitoring.db"
ARCHIVE_DIR = PROJECT_ROOT / "data" / "archive"

SLA_DAYS = 60

DATE_COLUMNS = [
    "File/Tender Received Date",
    "NIT Date",
    "Tender Floating Date",
    "Technical Bid Opening Date",
    "TBA Date",
    "CBA Date",
    "PBO Date",
    "Price Comparison Date",
    "Workability/Reasonability Date",
    "Negotiation Date",
    "Award Approval Date",
    "Order Issued Date",
]


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Create SQL-friendly column names while keeping the data readable."""
    rename_map = {
        "Tender ID": "tender_id",
        "Category": "category",
        "Officer ID": "officer_id",
        "Estimate Value (Lakhs)": "estimate_value_lakhs",
        "File/Tender Received Date": "file_received_date",
        "Bidders Participated": "bidders_participated",
        "TBA Rejected": "tba_rejected",
        "CBA Rejected": "cba_rejected",
        "Qualified Bidders": "qualified_bidders",
        "L1 Value (Lakhs)": "l1_value_lakhs",
        "Revised L1 Value (Lakhs)": "revised_l1_value_lakhs",
        "Post PBO Review Type": "post_pbo_review_type",
        "Negotiation Required": "negotiation_required",
        "NIT Date": "nit_date",
        "Tender Floating Date": "tender_floating_date",
        "Technical Bid Opening Date": "technical_bid_opening_date",
        "TBA Date": "tba_date",
        "CBA Date": "cba_date",
        "PBO Date": "pbo_date",
        "Price Comparison Date": "price_comparison_date",
        "Workability/Reasonability Date": "workability_reasonability_date",
        "Negotiation Date": "negotiation_date",
        "Award Approval Date": "award_approval_date",
        "Order Issued Date": "order_issued_date",
        "Tender Status": "tender_status",
        "Cancellation Reason": "cancellation_reason",
        "Remarks": "remarks",
    }
    return df.rename(columns=rename_map)


def extract_raw_data() -> str:
    """
    Extract tender data from the raw Excel workbook.
    Output: data/staging/tender_extracted.csv
    """
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw input file not found: {RAW_FILE}")

    df = pd.read_excel(RAW_FILE, sheet_name="tender_master")

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df.to_csv(STAGING_FILE, index=False)
    print(f"Extracted {len(df)} rows from tender_master sheet.")
    print(f"Saved staging file: {STAGING_FILE}")
    return str(STAGING_FILE)

def validate_tender_data() -> str:
    import pandas as pd
    import os

    staging_file = "data/staging/tender_extracted.csv"
    exception_file = "output/exception_report.csv"

    df = pd.read_csv(staging_file)
    exceptions = []

    def add_exception(row, rule_name, severity, issue_description):
        exceptions.append({
            "Tender ID": row.get("Tender ID", ""),
            "Officer ID": row.get("Officer ID", ""),
            "Category": row.get("Category", ""),
            "Tender Status": row.get("Tender Status", ""),
            "Rule Name": rule_name,
            "Severity": severity,
            "Issue Description": issue_description
        })

    mandatory_columns = [
        "Tender ID",
        "Officer ID",
        "Category",
        "File/Tender Received Date",
        "Estimate Value (Lakhs)",
        "Tender Status"
    ]

    date_columns = [
        "File/Tender Received Date",
        "NIT Date",
        "Tender Floating Date",
        "Technical Bid Opening Date",
        "TBA Date",
        "CBA Date",
        "PBO Date",
        "Price Comparison Date",
        "Workability/Reasonability Date",
        "Negotiation Date",
        "Award Approval Date",
        "Order Issued Date"
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for index, row in df.iterrows():

        # 1. Missing mandatory fields
        for col in mandatory_columns:
            if col in df.columns and pd.isna(row[col]):
                add_exception(
                    row,
                    "Missing Mandatory Field",
                    "Critical",
                    f"{col} is missing"
                )

        # 2. Stage dependency checks
        dependency_rules = [
            ("CBA Date", "TBA Date", "CBA exists but TBA is missing"),
            ("PBO Date", "CBA Date", "PBO exists but CBA is missing"),
            ("Price Comparison Date", "PBO Date", "Price Comparison exists but PBO is missing"),
            ("Workability/Reasonability Date", "Price Comparison Date", "Workability/Reasonability exists but Price Comparison is missing"),
            ("Award Approval Date", "PBO Date", "Award Approval exists but PBO is missing"),
            ("Order Issued Date", "Award Approval Date", "Order Issued exists but Award Approval is missing")
        ]

        for later_col, earlier_col, message in dependency_rules:
            if later_col in df.columns and earlier_col in df.columns:
                if pd.notna(row[later_col]) and pd.isna(row[earlier_col]):
                    add_exception(row, "Stage Dependency Issue", "High", message)

        # 3. Date sequence checks
        sequence_rules = [
            ("NIT Date", "File/Tender Received Date", "NIT Date is earlier than File Received Date"),
            ("Tender Floating Date", "NIT Date", "Tender Floating Date is earlier than NIT Date"),
            ("Technical Bid Opening Date", "Tender Floating Date", "Technical Bid Opening Date is earlier than Tender Floating Date"),
            ("TBA Date", "Technical Bid Opening Date", "TBA Date is earlier than Technical Bid Opening Date"),
            ("CBA Date", "TBA Date", "CBA Date is earlier than TBA Date"),
            ("PBO Date", "CBA Date", "PBO Date is earlier than CBA Date"),
            ("Price Comparison Date", "PBO Date", "Price Comparison Date is earlier than PBO Date"),
            ("Award Approval Date", "PBO Date", "Award Approval Date is earlier than PBO Date"),
            ("Order Issued Date", "Award Approval Date", "Order Issued Date is earlier than Award Approval Date")
        ]

        for later_col, earlier_col, message in sequence_rules:
            if later_col in df.columns and earlier_col in df.columns:
                if pd.notna(row[later_col]) and pd.notna(row[earlier_col]):
                    if row[later_col] < row[earlier_col]:
                        add_exception(row, "Date Sequence Issue", "High", message)

        # 4. Bidder validation checks
        bidders = row.get("Bidders Participated", 0)
        tba_rejected = row.get("TBA Rejected", 0)
        cba_rejected = row.get("CBA Rejected", 0)
        qualified = row.get("Qualified Bidders", 0)

        if pd.notna(bidders) and bidders < 0:
            add_exception(row, "Invalid Bidder Data", "High", "Bidders Participated cannot be negative")

        if pd.notna(tba_rejected) and pd.notna(bidders) and tba_rejected > bidders:
            add_exception(row, "Invalid Bidder Data", "High", "TBA Rejected cannot be greater than Bidders Participated")

        if pd.notna(qualified) and pd.notna(bidders) and qualified > bidders:
            add_exception(row, "Invalid Bidder Data", "High", "Qualified Bidders cannot be greater than Bidders Participated")

        if pd.notna(cba_rejected) and pd.notna(qualified) and cba_rejected > qualified:
            add_exception(row, "Invalid Bidder Data", "High", "CBA Rejected cannot be greater than Qualified Bidders")

        # 5. Remarks/date mismatch checks
        remarks = str(row.get("Remarks", "")).lower()

        if "order issued" in remarks and pd.isna(row.get("Order Issued Date")):
            add_exception(row, "Remarks Date Mismatch", "Medium", "Remarks mention order issued but Order Issued Date is missing")

        if "pbo" in remarks and pd.isna(row.get("PBO Date")):
            add_exception(row, "Remarks Date Mismatch", "Medium", "Remarks mention PBO but PBO Date is missing")

        if "floating" in remarks and pd.isna(row.get("Tender Floating Date")):
            add_exception(row, "Remarks Date Mismatch", "Medium", "Remarks mention floating but Tender Floating Date is missing")

        if "technical" in remarks and pd.isna(row.get("TBA Date")):
            add_exception(row, "Remarks Date Mismatch", "Medium", "Remarks mention technical evaluation but TBA Date is missing")

    exception_df = pd.DataFrame(exceptions)

    os.makedirs("output", exist_ok=True)

    if exception_df.empty:
        exception_df = pd.DataFrame(columns=[
            "Tender ID",
            "Officer ID",
            "Category",
            "Tender Status",
            "Rule Name",
            "Severity",
            "Issue Description"
        ])

    exception_df.to_csv(exception_file, index=False)

    print(f"Validation completed. Exceptions found: {len(exception_df)}")
    print(f"Exception report saved: {exception_file}")

    return exception_file

def _current_stage(row: pd.Series) -> str:
    """Identify current stage based on the latest available milestone date."""
    if pd.notna(row.get("order_issued_date")):
        return "Order Issued"
    if pd.notna(row.get("award_approval_date")):
        return "Pending Order Issue"
    if pd.notna(row.get("negotiation_date")):
        return "Pending Award Approval"
    if pd.notna(row.get("workability_reasonability_date")):
        return "Pending Award Approval"
    if pd.notna(row.get("price_comparison_date")):
        return "Pending Workability/Reasonability"
    if pd.notna(row.get("pbo_date")):
        return "Pending Price Comparison"
    if pd.notna(row.get("cba_date")):
        return "Pending PBO Approval"
    if pd.notna(row.get("tba_date")):
        return "Pending CBA"
    if pd.notna(row.get("technical_bid_opening_date")):
        return "Pending TBA"
    if pd.notna(row.get("tender_floating_date")):
        return "Pending Technical Bid Opening"
    if pd.notna(row.get("nit_date")):
        return "Pending Tender Floating"
    return "Pending NIT"


def _pending_with(stage: str) -> str:
    mapping = {
        "Pending NIT": "Officer",
        "Pending Tender Floating": "GM Contracts",
        "Pending Technical Bid Opening": "Officer",
        "Pending TBA": "Officer",
        "Pending CBA": "Officer",
        "Pending PBO Approval": "GM Contracts / Finance GM",
        "Pending Price Comparison": "Officer",
        "Pending Workability/Reasonability": "Finance GM",
        "Pending Award Approval": "GM Contracts / Finance GM",
        "Pending Order Issue": "Officer",
        "Order Issued": "Closed",
    }
    return mapping.get(stage, "Officer")


def transform_tender_data() -> str:
    """
    Clean and transform tender data.
    Adds current_stage, pending_with, cycle_days, delay_days, and sla_status.
    Output: data/processed/tender_cleaned.csv
    """
    df = pd.read_csv(STAGING_FILE)
    df = _standardize_columns(df)

    date_cols = [
        "file_received_date",
        "nit_date",
        "tender_floating_date",
        "technical_bid_opening_date",
        "tba_date",
        "cba_date",
        "pbo_date",
        "price_comparison_date",
        "workability_reasonability_date",
        "negotiation_date",
        "award_approval_date",
        "order_issued_date",
    ]

    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    today = pd.Timestamp(date.today())

    df["current_stage"] = df.apply(_current_stage, axis=1)
    df["pending_with"] = df["current_stage"].apply(_pending_with)

    df["end_date_for_sla"] = df["order_issued_date"].fillna(today)
    df["cycle_days"] = (df["end_date_for_sla"] - df["file_received_date"]).dt.days
    df["delay_days"] = (df["cycle_days"] - SLA_DAYS).clip(lower=0)

    def sla_status(row):
        status = str(row["tender_status"]).strip()
        if status == "Cancelled":
            return "Cancelled"
        if status == "Completed" and row["delay_days"] == 0:
            return "Completed Within SLA"
        if status == "Completed" and row["delay_days"] > 0:
            return "Completed Delayed"
        if status == "Under Process" and row["delay_days"] == 0:
            return "Running Within SLA"
        return "Running Delayed"

    df["sla_status"] = df.apply(sla_status, axis=1)

    # Convert dates to YYYY-MM-DD strings for clean CSV and SQLite output.
    for col in date_cols + ["end_date_for_sla"]:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    df.to_csv(PROCESSED_FILE, index=False)
    print(f"Transformed {len(df)} rows.")
    print(f"Saved processed file: {PROCESSED_FILE}")
    return str(PROCESSED_FILE)


def load_to_database() -> str:
    """
    Load processed tender data into SQLite.
    Output: database/tender_monitoring.db
    """
    df = pd.read_csv(PROCESSED_FILE)
    DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DATABASE_FILE) as conn:
        df.to_sql("tender_monitoring", conn, if_exists="replace", index=False)

        # Create simple summary tables for easy SQL demo.
        status_summary = df.groupby("sla_status", dropna=False).size().reset_index(name="tender_count")
        status_summary.to_sql("sla_status_summary", conn, if_exists="replace", index=False)

        officer_summary = (
            df[df["tender_status"] == "Under Process"]
            .groupby(["officer_id", "current_stage", "pending_with"], dropna=False)
            .size()
            .reset_index(name="running_tender_count")
        )
        officer_summary.to_sql("officer_workload_summary", conn, if_exists="replace", index=False)

    print(f"Loaded processed data into SQLite database: {DATABASE_FILE}")
    return str(DATABASE_FILE)


def generate_reports() -> str:
    """
    Generate MIS and exception-focused CSV reports.
    Outputs:
    - output/mis_status_summary.csv
    - output/officer_workload_summary.csv
    - output/high_delay_tenders.csv
    """
    df = pd.read_csv(PROCESSED_FILE)

    status_summary = (
        df.groupby(["tender_status", "sla_status"], dropna=False)
        .agg(
            tender_count=("tender_id", "count"),
            avg_cycle_days=("cycle_days", "mean"),
            max_delay_days=("delay_days", "max"),
            total_estimate_value_lakhs=("estimate_value_lakhs", "sum"),
        )
        .reset_index()
    )
    status_summary["avg_cycle_days"] = status_summary["avg_cycle_days"].round(2)
    status_summary["total_estimate_value_lakhs"] = status_summary["total_estimate_value_lakhs"].round(2)
    status_summary.to_csv(MIS_STATUS_FILE, index=False)

    officer_workload = (
        df[df["tender_status"] == "Under Process"]
        .groupby(["officer_id", "category", "current_stage", "pending_with"], dropna=False)
        .agg(
            running_tender_count=("tender_id", "count"),
            avg_delay_days=("delay_days", "mean"),
            max_delay_days=("delay_days", "max"),
            total_estimate_value_lakhs=("estimate_value_lakhs", "sum"),
        )
        .reset_index()
    )
    officer_workload["avg_delay_days"] = officer_workload["avg_delay_days"].round(2)
    officer_workload["total_estimate_value_lakhs"] = officer_workload["total_estimate_value_lakhs"].round(2)
    officer_workload.to_csv(OFFICER_WORKLOAD_FILE, index=False)

    high_delay = df[df["delay_days"] > 30].sort_values("delay_days", ascending=False)
    high_delay_cols = [
        "tender_id",
        "category",
        "officer_id",
        "estimate_value_lakhs",
        "tender_status",
        "current_stage",
        "pending_with",
        "cycle_days",
        "delay_days",
        "remarks",
    ]
    high_delay[high_delay_cols].to_csv(HIGH_DELAY_FILE, index=False)

    print("Reports generated:")
    print(f"1. {MIS_STATUS_FILE}")
    print(f"2. {OFFICER_WORKLOAD_FILE}")
    print(f"3. {HIGH_DELAY_FILE}")
    return str(PROJECT_ROOT / "output")


def archive_processed_file() -> str:
    """
    Archive a copy of the raw input file after successful pipeline run.
    """
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_file = ARCHIVE_DIR / f"tender_input_master_final_archived_{date.today().isoformat()}.xlsx"
    shutil.copy2(RAW_FILE, archive_file)
    print(f"Archived raw file to: {archive_file}")
    return str(archive_file)


def run_full_pipeline() -> None:
    """Run all ETL steps without Airflow for local testing."""
    extract_raw_data()
    validate_tender_data()
    transform_tender_data()
    load_to_database()
    generate_reports()
    archive_processed_file()


if __name__ == "__main__":
    run_full_pipeline()
