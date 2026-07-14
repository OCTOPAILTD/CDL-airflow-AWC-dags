"""
Healthcare Reporting DAG (Airflow 2.x compatible)
==================================================
Triggered automatically when the ETL pipeline updates the patient_360 and
claims datasets (Dataset-aware scheduling). Generates executive dashboards
and compliance reports.

Airflow features demonstrated:
- Dataset-triggered scheduling (runs when upstream datasets update)
- Dynamic task mapping (map over report types)
- Jinja templating in BashOperator
- Task-level retries
- EmptyOperator for flow control
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

import json
import pandas as pd
from sqlalchemy import create_engine, text


PATIENT_360_DATASET = Dataset("postgres://postgres:5432/airflow/healthcare_dw/dim_patients")
CLAIMS_DATASET = Dataset("postgres://postgres:5432/airflow/healthcare_dw/fact_claims")

PG_CONN = BaseHook.get_connection("postgres_default").get_uri()


def generate_executive_kpis(**context):
    """Pull aggregated KPIs for executive dashboard."""
    engine = create_engine(PG_CONN)

    with engine.connect() as conn:
        patients = pd.read_sql("SELECT * FROM healthcare_dw.dim_patients", conn)
        claims = pd.read_sql("SELECT * FROM healthcare_dw.fact_claims", conn)

    kpis = {
        "total_patients": len(patients),
        "avg_age": float(patients["age"].mean()) if "age" in patients.columns else 0,
        "high_risk_patients": int((patients["risk_tier"] == "High").sum()) if "risk_tier" in patients.columns else 0,
        "total_claims": len(claims),
        "total_revenue": float(claims["claim_amount"].sum()),
        "approval_rate": float(
            claims["approved_amount"].sum() / claims["claim_amount"].sum() * 100
        ),
        "avg_processing_days": float(claims["processing_days"].mean()) if "processing_days" in claims.columns else 0,
        "denied_claims_count": int((claims["status"] == "DENIED").sum()),
    }

    context["ti"].xcom_push(key="executive_kpis", value=json.dumps(kpis))
    print(f"Executive KPIs: {json.dumps(kpis, indent=2)}")
    return kpis


def generate_risk_stratification_report(**context):
    """Generate patient risk stratification breakdown."""
    engine = create_engine(PG_CONN)

    with engine.connect() as conn:
        patients = pd.read_sql("SELECT * FROM healthcare_dw.dim_patients", conn)

    if "risk_tier" in patients.columns and "age_group" in patients.columns:
        risk_by_age = patients.groupby(["age_group", "risk_tier"]).size().reset_index(name="count")
        report = risk_by_age.to_dict(orient="records")
    else:
        report = []

    context["ti"].xcom_push(key="risk_report", value=json.dumps(report))
    print(f"Risk stratification: {len(report)} segments")
    return report


def generate_provider_performance(**context):
    """Analyze provider claim approval rates."""
    engine = create_engine(PG_CONN)

    with engine.connect() as conn:
        claims = pd.read_sql("SELECT * FROM healthcare_dw.fact_claims", conn)

    provider_stats = claims.groupby("provider_id").agg(
        total_claims=("claim_id", "count"),
        total_billed=("claim_amount", "sum"),
        total_approved=("approved_amount", "sum"),
        avg_processing=("processing_days", "mean"),
    ).reset_index()
    provider_stats["approval_pct"] = (
        provider_stats["total_approved"] / provider_stats["total_billed"] * 100
    ).round(2)

    report = provider_stats.to_dict(orient="records")
    context["ti"].xcom_push(key="provider_report", value=json.dumps(report))
    return report


def generate_compliance_report(**context):
    """Check for compliance flags -- controlled substances, high-cost claims."""
    engine = create_engine(PG_CONN)

    with engine.connect() as conn:
        rx = pd.read_sql("SELECT * FROM healthcare_dw.fact_prescriptions", conn)
        claims = pd.read_sql("SELECT * FROM healthcare_dw.fact_claims", conn)

    flags = []

    if "is_controlled" in rx.columns:
        controlled = rx[rx["is_controlled"] == True]
        if len(controlled) > 0:
            flags.append({
                "type": "CONTROLLED_SUBSTANCE",
                "count": len(controlled),
                "details": controlled[["rx_id", "patient_id", "drug_name"]].to_dict(orient="records"),
            })

    high_cost = claims[claims["claim_amount"] > 5000]
    if len(high_cost) > 0:
        flags.append({
            "type": "HIGH_COST_CLAIM",
            "count": len(high_cost),
            "total_amount": float(high_cost["claim_amount"].sum()),
        })

    denied = claims[claims["status"] == "DENIED"]
    if len(denied) > 0:
        flags.append({
            "type": "DENIED_CLAIMS",
            "count": len(denied),
            "details": denied[["claim_id", "patient_id"]].to_dict(orient="records"),
        })

    context["ti"].xcom_push(key="compliance_flags", value=json.dumps(flags))
    return flags


default_args = {
    "owner": "healthcare_analytics",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="healthcare_reporting",
    default_args=default_args,
    description="Dataset-triggered reporting: KPIs, risk stratification, compliance",
    schedule=[PATIENT_360_DATASET, CLAIMS_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["healthcare", "reporting", "dataset-triggered", "cdp-demo"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start_reporting")

    with TaskGroup("reports") as reports_group:
        kpis = PythonOperator(
            task_id="executive_kpis",
            python_callable=generate_executive_kpis,
        )
        risk = PythonOperator(
            task_id="risk_stratification",
            python_callable=generate_risk_stratification_report,
        )
        provider = PythonOperator(
            task_id="provider_performance",
            python_callable=generate_provider_performance,
        )
        compliance = PythonOperator(
            task_id="compliance_report",
            python_callable=generate_compliance_report,
        )

    summary = BashOperator(
        task_id="print_report_summary",
        bash_command=(
            'echo "=== Healthcare Reports Generated ===" && '
            'echo "Report date: {{ ds }}" && '
            'echo "Triggered by dataset update" && '
            'echo "Reports: KPIs, Risk, Provider, Compliance"'
        ),
    )

    done = EmptyOperator(task_id="reporting_complete")

    start >> reports_group >> summary >> done
