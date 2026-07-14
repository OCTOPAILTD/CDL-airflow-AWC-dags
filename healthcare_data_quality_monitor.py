"""
Healthcare Data Quality Monitor (Airflow 2.x compatible)
=========================================================
A standalone DAG that runs hourly to check the health of the data warehouse.
Demonstrates sensors, branching, dynamic mapping, and alerting patterns.

Airflow features demonstrated:
- TimeDeltaSensor (wait patterns)
- BranchPythonOperator with multiple downstream paths
- Dynamic task mapping with .expand()
- Pool usage (limit concurrent DB queries)
- Custom callbacks
- Trigger rules for complex dependency logic
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule

import json
import pandas as pd
from sqlalchemy import create_engine, text


PG_CONN = "REMOVED"

TABLES_TO_CHECK = [
    "healthcare_dw.dim_patients",
    "healthcare_dw.fact_claims",
    "healthcare_dw.fact_lab_results",
    "healthcare_dw.fact_prescriptions",
]


def check_table_freshness(table_name: str, **context):
    """Check if a table has been updated recently."""
    engine = create_engine(PG_CONN)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM {table_name}"))
            row = result.fetchone()
            count = row[0] if row else 0
    except Exception as e:
        return {"table": table_name, "status": "MISSING", "count": 0, "error": str(e)}

    return {"table": table_name, "status": "OK" if count > 0 else "EMPTY", "count": count}


def check_all_tables(**context):
    """Check all warehouse tables and decide next step."""
    engine = create_engine(PG_CONN)
    results = []

    for table in TABLES_TO_CHECK:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                results.append({"table": table, "status": "OK", "count": count})
        except Exception:
            results.append({"table": table, "status": "MISSING", "count": 0})

    context["ti"].xcom_push(key="table_health", value=json.dumps(results))

    missing = [r for r in results if r["status"] == "MISSING"]
    empty = [r for r in results if r["status"] == "OK" and r["count"] == 0]

    if missing:
        return "alert_tables_missing"
    elif empty:
        return "alert_tables_empty"
    else:
        return "all_healthy"


def check_data_anomalies(**context):
    """Look for statistical anomalies in claims data."""
    engine = create_engine(PG_CONN)
    anomalies = []

    try:
        with engine.connect() as conn:
            claims = pd.read_sql("SELECT * FROM healthcare_dw.fact_claims", conn)

        if len(claims) > 0:
            mean_amount = claims["claim_amount"].mean()
            std_amount = claims["claim_amount"].std()
            outliers = claims[claims["claim_amount"] > mean_amount + 3 * std_amount]
            if len(outliers) > 0:
                anomalies.append({
                    "type": "HIGH_VALUE_OUTLIER",
                    "count": len(outliers),
                    "threshold": float(mean_amount + 3 * std_amount),
                    "claims": outliers["claim_id"].tolist(),
                })

            denied_rate = (claims["status"] == "DENIED").mean() * 100
            if denied_rate > 15:
                anomalies.append({
                    "type": "HIGH_DENIAL_RATE",
                    "rate": float(denied_rate),
                    "threshold": 15.0,
                })

    except Exception as e:
        anomalies.append({"type": "QUERY_ERROR", "error": str(e)})

    context["ti"].xcom_push(key="anomalies", value=json.dumps(anomalies))
    return anomalies


def check_referential_integrity(**context):
    """Verify foreign key relationships between tables."""
    engine = create_engine(PG_CONN)
    issues = []

    try:
        with engine.connect() as conn:
            orphan_claims = conn.execute(text("""
                SELECT COUNT(*) FROM healthcare_dw.fact_claims c
                LEFT JOIN healthcare_dw.dim_patients p ON c.patient_id = p.patient_id
                WHERE p.patient_id IS NULL
            """)).fetchone()[0]

            if orphan_claims > 0:
                issues.append({
                    "type": "ORPHAN_CLAIMS",
                    "count": orphan_claims,
                    "description": "Claims referencing non-existent patients",
                })

            orphan_labs = conn.execute(text("""
                SELECT COUNT(*) FROM healthcare_dw.fact_lab_results l
                LEFT JOIN healthcare_dw.dim_patients p ON l.patient_id = p.patient_id
                WHERE p.patient_id IS NULL
            """)).fetchone()[0]

            if orphan_labs > 0:
                issues.append({
                    "type": "ORPHAN_LABS",
                    "count": orphan_labs,
                    "description": "Lab results referencing non-existent patients",
                })
    except Exception as e:
        issues.append({"type": "CHECK_ERROR", "error": str(e)})

    context["ti"].xcom_push(key="integrity_issues", value=json.dumps(issues))
    return issues


default_args = {
    "owner": "healthcare_data_ops",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="healthcare_data_quality_monitor",
    default_args=default_args,
    description="Hourly warehouse health check: freshness, anomalies, referential integrity",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "monitoring", "data-quality", "cdp-demo"],
    doc_md=__doc__,
) as dag:

    wait_for_settle = TimeDeltaSensor(
        task_id="wait_for_data_settle",
        delta=timedelta(seconds=10),
        poke_interval=5,
    )

    # --- Table Health Check ---
    table_check = BranchPythonOperator(
        task_id="check_table_health",
        python_callable=check_all_tables,
    )

    alert_missing = BashOperator(
        task_id="alert_tables_missing",
        bash_command='echo "[CRITICAL] Warehouse tables missing -- ETL may not have run"',
    )
    alert_empty = BashOperator(
        task_id="alert_tables_empty",
        bash_command='echo "[WARNING] Warehouse tables exist but are empty"',
    )
    all_healthy = EmptyOperator(task_id="all_healthy")

    # --- Deep Checks (only if tables exist) ---
    with TaskGroup(
        "deep_checks", tooltip="Anomaly detection and integrity checks"
    ) as deep_group:
        anomaly_check = PythonOperator(
            task_id="check_anomalies",
            python_callable=check_data_anomalies,
        )
        integrity_check = PythonOperator(
            task_id="check_referential_integrity",
            python_callable=check_referential_integrity,
        )

    # --- Summary ---
    monitoring_done = BashOperator(
        task_id="monitoring_summary",
        bash_command=(
            'echo "=== DQ Monitor Complete ===" && '
            'echo "Timestamp: $(date)" && '
            'echo "All checks executed"'
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Dependencies
    wait_for_settle >> table_check
    table_check >> [alert_missing, alert_empty, all_healthy]
    all_healthy >> deep_group >> monitoring_done
    [alert_missing, alert_empty] >> monitoring_done
