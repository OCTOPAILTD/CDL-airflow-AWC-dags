"""
Healthcare Enterprise ETL Pipeline (Airflow 2.x compatible)
============================================================
A comprehensive DAG demonstrating enterprise Airflow patterns for a healthcare
company. Processes patient demographics, medical claims, lab results, and
prescriptions through a multi-stage pipeline: Extract -> Validate -> Clean ->
Transform -> Load.

Airflow features demonstrated:
- TaskGroups (logical grouping of related tasks)
- BranchPythonOperator (conditional execution paths)
- XComs (inter-task data passing)
- Dynamic task mapping (expand over datasets)
- Custom operators (DataQualityOperator, PHIComplianceOperator)
- Callbacks (on_failure_callback, on_success_callback, sla_miss_callback)
- Retries with exponential backoff
- Trigger rules (ALL_SUCCESS, NONE_FAILED_MIN_ONE_SUCCESS, ALL_DONE)
- Priority weights
- Pools (resource management)
- BashOperator + PythonOperator
- Dataset-aware scheduling (produces datasets for downstream DAG)
"""
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from cdpdemo.scripts.extract import (
    extract_patients, extract_claims, extract_lab_results, extract_prescriptions,
)
from cdpdemo.scripts.validate import (
    validate_patients, validate_claims, validate_lab_results,
    validate_prescriptions, check_validation_results,
)
from cdpdemo.scripts.transform import (
    clean_patients, clean_claims, clean_lab_results, clean_prescriptions,
    build_patient_360, generate_financial_summary, generate_clinical_alerts,
)
from cdpdemo.scripts.load import (
    init_warehouse_schema, load_patients_to_pg, load_claims_to_pg,
    load_labs_to_pg, load_prescriptions_to_pg, load_financial_report,
    load_clinical_alerts, export_parquet_files,
)
from healthcare_operators import DataQualityOperator, PHIComplianceOperator


# --- Datasets (data-aware scheduling) ---
PATIENT_360_DATASET = Dataset("postgres://postgres:5432/airflow/healthcare_dw/dim_patients")
CLAIMS_DATASET = Dataset("postgres://postgres:5432/airflow/healthcare_dw/fact_claims")
REPORTS_DATASET = Dataset(uri="file:///opt/airflow/dags/cdpdemo/output/clinical_alerts.parquet")


# --- Callbacks ---
def on_failure_alert(context):
    task_instance = context["task_instance"]
    print(
        f"[ALERT] Task FAILED: {task_instance.task_id} "
        f"in DAG {task_instance.dag_id} "
        f"at {context['execution_date']}"
    )


def on_success_log(context):
    task_instance = context["task_instance"]
    print(f"[OK] Task completed: {task_instance.task_id}")


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(
        f"[SLA MISS] DAG {dag.dag_id} missed SLA. "
        f"Tasks: {[t.task_id for t in task_list]}"
    )


# --- DAG Definition ---
default_args = {
    "owner": "healthcare_data_team",
    "depends_on_past": False,
    "email": ["data-alerts@healthcare-corp.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_alert,
    "on_success_callback": on_success_log,
}

with DAG(
    dag_id="healthcare_etl_pipeline",
    default_args=default_args,
    description="Enterprise healthcare ETL: patients, claims, labs, prescriptions -> data warehouse",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    tags=["healthcare", "etl", "enterprise", "cdp-demo"],
    doc_md=__doc__,
    sla_miss_callback=sla_miss_alert,
    render_template_as_native_obj=True,
) as dag:

    # =========================================================================
    # STAGE 1: EXTRACT
    # =========================================================================
    with TaskGroup("extract", tooltip="Extract data from source CSV files") as extract_group:
        extract_patients_task = PythonOperator(
            task_id="extract_patients",
            python_callable=extract_patients,
            priority_weight=10,
            sla=timedelta(minutes=5),
        )
        extract_claims_task = PythonOperator(
            task_id="extract_claims",
            python_callable=extract_claims,
            priority_weight=10,
            sla=timedelta(minutes=5),
        )
        extract_labs_task = PythonOperator(
            task_id="extract_lab_results",
            python_callable=extract_lab_results,
            priority_weight=10,
        )
        extract_rx_task = PythonOperator(
            task_id="extract_prescriptions",
            python_callable=extract_prescriptions,
            priority_weight=10,
        )

    # =========================================================================
    # STAGE 2: VALIDATE
    # =========================================================================
    with TaskGroup("validate", tooltip="Run data quality validations") as validate_group:
        validate_patients_task = PythonOperator(
            task_id="validate_patients",
            python_callable=validate_patients,
        )
        validate_claims_task = PythonOperator(
            task_id="validate_claims",
            python_callable=validate_claims,
        )
        validate_labs_task = PythonOperator(
            task_id="validate_lab_results",
            python_callable=validate_lab_results,
        )
        validate_rx_task = PythonOperator(
            task_id="validate_prescriptions",
            python_callable=validate_prescriptions,
        )

    # Branch based on validation results
    validation_check = BranchPythonOperator(
        task_id="check_validation_gate",
        python_callable=check_validation_results,
    )

    validation_passed = EmptyOperator(task_id="validation_passed")
    validation_failed_but_continue = EmptyOperator(task_id="validation_failed_but_continue")

    # =========================================================================
    # STAGE 2.5: DATA QUALITY (Custom Operators)
    # =========================================================================
    with TaskGroup(
        "data_quality", tooltip="Custom operator data quality checks"
    ) as dq_group:
        dq_patients = DataQualityOperator(
            task_id="dq_check_patients",
            dataset_name="patients",
            max_null_pct=10.0,
            max_duplicate_pct=1.0,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        dq_claims = DataQualityOperator(
            task_id="dq_check_claims",
            dataset_name="claims",
            max_null_pct=5.0,
            max_duplicate_pct=0.5,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        dq_labs = DataQualityOperator(
            task_id="dq_check_lab_results",
            dataset_name="lab_results",
            max_null_pct=5.0,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        dq_rx = DataQualityOperator(
            task_id="dq_check_prescriptions",
            dataset_name="prescriptions",
            max_null_pct=5.0,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

    # =========================================================================
    # STAGE 3: CLEAN / TRANSFORM
    # =========================================================================
    with TaskGroup("transform", tooltip="Clean and transform datasets") as transform_group:
        clean_patients_task = PythonOperator(
            task_id="clean_patients",
            python_callable=clean_patients,
            priority_weight=8,
        )
        clean_claims_task = PythonOperator(
            task_id="clean_claims",
            python_callable=clean_claims,
            priority_weight=8,
        )
        clean_labs_task = PythonOperator(
            task_id="clean_lab_results",
            python_callable=clean_lab_results,
            priority_weight=8,
        )
        clean_rx_task = PythonOperator(
            task_id="clean_prescriptions",
            python_callable=clean_prescriptions,
            priority_weight=8,
        )

    # =========================================================================
    # STAGE 3.5: PHI COMPLIANCE CHECK
    # =========================================================================
    phi_check = PHIComplianceOperator(
        task_id="phi_compliance_check",
        xcom_key="clean_patients",
    )

    # =========================================================================
    # STAGE 4: ENRICH & AGGREGATE
    # =========================================================================
    with TaskGroup("enrich", tooltip="Build enriched views and aggregations") as enrich_group:
        patient_360_task = PythonOperator(
            task_id="build_patient_360",
            python_callable=build_patient_360,
            priority_weight=6,
        )
        financial_task = PythonOperator(
            task_id="generate_financial_summary",
            python_callable=generate_financial_summary,
            priority_weight=6,
        )
        alerts_task = PythonOperator(
            task_id="generate_clinical_alerts",
            python_callable=generate_clinical_alerts,
            priority_weight=7,
        )

    # =========================================================================
    # STAGE 5: LOAD
    # =========================================================================
    init_schema = PythonOperator(
        task_id="init_warehouse_schema",
        python_callable=init_warehouse_schema,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    with TaskGroup("load_to_postgres", tooltip="Load to PostgreSQL warehouse") as load_pg_group:
        load_patients_task = PythonOperator(
            task_id="load_dim_patients",
            python_callable=load_patients_to_pg,
            outlets=[PATIENT_360_DATASET],
        )
        load_claims_task = PythonOperator(
            task_id="load_fact_claims",
            python_callable=load_claims_to_pg,
            outlets=[CLAIMS_DATASET],
        )
        load_labs_task = PythonOperator(
            task_id="load_fact_labs",
            python_callable=load_labs_to_pg,
        )
        load_rx_task = PythonOperator(
            task_id="load_fact_prescriptions",
            python_callable=load_prescriptions_to_pg,
        )
        load_fin_task = PythonOperator(
            task_id="load_financial_report",
            python_callable=load_financial_report,
        )
        load_alerts_task = PythonOperator(
            task_id="load_clinical_alerts",
            python_callable=load_clinical_alerts,
        )

    # =========================================================================
    # STAGE 6: EXPORT PARQUET
    # =========================================================================
    export_parquet = PythonOperator(
        task_id="export_parquet_files",
        python_callable=export_parquet_files,
        outlets=[REPORTS_DATASET],
    )

    # =========================================================================
    # STAGE 7: COMPLETION
    # =========================================================================
    record_count_summary = BashOperator(
        task_id="log_pipeline_summary",
        bash_command=(
            'echo "=== Healthcare ETL Pipeline Complete ===" && '
            'echo "Execution date: {{ ds }}" && '
            'echo "Run ID: {{ run_id }}" && '
            'echo "Pipeline finished at: $(date)"'
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =========================================================================
    # DEPENDENCIES
    # =========================================================================

    # Extract -> Validate
    extract_group >> validate_group

    # Validate -> Branch
    validate_group >> validation_check
    validation_check >> [validation_passed, validation_failed_but_continue]

    # Branch -> Data Quality checks (run regardless of which branch)
    [validation_passed, validation_failed_but_continue] >> dq_group

    # Data Quality -> Transform
    dq_group >> transform_group

    # Transform -> PHI Check -> Enrich
    transform_group >> phi_check >> enrich_group

    # Enrich -> Init Schema -> Load
    enrich_group >> init_schema >> load_pg_group

    # Load -> Export Parquet
    load_pg_group >> export_parquet

    # All loads -> Summary
    [export_parquet] >> record_count_summary >> pipeline_complete
