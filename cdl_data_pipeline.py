"""
CDL Data Pipeline (Airflow 2.x compatible)
============================================
End-to-end data pipeline demonstrating S3 ingestion, SQL transformations,
Python processing, Bash utilities, and conditional branching.

Flow: S3 Ingest -> Validate (branch) -> SQL Transform -> Python Enrich -> S3 Export

Airflow features demonstrated:
- S3 operators (S3KeySensor, S3ToLocalFilesystem, LocalFilesystemToS3)
- BranchPythonOperator (conditional execution paths)
- SQLExecuteQueryOperator (multi-stage SQL transformations)
- PythonOperator (data enrichment and business logic)
- BashOperator (environment prep, cleanup, notifications)
- TaskGroups (logical grouping)
- Trigger rules for complex dependency logic
- XComs (inter-task data passing)
- Callbacks (failure alerting)
"""
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.transfers.s3_to_local_filesystem import S3ToLocalFilesystemOperator
from airflow.providers.amazon.aws.transfers.local_filesystem_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule


# --- Configuration ---
S3_BUCKET = "cdl-data-lake"
S3_RAW_PREFIX = "raw/{{ ds }}"
S3_PROCESSED_PREFIX = "processed/{{ ds }}"
S3_ARCHIVE_PREFIX = "archive/{{ ds }}"
SQL_CONN_ID = "cdl_warehouse"
AWS_CONN_ID = "aws_default"


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


# --- Python Callables ---
def validate_ingested_data(**context):
    """Validate data quality after S3 ingestion and decide branch path."""
    ti = context["ti"]

    validation_results = {
        "row_count": 1500,
        "null_pct": 2.3,
        "duplicate_pct": 0.1,
        "schema_valid": True,
    }

    ti.xcom_push(key="validation_results", value=json.dumps(validation_results))

    if not validation_results["schema_valid"]:
        return "validation_gates.critical_failure"
    elif validation_results["null_pct"] > 10 or validation_results["duplicate_pct"] > 5:
        return "validation_gates.quality_warning"
    else:
        return "validation_gates.quality_passed"


def enrich_customer_data(**context):
    """Apply business logic enrichment to transformed data."""
    ti = context["ti"]

    enrichment_stats = {
        "records_enriched": 1450,
        "new_segments_assigned": 320,
        "risk_scores_computed": 1450,
        "geo_lookups_resolved": 1200,
    }

    ti.xcom_push(key="enrichment_stats", value=json.dumps(enrichment_stats))
    print(f"Enrichment complete: {enrichment_stats['records_enriched']} records processed")
    return enrichment_stats


def compute_aggregations(**context):
    """Generate summary aggregations for reporting layer."""
    ti = context["ti"]

    aggregations = {
        "daily_revenue": 245000.50,
        "active_customers": 890,
        "churn_risk_high": 45,
        "new_signups": 23,
    }

    ti.xcom_push(key="aggregations", value=json.dumps(aggregations))
    print(f"Aggregations computed: {len(aggregations)} metrics")
    return aggregations


def generate_export_manifest(**context):
    """Create manifest file listing all exported artifacts."""
    ti = context["ti"]
    ds = context["ds"]

    manifest = {
        "execution_date": ds,
        "exports": [
            {"file": f"processed/{ds}/customers_enriched.parquet", "rows": 1450},
            {"file": f"processed/{ds}/aggregations.json", "rows": 4},
            {"file": f"processed/{ds}/quality_report.json", "rows": 1},
        ],
        "status": "SUCCESS",
    }

    ti.xcom_push(key="manifest", value=json.dumps(manifest))
    print(f"Export manifest generated: {len(manifest['exports'])} files")
    return manifest


# --- DAG Definition ---
default_args = {
    "owner": "cdl_data_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_alert,
    "on_success_callback": on_success_log,
}

with DAG(
    dag_id="cdl_data_pipeline",
    default_args=default_args,
    description="End-to-end CDL pipeline: S3 ingest -> validate -> SQL transform -> enrich -> export",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    tags=["cdl", "etl", "s3", "sql", "enterprise"],
    doc_md=__doc__,
    render_template_as_native_obj=True,
) as dag:

    # =========================================================================
    # STAGE 1: ENVIRONMENT PREP (Bash)
    # =========================================================================
    prep_environment = BashOperator(
        task_id="prep_environment",
        bash_command=(
            'echo "=== CDL Pipeline Starting ===" && '
            'echo "Execution date: {{ ds }}" && '
            'echo "Run ID: {{ run_id }}" && '
            'mkdir -p /tmp/cdl_pipeline/{{ ds }}/raw && '
            'mkdir -p /tmp/cdl_pipeline/{{ ds }}/processed && '
            'echo "Working directories created"'
        ),
    )

    # =========================================================================
    # STAGE 2: S3 INGESTION
    # =========================================================================
    with TaskGroup("s3_ingest", tooltip="Wait for and download data from S3") as s3_ingest_group:

        wait_for_source_data = S3KeySensor(
            task_id="wait_for_source_file",
            bucket_name=S3_BUCKET,
            bucket_key=f"{S3_RAW_PREFIX}/customers.csv",
            aws_conn_id=AWS_CONN_ID,
            poke_interval=60,
            timeout=600,
            mode="poke",
        )

        download_customers = S3ToLocalFilesystemOperator(
            task_id="download_customers",
            bucket=S3_BUCKET,
            key=f"{S3_RAW_PREFIX}/customers.csv",
            local_path="/tmp/cdl_pipeline/{{ ds }}/raw/customers.csv",
            aws_conn_id=AWS_CONN_ID,
        )

        download_transactions = S3ToLocalFilesystemOperator(
            task_id="download_transactions",
            bucket=S3_BUCKET,
            key=f"{S3_RAW_PREFIX}/transactions.csv",
            local_path="/tmp/cdl_pipeline/{{ ds }}/raw/transactions.csv",
            aws_conn_id=AWS_CONN_ID,
        )

        wait_for_source_data >> [download_customers, download_transactions]

    # =========================================================================
    # STAGE 3: VALIDATION & BRANCHING
    # =========================================================================
    validation_branch = BranchPythonOperator(
        task_id="validate_and_branch",
        python_callable=validate_ingested_data,
    )

    with TaskGroup("validation_gates", tooltip="Quality gate branching") as validation_gates:
        quality_passed = EmptyOperator(task_id="quality_passed")

        quality_warning = BashOperator(
            task_id="quality_warning",
            bash_command=(
                'echo "[WARNING] Data quality below threshold for {{ ds }}" && '
                'echo "Proceeding with caution -- results may need review"'
            ),
        )

        critical_failure = BashOperator(
            task_id="critical_failure",
            bash_command=(
                'echo "[CRITICAL] Schema validation failed for {{ ds }}" && '
                'echo "Pipeline will skip transformation and alert team"'
            ),
        )

    # =========================================================================
    # STAGE 4: SQL TRANSFORMATIONS
    # =========================================================================
    with TaskGroup("sql_transform", tooltip="SQL-based data transformations") as sql_transform_group:

        stage_raw_data = SQLExecuteQueryOperator(
            task_id="stage_raw_data",
            conn_id=SQL_CONN_ID,
            sql="""
                CREATE TABLE IF NOT EXISTS staging.customers_raw_{{ ds_nodash }} AS
                SELECT *
                FROM external_schema.customers_{{ ds_nodash }}
                WHERE load_date = '{{ ds }}';
            """,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        deduplicate = SQLExecuteQueryOperator(
            task_id="deduplicate_records",
            conn_id=SQL_CONN_ID,
            sql="""
                DELETE FROM staging.customers_raw_{{ ds_nodash }}
                WHERE ctid NOT IN (
                    SELECT MIN(ctid)
                    FROM staging.customers_raw_{{ ds_nodash }}
                    GROUP BY customer_id
                );
            """,
        )

        transform_customers = SQLExecuteQueryOperator(
            task_id="transform_customers",
            conn_id=SQL_CONN_ID,
            sql="""
                INSERT INTO warehouse.dim_customers (customer_id, name, segment, region, updated_at)
                SELECT
                    customer_id,
                    TRIM(UPPER(name)),
                    COALESCE(segment, 'UNKNOWN'),
                    COALESCE(region, 'UNASSIGNED'),
                    NOW()
                FROM staging.customers_raw_{{ ds_nodash }}
                ON CONFLICT (customer_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    segment = EXCLUDED.segment,
                    region = EXCLUDED.region,
                    updated_at = NOW();
            """,
        )

        aggregate_transactions = SQLExecuteQueryOperator(
            task_id="aggregate_transactions",
            conn_id=SQL_CONN_ID,
            sql="""
                INSERT INTO warehouse.fact_daily_transactions (tx_date, customer_id, total_amount, tx_count)
                SELECT
                    '{{ ds }}'::date,
                    customer_id,
                    SUM(amount),
                    COUNT(*)
                FROM staging.transactions_{{ ds_nodash }}
                GROUP BY customer_id
                ON CONFLICT (tx_date, customer_id)
                DO UPDATE SET
                    total_amount = EXCLUDED.total_amount,
                    tx_count = EXCLUDED.tx_count;
            """,
        )

        stage_raw_data >> deduplicate >> transform_customers >> aggregate_transactions

    # =========================================================================
    # STAGE 5: PYTHON ENRICHMENT
    # =========================================================================
    with TaskGroup("python_enrich", tooltip="Python-based enrichment and aggregation") as python_enrich_group:

        enrich_task = PythonOperator(
            task_id="enrich_customer_data",
            python_callable=enrich_customer_data,
            priority_weight=8,
        )

        aggregate_task = PythonOperator(
            task_id="compute_aggregations",
            python_callable=compute_aggregations,
            priority_weight=6,
        )

        manifest_task = PythonOperator(
            task_id="generate_export_manifest",
            python_callable=generate_export_manifest,
            priority_weight=4,
        )

        enrich_task >> aggregate_task >> manifest_task

    # =========================================================================
    # STAGE 6: S3 EXPORT
    # =========================================================================
    with TaskGroup("s3_export", tooltip="Upload results and archive source data") as s3_export_group:

        upload_processed = LocalFilesystemToS3Operator(
            task_id="upload_processed_data",
            filename="/tmp/cdl_pipeline/{{ ds }}/processed/customers_enriched.parquet",
            dest_bucket=S3_BUCKET,
            dest_key=f"{S3_PROCESSED_PREFIX}/customers_enriched.parquet",
            aws_conn_id=AWS_CONN_ID,
            replace=True,
        )

        upload_aggregations = LocalFilesystemToS3Operator(
            task_id="upload_aggregations",
            filename="/tmp/cdl_pipeline/{{ ds }}/processed/aggregations.json",
            dest_bucket=S3_BUCKET,
            dest_key=f"{S3_PROCESSED_PREFIX}/aggregations.json",
            aws_conn_id=AWS_CONN_ID,
            replace=True,
        )

        archive_source = S3CopyObjectOperator(
            task_id="archive_source_data",
            source_bucket_name=S3_BUCKET,
            source_bucket_key=f"{S3_RAW_PREFIX}/customers.csv",
            dest_bucket_name=S3_BUCKET,
            dest_bucket_key=f"{S3_ARCHIVE_PREFIX}/customers.csv",
            aws_conn_id=AWS_CONN_ID,
        )

        [upload_processed, upload_aggregations] >> archive_source

    # =========================================================================
    # STAGE 7: CLEANUP & NOTIFICATION (Bash)
    # =========================================================================
    cleanup = BashOperator(
        task_id="cleanup_temp_files",
        bash_command=(
            'echo "Cleaning up temporary files..." && '
            'rm -rf /tmp/cdl_pipeline/{{ ds }} && '
            'echo "Temp directory removed"'
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pipeline_summary = BashOperator(
        task_id="pipeline_summary",
        bash_command=(
            'echo "=== CDL Pipeline Complete ===" && '
            'echo "Execution date: {{ ds }}" && '
            'echo "Run ID: {{ run_id }}" && '
            'echo "Completed at: $(date)" && '
            'echo "Status: SUCCESS"'
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

    # Prep -> S3 Ingest
    prep_environment >> s3_ingest_group

    # S3 Ingest -> Validation Branch
    s3_ingest_group >> validation_branch

    # Branch -> Gates
    validation_branch >> [
        validation_gates.quality_passed,
        validation_gates.quality_warning,
        validation_gates.critical_failure,
    ]

    # Passed/Warning -> SQL Transform (critical failure skips)
    [validation_gates.quality_passed, validation_gates.quality_warning] >> sql_transform_group

    # SQL Transform -> Python Enrich -> S3 Export
    sql_transform_group >> python_enrich_group >> s3_export_group

    # Export -> Cleanup -> Summary -> Done
    s3_export_group >> cleanup >> pipeline_summary >> pipeline_complete

    # Critical failure still triggers cleanup
    validation_gates.critical_failure >> cleanup
