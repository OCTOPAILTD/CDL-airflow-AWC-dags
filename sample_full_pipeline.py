from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from datetime import datetime


def transform_data(**context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_raw_data")
    print(f"Transforming {len(raw_data) if raw_data else 0} records")
    return {"status": "transformed", "record_count": 100}


with DAG(
    dag_id="sample_full_pipeline",
    start_date=datetime(2026, 7, 14),
    schedule="@daily",
    catchup=False,
    tags=["sample", "demo"],
) as dag:

    # Step 1: SQL - Extract raw data
    extract_raw_data = SQLExecuteQueryOperator(
        task_id="extract_raw_data",
        conn_id="my_database",
        sql="""
            SELECT id, name, created_at
            FROM raw_events
            WHERE event_date = '{{ ds }}'
        """,
    )

    # Step 2: Python - Transform data
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # Step 3: Bash - Run validation script
    validate = BashOperator(
        task_id="validate_output",
        bash_command="echo 'Validation passed for {{ ds }}' && exit 0",
    )

    # Step 4: SQL - Load into target table
    load_to_target = SQLExecuteQueryOperator(
        task_id="load_to_target",
        conn_id="my_database",
        sql="""
            INSERT INTO processed_events (id, name, processed_at)
            SELECT id, name, NOW()
            FROM staging_events
            WHERE batch_date = '{{ ds }}'
        """,
    )

    # Step 5: S3 - Export results to S3
    export_to_s3 = SqlToS3Operator(
        task_id="export_to_s3",
        query="SELECT * FROM processed_events WHERE batch_date = '{{ ds }}'",
        s3_bucket="my-data-lake",
        s3_key="exports/processed_events/{{ ds }}/data.csv",
        sql_conn_id="my_database",
        aws_conn_id="my_aws",
        file_format="csv",
        replace=True,
    )

    extract_raw_data >> transform >> validate >> load_to_target >> export_to_s3
