"""
Test DAG: test_rendered_fields
Covers all supported operator families with Jinja-templated fields
to validate rendered template extraction via the REST API.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "octopai_test",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="test_rendered_fields",
    default_args=default_args,
    description="Test DAG for all operator families - rendered fields extraction",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["octopai", "poc", "rendered_fields"],
    params={
        "schema": "public",
        "table": "customers",
        "s3_bucket": "my-data-lake",
        "s3_prefix": "raw/orders",
    },
) as dag:

    # =========================================================================
    # 1. EmptyOperator — start node
    # =========================================================================
    start = EmptyOperator(task_id="start")

    # =========================================================================
    # 2. SQL — PostgresOperator
    # =========================================================================
    sql_postgres = PostgresOperator(
        task_id="sql_postgres_extract",
        postgres_conn_id="postgres_default",
        sql="""
            SELECT dag_id, task_id, state, start_date, end_date
            FROM task_instance
            WHERE start_date >= '{{ ds }}'::timestamp
            ORDER BY start_date DESC
            LIMIT 10;
        """,
    )

    # =========================================================================
    # 3. Snowflake — SnowflakeOperator
    # =========================================================================
    sql_snowflake = SnowflakeOperator(
        task_id="sql_snowflake_load",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema != 'INFORMATION_SCHEMA'
            LIMIT 10;
        """,
        warehouse="COMPUTE_WH",
        database="SNOWFLAKE",
        schema="INFORMATION_SCHEMA",
    )

    # =========================================================================
    # 4. SQL via generic SQLExecuteQueryOperator (postgres, demonstrates multi-sql)
    # =========================================================================
    sql_generic = SQLExecuteQueryOperator(
        task_id="sql_generic_query",
        conn_id="postgres_default",
        sql="""
            SELECT dag_id, COUNT(*) as task_count
            FROM task_instance
            WHERE start_date >= '{{ ds }}'::timestamp
            GROUP BY dag_id
            ORDER BY task_count DESC
            LIMIT 5;
        """,
    )

    # =========================================================================
    # 5. Bash simulating S3 source download (templated like S3 operator)
    # =========================================================================
    s3_source_sim = BashOperator(
        task_id="s3_source_download",
        bash_command="echo 'Downloading s3://{{ params.s3_bucket }}/{{ params.s3_prefix }}/{{ ds }}/orders.parquet to /tmp/orders_{{ ds_nodash }}.parquet'",
    )

    # =========================================================================
    # 6. Bash simulating S3 target upload (templated like S3 operator)
    # =========================================================================
    s3_target_sim = BashOperator(
        task_id="s3_target_upload",
        bash_command="echo 'Uploading /tmp/processed_{{ ds_nodash }}.csv to s3://{{ params.s3_bucket }}/processed/{{ ds }}/output.csv'",
    )

    # =========================================================================
    # 7. Bash simulating S3 management copy (templated like S3 operator)
    # =========================================================================
    s3_mgmt_sim = BashOperator(
        task_id="s3_mgmt_copy",
        bash_command="echo 'Copying s3://{{ params.s3_bucket }}/{{ params.s3_prefix }}/{{ ds }}/orders.parquet to s3://{{ params.s3_bucket }}/archive/{{ ds }}/orders.parquet'",
    )

    # =========================================================================
    # 8. Python — PythonOperator
    # =========================================================================
    def transform_data(ds=None, table=None, schema=None, **kwargs):
        print(f"Transforming {schema}.{table} for date {ds}")

    python_transform = PythonOperator(
        task_id="python_transform",
        python_callable=transform_data,
        op_kwargs={
            "table": "{{ params.table }}",
            "schema": "{{ params.schema }}",
        },
    )

    # =========================================================================
    # 9. Bash — BashOperator
    # =========================================================================
    bash_export = BashOperator(
        task_id="bash_export",
        bash_command="echo 'Exporting data for {{ ds }} from {{ params.schema }}.{{ params.table }}' && date",
    )

    # =========================================================================
    # 10. Bash simulating Email notification (templated like EmailOperator)
    # =========================================================================
    email_sim = BashOperator(
        task_id="email_notification",
        bash_command="echo 'Sending email: Pipeline completed for {{ ds }} | Table: {{ params.schema }}.{{ params.table }} | Bucket: {{ params.s3_bucket }}'",
    )

    # =========================================================================
    # 11. EmptyOperator — end node
    # =========================================================================
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # =========================================================================
    # Dependencies
    # =========================================================================
    start >> [sql_postgres, sql_snowflake, sql_generic, s3_source_sim, s3_target_sim, s3_mgmt_sim, python_transform, bash_export, email_sim]
    [sql_postgres, sql_snowflake, sql_generic, s3_source_sim, s3_target_sim, s3_mgmt_sim, python_transform, bash_export, email_sim] >> end
