"""
DAG: lineage_demo_etl
Demonstrates all dynamic parameter sources for lineage resolution testing.
Uses: params, conf, Variables, Connections, XCom, Jinja macros, env vars.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lineage_demo_etl",
    default_args=default_args,
    description="ETL pipeline demonstrating all lineage parameter types",
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 24),
    catchup=False,
    params={
        "source_schema": "raw_data",
        "target_schema": "analytics",
        "batch_size": 5000,
        "enable_dedup": True,
        "priority_tables": ["orders", "customers", "payments"],
    },
    tags=["lineage", "demo", "etl"],
) as dag:

    # ─── Task 1: Extract config from Variables ───────────────────────────
    def extract_config(**context):
        """Pull dynamic config from Airflow Variables and conf."""
        conf = context["dag_run"].conf or {}
        source_schema = conf.get("source_schema", context["params"]["source_schema"])
        target_schema = conf.get("target_schema", context["params"]["target_schema"])
        batch_size = conf.get("batch_size", context["params"]["batch_size"])

        try:
            db_host = Variable.get("etl_db_host", default_var="localhost")
            db_port = Variable.get("etl_db_port", default_var="5432")
            etl_mode = Variable.get("etl_mode", default_var="incremental")
        except Exception:
            db_host = "localhost"
            db_port = "5432"
            etl_mode = "incremental"

        config = {
            "source_schema": source_schema,
            "target_schema": target_schema,
            "batch_size": batch_size,
            "db_host": db_host,
            "db_port": db_port,
            "etl_mode": etl_mode,
            "run_date": context["ds"],
            "run_ts": context["ts"],
        }
        context["ti"].xcom_push(key="etl_config", value=config)
        print(f"Config resolved: {config}")
        return config

    get_config = PythonOperator(
        task_id="get_config",
        python_callable=extract_config,
    )

    # ─── Task 2: Bash - Extract data using SQL with Jinja templates ──────
    extract_orders = BashOperator(
        task_id="extract_orders",
        bash_command="""
echo "Extracting orders for date: {{ ds }}"
echo "Schema: {{ params.source_schema }}"
echo "Batch: {{ params.batch_size }}"
echo "SQL:"
cat <<SQL
SELECT order_id, customer_id, amount, status, created_at
FROM {{ params.source_schema }}.orders
WHERE created_at >= '{{ ds }}' AND created_at < '{{ next_ds }}'
ORDER BY created_at
LIMIT {{ params.batch_size }};
SQL
""",
    )

    extract_customers = BashOperator(
        task_id="extract_customers",
        bash_command="""
echo "Extracting customers modified since {{ data_interval_start }}"
echo "SQL:"
cat <<SQL
SELECT customer_id, name, email, segment, region
FROM {{ params.source_schema }}.customers
WHERE updated_at >= '{{ data_interval_start }}'
  AND updated_at < '{{ data_interval_end }}'
ORDER BY customer_id;
SQL
""",
    )

    extract_payments = BashOperator(
        task_id="extract_payments",
        bash_command="""
echo "Extracting payments for {{ ds_nodash }}"
echo "SQL:"
cat <<SQL
SELECT payment_id, order_id, method, amount, currency, paid_at
FROM {{ params.source_schema }}.payments
WHERE DATE(paid_at) = '{{ ds }}'
  AND status = 'completed';
SQL
""",
    )

    # ─── Task 3: Python - Transform with XCom and dynamic params ─────────
    def transform_data(**context):
        """Transform extracted data using config from XCom."""
        config = context["ti"].xcom_pull(task_ids="get_config", key="etl_config")
        target_schema = config["target_schema"]
        etl_mode = config["etl_mode"]
        batch_size = config["batch_size"]

        tables_processed = []
        for table in context["params"]["priority_tables"]:
            result = {
                "table": f"{target_schema}.fact_{table}",
                "source": f"{config['source_schema']}.{table}",
                "rows_processed": batch_size,
                "mode": etl_mode,
                "partition_date": context["ds"],
            }
            tables_processed.append(result)
            print(f"Transformed: {result}")

        context["ti"].xcom_push(key="transform_results", value=tables_processed)
        context["ti"].xcom_push(key="row_count", value=len(tables_processed) * batch_size)
        return tables_processed

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # ─── Task 4: Bash - Load into target using connection details ────────
    load_to_target = BashOperator(
        task_id="load_to_target",
        bash_command="""
echo "Loading data into {{ params.target_schema }}"
echo "Run ID: {{ run_id }}"
echo "Logical date: {{ logical_date }}"
echo "SQL:"
cat <<SQL
INSERT INTO {{ params.target_schema }}.fact_orders
SELECT o.order_id, o.customer_id, c.segment, o.amount, o.status,
       p.method as payment_method, o.created_at
FROM staging.orders_{{ ds_nodash }} o
JOIN staging.customers_{{ ds_nodash }} c ON o.customer_id = c.customer_id
LEFT JOIN staging.payments_{{ ds_nodash }} p ON o.order_id = p.order_id
WHERE o.created_at >= '{{ ds }}';

INSERT INTO {{ params.target_schema }}.dim_customers
SELECT customer_id, name, email, segment, region, '{{ ds }}' as valid_from
FROM staging.customers_{{ ds_nodash }}
ON CONFLICT (customer_id) DO UPDATE SET
  segment = EXCLUDED.segment,
  region = EXCLUDED.region,
  valid_from = '{{ ds }}';
SQL
""",
    )

    # ─── Task 5: Python - Generate summary with all resolved values ──────
    def generate_summary(**context):
        """Summarize the ETL run with all dynamic values resolved."""
        config = context["ti"].xcom_pull(task_ids="get_config", key="etl_config")
        transform_results = context["ti"].xcom_pull(
            task_ids="transform_data", key="transform_results"
        )
        row_count = context["ti"].xcom_pull(task_ids="transform_data", key="row_count")

        summary = {
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "execution_date": context["ds"],
            "data_interval": f"{context['data_interval_start']} to {context['data_interval_end']}",
            "source_schema": config["source_schema"],
            "target_schema": config["target_schema"],
            "etl_mode": config["etl_mode"],
            "tables_loaded": [r["table"] for r in (transform_results or [])],
            "total_rows": row_count,
            "db_host": config["db_host"],
        }
        context["ti"].xcom_push(key="etl_summary", value=summary)
        print(f"ETL Summary: {summary}")
        return summary

    summarize = PythonOperator(
        task_id="generate_summary",
        python_callable=generate_summary,
    )

    # ─── Task 6: Bash - Cleanup staging with date partitions ─────────────
    cleanup = BashOperator(
        task_id="cleanup_staging",
        bash_command="""
echo "Cleaning staging tables for {{ ds_nodash }}"
echo "SQL:"
cat <<SQL
DROP TABLE IF EXISTS staging.orders_{{ ds_nodash }};
DROP TABLE IF EXISTS staging.customers_{{ ds_nodash }};
DROP TABLE IF EXISTS staging.payments_{{ ds_nodash }};
VACUUM ANALYZE {{ params.target_schema }}.fact_orders;
VACUUM ANALYZE {{ params.target_schema }}.dim_customers;
SQL
echo "Cleanup complete at $(date)"
""",
    )

    # ─── Dependencies ────────────────────────────────────────────────────
    get_config >> [extract_orders, extract_customers, extract_payments]
    [extract_orders, extract_customers, extract_payments] >> transform
    transform >> load_to_target >> summarize >> cleanup
