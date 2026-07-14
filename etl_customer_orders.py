from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='etl_customer_orders',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

extract_orders_sql = PostgresOperator(
    task_id='extract_orders_sql',
    postgres_conn_id='warehouse_pg',
    sql="""
        INSERT INTO staging.raw_orders
        SELECT order_id, customer_id, amount, order_date
        FROM production.orders
        WHERE order_date = '{{ ds }}'
    """,
    dag=dag,
)

transform_orders_python = PythonOperator(
    task_id='transform_orders_python',
    python_callable=lambda: __import__('subprocess').run(
        ['python', '/opt/airflow/scripts/transform_orders.py', '--date', '{{ ds }}']
    ),
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
)

load_summary_bash_sql = BashOperator(
    task_id='load_summary_bash_sql',
    bash_command="""
        psql -h prod-db.company.com -U etl_user -d analytics -c "
        INSERT INTO reporting.daily_order_summary (order_date, total_orders, total_revenue)
        SELECT '{{ ds }}', COUNT(*), SUM(amount)
        FROM staging.transformed_orders
        WHERE order_date = '{{ ds }}'
        "
    """,
    dag=dag,
)

validate_output_sql = PostgresOperator(
    task_id='validate_output_sql',
    postgres_conn_id='warehouse_pg',
    sql="""
        SELECT CASE
            WHEN COUNT(*) = 0 THEN RAISE('No rows in daily_order_summary for {{ ds }}')
            ELSE 'OK'
        END
        FROM reporting.daily_order_summary
        WHERE order_date = '{{ ds }}'
    """,
    dag=dag,
)

extract_orders_sql >> transform_orders_python >> load_summary_bash_sql >> validate_output_sql
