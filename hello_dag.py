from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello():
    print("Hello from CDL Airflow!")


with DAG(
    dag_id="hello_dag",
    start_date=datetime(2026, 7, 14),
    schedule="@daily",
    catchup=False,
) as dag:
    PythonOperator(task_id="hello_task", python_callable=hello)
