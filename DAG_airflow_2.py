"""
Simple marker DAG to identify this as an Airflow 2 instance.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "octopai",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="DAG_airflow_2",
    default_args=default_args,
    description="Marker DAG - identifies this as an Airflow 2.x instance",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["octopai", "marker", "airflow-2"],
) as dag:

    identify = BashOperator(
        task_id="identify_version",
        bash_command='echo "This is Airflow 2.x running on port 8081"',
    )
