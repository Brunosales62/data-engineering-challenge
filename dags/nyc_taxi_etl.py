from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="nyc_taxi_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["challenge", "etl"],
) as dag:

    source_task = BashOperator(
        task_id="source",
        bash_command="""
        python /opt/airflow/challenge/scripts/source.py
        """
    )

    extract_task = BashOperator(
        task_id="extract",
        bash_command="""
        python /opt/airflow/challenge/scripts/extract.py
        """
    )

    transform_task = BashOperator(
        task_id="transform",
        bash_command="""
        python /opt/airflow/challenge/scripts/transform.py
        """
    )

    refined_task = BashOperator(
        task_id="refined",
        bash_command="""
        python /opt/airflow/challenge/scripts/refined.py
        """
    )

    load_task = BashOperator(
        task_id="load",
        bash_command="""
        python /opt/airflow/challenge/scripts/load.py
        """
    )

    (
        source_task
        >> extract_task
        >> transform_task
        >> refined_task
        >> load_task
    )