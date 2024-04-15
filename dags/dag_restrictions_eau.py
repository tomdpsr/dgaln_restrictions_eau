from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from main import extract, load


with DAG(
    "dag_restrictions_eau",
    description="DAG Mise à jour des données de restrictions eau",
    schedule=None,
    start_date=pendulum.datetime(2024, 4, 13, tz="UTC"),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> load_task
