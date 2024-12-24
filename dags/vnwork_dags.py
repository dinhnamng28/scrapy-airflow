from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from unidecode import unidecode

from vnwork import scrape_vietnamworks,save_data_to_csv

def scrape_and_save():
    jobs_list = scrape_vietnamworks()
    save_data_to_csv(jobs_list)

with DAG(
    dag_id = "vnwork",
    start_date= datetime(2024,12,1),
    schedule_interval='0 0 * * 1'
) as dag:

    run_vnwork_task = PythonOperator(
        task_id='run_vnwork',
        python_callable=scrape_and_save,
    )


run_vnwork_task