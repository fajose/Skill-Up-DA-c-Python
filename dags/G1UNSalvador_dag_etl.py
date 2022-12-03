import os
import logging
import csv
import boto3
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task


default_args = {
    "owner": "Kevin Marcos Agui Manera",
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

# Instantiate DAG
with DAG(
    dag_id="G1UNSalvador_dag_etl",
    start_date=datetime(2022, 12, 1),
    max_active_runs=5,
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
) as dag:

    with open("include/GrupoB_salvador_universidad.sql", 'r') as myfile:
        data = myfile.read()
        print(data)
    
    @task()
    def Extraccion(**kwargs):
        try:
            hook = PostgresHook(postgres_conn_id="alkemy_db")
            df = hook.get_pandas_df(sql=data)
            df.to_csv("files/G1UNSalvador.csv")
            logging.info('Tarea de extraccion EXITOSA')
        except:
            logging.info('ERROR al extraer')
            
    Extraccion()
