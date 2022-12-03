from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import timedelta, datetime
import os
from pathlib import Path
import pandas as pd

# Connection with database
POSTGRES_ID = "alkemy_db"
AWS_ID = "aws_s3_bucket"

def extraction():
    query = ""
    with open("./include/GrupoC_jujuy_universidad.sql", "r", encoding='utf-8') as database:
        query = database.read()
    hook = PostgresHook(postgres_conn_id=POSTGRES_ID)
    conection = hook.get_conn()
    df = hook.get_pandas_df(sql=query)    
    df.to_csv('./files/GC_UNJujuy_select.csv', index=False)
    conection.close()

def processing_with_pandas():
    df = pd.read_csv('./files/GC_UNJujuy_select.csv', index_col=False)
    
def upload_to_AWS():
    pass

with DAG(
    'GCUNJujuy_dag_ETL',
    description='GRUPO C ETL de Universidad de Jujuy',
    schedule_interval= timedelta(days=1),
    start_date= datetime(2022, 12, 1),  
) as dag:

    extraccion = PythonOperator(task_id="extraccion", python_callable=extraction)
    procesamiento = PythonOperator(task_id="procesamiento", python_callable=processing_with_pandas)
    subida = PythonOperator(task_id="subida", python_callable=upload_to_AWS)

    
    extraccion >> procesamiento >> subida