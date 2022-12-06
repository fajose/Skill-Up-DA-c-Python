import pandas as pd
#ariflow imports
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta, datetime
#helper functions imports
from plugins.helper_functions.logger_setup import logger_creation
from plugins.helper_functions.extracting import extraction

university = 'GrupoE_interamericana_universidad'

#logger config
logger = logger_creation(university)

# Connection with database
POSTGRES_ID = "alkemy_db"

def extract():
    try:
        logger.info('Beggining of ETL extraction')
        extraction(university)
        logger.info('Extraction finished without errors')
    except Exception as err:
        logger.error(err)
        raise

def transformation():
    pass

default_args = {
    'owner': 'Leandro Serra',
    'retries': '5',
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'UAInteramericana_dag_etl',
    default_args= default_args,
    schedule= '@hourly',
    start_date= datetime(2022, 11, 28)
) as dag:
    extract = PythonOperator(
        task_id='Extract',
        python_callable=extract
    )
    transform = PythonOperator(
        task_id='transfrom',
        python_callable=transformation
    )
    load = EmptyOperator(task_id='load') #pythonOperator > s3Hook
    extract >> transform >> load