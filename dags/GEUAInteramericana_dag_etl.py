import logging
import pandas as pd
#ariflow imports
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta, datetime

university = 'GEAUInteramericana'

#logger config
logger = logging.getLogger(university)
logger.setLevel('INFO')
logPath = f'./dags/logs/{university}.log'
fileHandler = logging.FileHandler(filename=logPath, delay=True)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

# Connection with database
POSTGRES_ID = "alkemy_db"

def extraction():
    logger.info('Beggining of ETL extraction')
    with open('./include/GrupoE_interamericana_universidad.sql', 'r', encoding='utf-8') as sqlFile:
        sqlQuery = sqlFile.read()
    hook = PostgresHook(postgres_conn_id=POSTGRES_ID)
    connection = hook.get_conn()
    df = hook.get_pandas_df(sql=sqlQuery)
    df.to_csv(path_or_buf=f'./files/{university}.csv')
    connection.close()
    logger.info('Extraction finished without errors')

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
        python_callable=extraction
        )
    transform = PythonOperator(
        task_id='transfrom',
        python_callable=transformation
    )
    load = EmptyOperator(task_id='load') #pythonOperator > s3Hook
    extract >> transform >> load