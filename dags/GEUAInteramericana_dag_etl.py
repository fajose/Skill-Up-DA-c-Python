import os
import logging
#ariflow imports
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

os.chdir('./dags')
university = 'GEAUInteramericana'
logger = logging.getLogger(university)
logger.setLevel('INFO')
logPath = f'./logs/{university}.log'
fileHandler = logging.FileHandler(filename=logPath, delay=True)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

def extraction():
    pass

default_args = {
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
    transform = EmptyOperator(task_id='Transform') #pythonOperator?
    load = EmptyOperator(task_id='load') #pythonOperator > s3Hook
    extract >> transform >> load