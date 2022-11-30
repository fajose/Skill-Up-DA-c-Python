from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import timedelta, datetime

default_args = {
    'retries': '5',
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'UMoron_dag_etl',
    default_args= default_args,
    schedule= '@hourly',
    start_date= datetime(2022, 11, 28)
) as dag:
    extract = EmptyOperator(task_id='Extract') #pythonOperator > postgresOperator/ postgresHook ?
    transform = EmptyOperator(task_id='Transform') #pythonOperator?
    load = EmptyOperator(task_id='load') #pythonOperator > s3Hook
    extract >> transform >> load