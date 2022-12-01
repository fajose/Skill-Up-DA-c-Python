from datetime import timedelta,datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator

with DAG(
    'Glsc_dag_etl',
    description='First Dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,11,28)
) as dag:
    tarea_1 = DummyOperator(task_id='tarea_1')
    tarea_2 = DummyOperator(task_id='tarea_2')
    tarea_3 = DummyOperator(task_id='tarea_3')

    tarea_1 >> [tarea_2, tarea_3]