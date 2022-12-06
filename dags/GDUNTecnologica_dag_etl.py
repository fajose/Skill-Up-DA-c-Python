from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from plugins.helper_functions import logger_setup
from plugins.helper_functions.extracting import extraction
import pandas as pd

# Universidad
university = 'GrupoD_tecnologica_universidad'

# Default args de airflow
default_args = {
    'owner': 'Gastón Orphant',
    'start_date': datetime(2022, 12, 1),
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'description':'Dag para la extracción, transformación y carga de la información de la Universidad Nacional Tecnologica'
}

# Configuracion del logger
logger = logger_setup.logger_creation(university)

# Funciones de python

#Extraccion de datos

def extract():
    logger.info('Inicio de proceso de extracción')
    try:
        extraction(university)
        logger.info("Se creo el csv con la información de la universidad.")
    except Exception as e:
        logger.error(e)

# Definimos el DAG
with DAG(f'{university}_dag_etl',
         default_args=default_args,
         catchup=False,
         schedule_interval='@hourly'
         ) as dag:      
    
    extraccion = PythonOperator(task_id = 'extraccion', python_callable=extract)
    
    extraccion