from airflow import DAG
from airflow.decorators import task

from datetime import datetime, timedelta
from helper_functions import logger_setup
from helper_functions.utils import *
from helper_functions.transforming import transformation
from helper_functions.extracting import extraction
import pandas as pd


# Universidad
university = 'GrupoB_comahue_universidad'

# Configuracion del logger
logger = logger_setup.logger_creation(university)

# Definimos el DAG
with DAG(f'{university}_dag_etl',
         default_args=default_args,
         catchup=False
         ) as dag:      
    
    # Extracción de Datos
    @task()
    def extract():
        logger.info('Inicio de proceso de extracción')
        try:
            extraction(university)
            logger.info("Se creo el csv con la información de la universidad")

        except Exception as e:
            logger.error(e)
    
    # Transformación de Datos
    @task()
    def transform():
        logger.info('Inicia proceso de transformación de los datos')

        try:
            transformation(university)
            logger.info('Se creo archivo csv con la información transformada')
            
        except Exception as e:
            logger.error(e)
    
    extract() >> transform()
