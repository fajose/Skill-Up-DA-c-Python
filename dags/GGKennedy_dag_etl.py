from airflow import DAG
from airflow.decorators import task

from plugins.helper_functions import logger_setup
from plugins.helper_functions.utils import *
from plugins.helper_functions.transformer import Transformer
from plugins.helper_functions.extractor import Extractor
from plugins.helper_functions.loader import Loader


# Universidad
university = 'GrupoG_Kennedy'

# Configuracion del logger
logger = logger_setup.logger_creation(university)

# Definimos el DAG
with DAG(f'{university}_dag_etl',
         default_args=default_args,
         catchup=False,
         schedule_interval='@hourly'
         ) as dag:      
    
    # Extracción de Datos
    @task()
    def extract(**kwargd):
        df_extractor = Extractor(university, logger)
        df_extractor.to_extract()
    
    # Transformación de Datos
    @task()
    def transform(**kwargd):
        df_transformer = Transformer(university, logger)
        df_transformer.to_transform()

    @task()
    def load(**kwargd):
        df_loader = Loader(university, logger)
        df_loader.to_load()
    
    extract() >> transform() >> load()