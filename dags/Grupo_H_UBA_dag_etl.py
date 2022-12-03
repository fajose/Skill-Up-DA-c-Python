from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime, timedelta
import logging
import pandas as pd


# Universidad
university = 'GrupoH_UBA'

# Diccionario con argumentos por defecto para pasar al DAG
default_args = {
    'owner': 'Fabian Manrique',
    'start_date': datetime(2022, 12, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'description':"Dag para la extracción, transformación y cargue de la información de la facultad latinoamericana de Ciencias Sociales",
}

# Configuracion del logger
logger = logging.getLogger()
fhandler = logging.FileHandler(filename=f'dags/logs/{university}.log', mode='a')
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
logger.setLevel(logging.DEBUG)

def column_processor(df):
    df = df.str.lower()
    df = df.str.replace('-', ' ')
    df = df.str.strip()
    return df

# Definimos el DAG
with DAG(f'{university}_dag_etl',
         schedule_interval='@hourly',
         default_args=default_args,
         catchup=False
         ) as dag:      
    
    # Extracción de Datos
    @task()
    def extract():
        logger.info('Inicio de proceso de extracción')


        try:
            # Lee el archi .sql con el query para consultar los datos de la universidad
            with open(f'include/{university}.sql','r', encoding='utf-8') as f:
                sql_script = f.read()

            # Inicia el hook a Postgres para conectar a la base de datos
            hook = PostgresHook(postgres_conn_id="postgres_alkemy")

            # Se crea el dataframe y lo exporta a la carpeta files
            df = hook.get_pandas_df(sql=sql_script)
            df.to_csv(f"files/{university}.csv")
            logger.info("Se creo el csv con la información de la universidad")

        except Exception as e:
            logger.error(e)
    
    # Transformación de Datos
    @task()
    def transform():
        logger.info('Inicia proceso de transformación de los datos')

        try:
            df = pd.read_csv(f"files/{university}.csv", index_col=0)

            df['last_name'] = df['first_name'].str.split('-', expand = True)[1]
            df['first_name'] = df['first_name'].str.split('-', expand = True)[0]

            columns_to_transform = ['university', 'career', 'first_name', 'last_name']
            for column in columns_to_transform:
                df[column] = column_processor(df[column])

            df.to_csv(f'datasets/{university}_transformed.csv')
            logger.info('TXT AND CSV FILES CREATED')
            
        except Exception as e:
            logger.error(e)
    
    extract() >> transform()