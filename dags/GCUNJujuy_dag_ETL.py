import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from helper_functions import logger_setup
from helper_functions.extracting import extraction
from helper_functions.transforming import transformer
import pandas as pd
#asd
def transform(database_path_csv:str) -> pd.DataFrame:

    DataLocationPostal_Code = open('../assets/codigos_postales.csv', 'r')  #abrir csv codigos postales y guardarlos
    data_path=open('../files/{}'.format(database_path_csv), encoding="utf8")


    data=pd.read_csv(data_path)
    dataLocation = pd.read_csv(DataLocationPostal_Code)    # leer y guardar en DataFrame los codigos postales y guardarlos en dataframe
    dataLocation = dataLocation.rename(columns={'localidad': 'location',
                                                'codigo_postal': 'postal_code'})  # Transformo los nombres de la columnas del cdv de codigos_postales
    dataLocation['location'] = dataLocation[
        'location'].str.lower()  # Transformo los valores de localidad de codigos postales


    for col in data.columns:  # convertimos todo a minusculas
        if data[col].dtype == 'object':
            data[col] = (data[col].str.lower()
                         .str.replace('-', ' ')  # reemplazamos los guiones internos por espacios para separar palabras
                         .str.replace('_', ' ')  # reemplazamos los guiones internos por espacios para separar palabras
                         .str.lstrip('-')  # sacamos guiones al inicio
                         .str.rstrip('-'))  # sacamos guiones al final

    data.first_name = (data.first_name
                  .str.split('-')  # separo por -
                  .apply(lambda x: " ".join(x))  # uno la lista con espacios
                  .str.split('\.', n=1)  # separo todas aquellas ocurrencias que contengan . como dr., ms., etc
                  .apply(
        lambda x: x[1] if len(x) >= 2 else x)  # me quedo con el segundo elemento si la lista tiene un largo mayor a 2
                  .apply(
        lambda x: "".join(x) if type(x) == list else x)  # convierto a string los elementos de tipo lista
                  .str.strip()
                  )
    data.gender = data.loc[:, ['gender']].replace('m', 'male').replace('f', 'female')

    nom_apell = (data.first_name
                 .str.rsplit(expand=True, n=1)
                 .rename(columns={0: 'first_name', 1: 'last_name'}))
                 
    data[['first_name','last_name']] = nom_apell
    data.inscription_date = pd.to_datetime(data.inscription_date, infer_datetime_format=True)

    for data in [data]:
        data.age = pd.to_datetime(data.age , infer_datetime_format=True)
        age = data.age.apply(
        lambda x: dt.date.today().year - x.year - ((dt.date.today().month, dt.date.today().day) < (x.month, x.day)))
        data['age'] = age

    if 'location' in data.columns:  # Agrego las columnas postal_code o location

        data = data.merge(dataLocation, how='inner', on='location', copy=False, suffixes=('X', ''))
    else:
        data['postal_code'] = data['postal_code'].astype(int).replace('.0','')
        data = data.merge(dataLocation, how='inner', on='postal_code', copy=False, suffixes=('X', ''))
    


    data = data.loc[:,~data.columns.str.contains('X')]# drop columna vacía

    data.to_csv('./datasets/jujuy.txt', header=None, index=None, sep=' ', mode='a')

    return data




# Universidad
university = 'GrupoC_jujuy_universidad'

# Default args de airflow
default_args = {
    'owner': 'Gastón Orphant',
    'start_date': datetime(2022, 12, 1),
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'description':'Dag para la extracción, transformación y carga de la información de la Universidad Nacional de Jujuy'
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
    
    transformacion = PythonOperator(task_id = 'transform', python_callable=transform, args='files/GrupoC_jujuy_universidad_select.csv')

    extraccion