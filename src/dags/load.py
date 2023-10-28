import requests
import pandas as pd
import psycopg2
# import pymongo
from airflow import DAG
from airflow.operators import PostgresOperator
from airflow.operators import DummyOperator
from airflow.operators import PythonOperator
import datetime

connect_to_db = psycopg2.connect("host=localhost port=15432 dbname=de user=jovyan password=jovyan")

LIMIT = 50
OFFSET = 0
SORT_DIRECTION = 'asc'
ENTITIES = ('restaurants', 'deliveries', 'couriers')
HEADERS = {
    'X-Nickname': 'nikita-kovalev17',
    'X-Cohort': '18',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
}

def get_data_from_api(entity: str, offset, is_snapshot):
    if not is_snapshot:
        if entity == 'restaurants':
            sort_field = '_id'
        elif entity == 'deliveries':
            sort_field = 'order_id'
        elif entity == 'couriers':
            sort_field = '_id'
        raw_data = requests.get(f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{entity}?sort_field={sort_field}&sort_direction={SORT_DIRECTION}&limit={LIMIT}&offset={offset}", headers=HEADERS)
    else:
        raw_data = requests.get(f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{entity}", headers=HEADERS)

    json_data = raw_data.json()
    df = pd.DataFrame(json_data)
    for row in df.values:
        pass


with DAG(
    dag_id='test_dag_id',
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    
    dummy = DummyOperator(
        task_id="dummy"
    )

    api_get_data = PythonOperator(
        python_callable=get_data_from_api,
        task_id="get_data_from_api"
    )

    stg_ddl = PostgresOperator(
        task_id="stg_ddl",
        sql='sql/stg.sql'
    )

    dds_ddl = PostgresOperator(
        task_id="dds_ddl",
        sql='sql/dds.sql'
    )

    cdm_ddl = PostgresOperator(
        task_id="cdm_ddl",
        sql='sql/cdm.sql'
    )

    datamart_ddl = PostgresOperator(
        task_id="datamart_ddl",
        sql='sql/datamart.sql'
    )

    
dummy>>[stg_ddl>>dds_ddl>>cdm_ddl]>>api_get_data>>datamart_ddl
