from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres         import PostgresHook
from airflow.operators.python                          import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine

# Nome da conexão definida no Airflow Connections
connection_id = "elipse_sob"

# Obtendo a conexão
conn = BaseHook.get_connection(connection_id)

#reads the sql file and returns the query
def read_sql_file(file_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(dir, f'sql_files/{file_path}')

    with open(sql_dir, 'r')  as file:
        query = file.read()
    return query


default_args = {
    'owner': 'yan arcanjo',
    'start_date': datetime(2025, 1, 26, 6, 5)
}

def extract_data():
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    df = pd.read_sql_query(read_sql_file('extract_query.sql'), hook)

    hook = PostgresHook(postgres_conn_id='postgres_eng_server')
    df.to_sql("temp_maquinas", hook.get_sqlalchemy_engine(), schema="cadastros", if_exists="replace", index=False, chunksize=1000)


with DAG(
    'BRONZE_D_MAQUINAS_SIMON',
    default_args=default_args, 
    schedule='10 9 * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    task1 = PythonOperator(
        task_id="extract_and_load_temp_table",
        python_callable=extract_data
    )
    task2 = PostgresOperator(
        task_id="merge_table_and_drop_temp",
        postgres_conn_id = "postgres_eng_server",
        sql="./sql_files/merge_query.sql"
    )

    task1 >> task2   