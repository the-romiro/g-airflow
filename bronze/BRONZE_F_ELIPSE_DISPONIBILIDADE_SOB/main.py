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
connection_id_sob = "elipse_sob"
connection_id_for = "elipse_for"
connection_id_cra = "elipse_cra"

# Obtendo a conexão
conn = BaseHook.get_connection(connection_id_sob)

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
    conn = BaseHook.get_connection(connection_id_sob)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (20,) # id_estabelecimento
    df_sob = pd.read_sql_query(read_sql_file('extract_paradas.sql'), hook, params=params)

    conn = BaseHook.get_connection(connection_id_cra)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (40,) # id_estabelecimento
    df_cra = pd.read_sql_query(read_sql_file('extract_paradas.sql'), hook, params=params)

    conn = BaseHook.get_connection(connection_id_for)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (21,) # id_estabelecimento
    df_for = pd.read_sql_query(read_sql_file('extract_paradas.sql'), hook, params=params)

    df = pd.concat([df_sob, df_cra, df_for], ignore_index=True)

    df['ID_Grupo'] = df['ID_Grupo'].astype('Int64')

    hook = PostgresHook(postgres_conn_id='postgres_eng_server')
    df.to_sql("temp_paradas", hook.get_sqlalchemy_engine(), schema="oee", if_exists="replace", index=False, chunksize=1000)


with DAG(
    'BRONZE_F_DISPONIBILIDADE_SIMON',
    default_args=default_args, 
    schedule='10 9 * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    task = PythonOperator(
        task_id="extract_and_load_temp_table",
        python_callable=extract_data
    )
    task2 = PostgresOperator(
        task_id="merge_table_and_drop_temp",
        postgres_conn_id = "postgres_eng_server",
        sql="./sql_files/merge_query.sql"
    )

task >> task2