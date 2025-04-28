from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres         import PostgresHook
from airflow.operators.python                          import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine
import requests
import json

# Nome da conexão definida no Airflow Connections
connection_id_sob = "elipse_sob"
connection_id_for = "elipse_for"
connection_id_cra = "elipse_cra"

# Conexão com o Teams
TEAMS_WEBHOOK_URL = Variable.get("WEBHOOK_TEAMS")

def send_teams_message(message: str, webhook_url: str):
    """
    Envia uma mensagem para um canal do Microsoft Teams usando o Webhook.

    :param message: A mensagem a ser enviada.
    :param webhook_url: A URL do webhook do Microsoft Teams.
    """
    headers = {"Content-Type": "application/json"}
    payload = {
        "text": message
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        raise ValueError(f"Failed to send message: {response.status_code}, {response.text}")
    
# Função para enviar a mensagem
def notify_teams_on_failure(context):
    message = f"""
    Ocurred an error in the following data pipeline:
    Dag_id:{context['dag'].dag_id}
    Run_id:{context['dag_run'].run_id}
    task_id = {context.get('task_instance').task_id}
    Status: Failure
    Event_date:{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
    """
    send_teams_message(message, TEAMS_WEBHOOK_URL)

#reads the sql file and returns the query
def read_sql_file(file_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(dir, f'sql_files/{file_path}')

    with open(sql_dir, 'r')  as file:
        query = file.read()
    return query


default_args = {
    'owner': 'yan arcanjo',
    'start_date': datetime(2025, 1, 26, 6, 5),
    'on_failure_callback': notify_teams_on_failure
}

def extract_data_sob():
    # Obtendo a conexão
    conn = BaseHook.get_connection(connection_id_sob)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (20,) #id_estabelecimento
    df = pd.read_sql_query(read_sql_file('extract_query.sql'), hook, params=params)

    return df
   

def extract_data_cra():
    # Obtendo a conexão
    conn = BaseHook.get_connection(connection_id_cra)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (40,) #id_estabelecimento
    df = pd.read_sql_query(read_sql_file('extract_query.sql'), hook, params=params)

    return df

def extract_data_for():
    # Obtendo a conexão
    conn = BaseHook.get_connection(connection_id_for)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (21,) #id_estabelecimento
    df = pd.read_sql_query(read_sql_file('extract_query.sql'), hook, params=params)

    return df

def concat_df_and_load_temp(**kwargs):
    ti = kwargs['ti']

    df_sob = ti.xcom_pull(task_ids='extract_data_sob')
    df_cra = ti.xcom_pull(task_ids='extract_data_cra')
    df_for = ti.xcom_pull(task_ids='extract_data_for')

    df = pd.concat([df_sob, df_cra, df_for], ignore_index=True)
    hook = PostgresHook(postgres_conn_id='postgres_eng_server')
    df.to_sql("temp_setores", hook.get_sqlalchemy_engine(), schema="silver", if_exists="replace", index=False, chunksize=1000)

with DAG(
    'SILVER_D_SETORES_SIMON',
    default_args=default_args, 
    schedule='10 9 * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    extract_sob = PythonOperator(
        task_id="extract_data_sob",
        python_callable=extract_data_sob
    )
    extract_for = PythonOperator(
        task_id="extract_data_for",
        python_callable=extract_data_for
    )
    extract_cra = PythonOperator(
        task_id="extract_data_cra",
        python_callable=extract_data_cra
    )
    concat_and_load_temp = PythonOperator(
        task_id="concat_data_and_load_temp",
        python_callable=concat_df_and_load_temp
    )
    merge_data = PostgresOperator(
        task_id="merge_table_and_drop_temp",
        postgres_conn_id = "postgres_eng_server",
        sql="./sql_files/merge_query.sql"
    )

    [extract_sob, extract_for, extract_cra] >> concat_and_load_temp >> merge_data