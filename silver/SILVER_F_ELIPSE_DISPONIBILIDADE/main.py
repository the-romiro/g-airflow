import json
import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

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
    payload = {"text": message}
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


# reads the sql file and returns the query
def read_sql_file(file_path: str):
    dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(dir, f"sql_files/{file_path}")

    with open(sql_dir, "r") as file:
        query = file.read()
    return query


default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
}


def extract_data_sob():
    conn = BaseHook.get_connection(connection_id_sob)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (20,)  # id_estabelecimento
    df = pd.read_sql_query(read_sql_file("extract_paradas.sql"), hook, params=params)
    df["ID_Grupo"] = df["ID_Grupo"].astype("Int64")

    return df


def extract_data_for():
    conn = BaseHook.get_connection(connection_id_for)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (21,)  # id_estabelecimento
    df = pd.read_sql_query(read_sql_file("extract_paradas.sql"), hook, params=params)
    df["ID_Grupo"] = df["ID_Grupo"].astype("Int64")

    return df


def extract_data_cra():
    conn = BaseHook.get_connection(connection_id_cra)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    hook = create_engine(url)
    params = (40,)  # id_estabelecimento
    df = pd.read_sql_query(read_sql_file("extract_paradas.sql"), hook, params=params)
    df["ID_Grupo"] = df["ID_Grupo"].astype("Int64")

    return df


def concat_data_and_load_temp(**kwargs):
    ti = kwargs["ti"]
    df_sob = ti.xcom_pull(task_ids="extract_data_sob")
    df_cra = ti.xcom_pull(task_ids="extract_data_cra")
    df_for = ti.xcom_pull(task_ids="extract_data_for")

    df = pd.concat([df_sob, df_cra, df_for], ignore_index=True)

    hook = PostgresHook(postgres_conn_id="postgres_eng_server")
    df.to_sql(
        "temp_paradas",
        hook.get_sqlalchemy_engine(),
        schema="silver",
        if_exists="replace",
        index=False,
        chunksize=5000,
    )


with DAG(
    "SILVER_F_DISPONIBILIDADE_SIMON",
    default_args=default_args,
    schedule="20 9,18 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    extract_sob = PythonOperator(task_id="extract_data_sob", python_callable=extract_data_sob)
    extract_for = PythonOperator(task_id="extract_data_for", python_callable=extract_data_for)
    extract_cra = PythonOperator(task_id="extract_data_cra", python_callable=extract_data_cra)
    concat_and_load = PythonOperator(
        task_id="concat_data_and_load_temp", python_callable=concat_data_and_load_temp
    )
    merge_data = PostgresOperator(
        task_id="merge_table_and_drop_temp",
        postgres_conn_id="postgres_eng_server",
        sql="./sql_files/merge_query.sql",
        autocommit=True
    )
    vacuum_task = PostgresOperator(
        task_id="vacuum_task",
        sql="VACUUM elipse.silver.oee_fparadas;",
        postgres_conn_id="postgres_eng_server",  # Certifique-se de que você tenha a conexão configurada no Airflow
        autocommit=True,  # Isso desabilita a transação para permitir o VACUUM
    )
    analyze_task = PostgresOperator(
        task_id="analyze_task",
        sql="ANALYZE elipse.silver.oee_fparadas;",
        postgres_conn_id="postgres_eng_server",
        autocommit=True,
    )  # Usando TriggerDagRunOperator para acionar a DAG2 após a execução de DAG1
    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="GOLD_F_DISPONIBILIDADE_SIMON",  # Nome da DAG a ser acionada
    )

(
    [extract_sob, extract_for, extract_cra]
    >> concat_and_load
    >> vacuum_task
    >> analyze_task
    >> merge_data
    >> trigger_dag
)
