import json
import logging
import os
from datetime import datetime
from typing import Literal

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from urllib3 import filepost

# Pegando a pasta onde o script está
PASTA_ATUAL = os.path.dirname(os.path.abspath(__file__))

# Obter data e hora atual
data_hora_atual = (datetime.now()).strftime("%Y%m%d%H%M%S")

# Nome da conexão definida no Airflow Connections
connection_id_sob = "elipse_sob"
connection_id_for = "elipse_for"
connection_id_cra = "elipse_cra"

type Estabelecimento = Literal["sob", "cra", "for"]


def get_parquet_path(estab: Estabelecimento):
    file_path = os.path.join(PASTA_ATUAL, f"extract_data_{str(estab)}.parquet")
    return file_path


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
        raise ValueError(
            f"Failed to send message: {response.status_code}, {response.text}"
        )


# Função para enviar a mensagem
def notify_teams_on_failure(context):
    message = f"""
    Ocurred an error in the following data pipeline:
    Dag_id:{context["dag"].dag_id}
    Run_id:{context["dag_run"].run_id}
    task_id = {context.get("task_instance").task_id}
    Status: Failure
    Event_date:{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
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
    "start_date": datetime(2025, 2, 21, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
}

extraction_sql = "extract_performance.sql"


def extract_data(cod_estab: int):
    match cod_estab:
        case 20:
            file_path = get_parquet_path("sob")
            conn = BaseHook.get_connection(connection_id_sob)
        case 21:
            file_path = get_parquet_path("for")
            conn = BaseHook.get_connection(connection_id_for)
        case 40:
            file_path = get_parquet_path("cra")
            conn = BaseHook.get_connection(connection_id_cra)
        case _:
            raise ValueError("Código de estabelecimento inválido")

    try:
        url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
        hook = create_engine(url)
        params = (cod_estab,)  # id_estabelecimento
        df = pd.read_sql_query(read_sql_file(extraction_sql), hook, params=params)
        df.to_parquet(file_path)

    except Exception as e:
        logging.error(f"FALHA NA CONEXÃO COM O BANCO DE DADOS: {str(e)}")
        send_teams_message(
            f"Erro de conexão com o banco de dados: {str(e)}", TEAMS_WEBHOOK_URL
        )
    return file_path


def concat_data_and_load_temp():
    file_path = os.path.join(PASTA_ATUAL, "concat_dataframes.parquet")
    file_sob = get_parquet_path("sob")
    file_for = get_parquet_path("for")
    file_cra = get_parquet_path("cra")

    df_sob = pd.read_parquet(file_sob)
    df_for = pd.read_parquet(file_for)
    df_cra = pd.read_parquet(file_cra)

    df = pd.concat([df_sob, df_cra, df_for], ignore_index=True)

    # df.to_parquet(
    #     f"/datalake/bronze/bronze_elipse_performance/bronze_elipse_performance{data_hora_atual}.parquet", index=False
    # )

    df = df.drop(columns=["linha"])

    df = df.sort_values("E3TimeStamp").drop_duplicates(
        subset=["E3TimeStamp", "Maquina_ID", "id_estabelecimento"], keep="last"
    )

    hook = PostgresHook(postgres_conn_id="postgres_eng_server")
    df.to_sql(
        "temp_performance",
        hook.get_sqlalchemy_engine(),
        schema="silver",
        if_exists="replace",
        index=False,
        chunksize=5000,
    )

    df.to_parquet(file_path)

    return file_path


with DAG(
    "SILVER_F_PERFORMANCE_SIMON",
    default_args=default_args,
    schedule="20 9,18 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["elipse", "performance", "silver"],
) as dag:
    with TaskGroup("extract_all") as extraction:
        extract_sob = PythonOperator(
            task_id="extract_data_sob", python_callable=extract_data, op_args=[20]
        )
        extract_for = PythonOperator(
            task_id="extract_data_for", python_callable=extract_data, op_args=[21]
        )
        extract_cra = PythonOperator(
            task_id="extract_data_cra", python_callable=extract_data, op_args=[40]
        )

    concat_and_load = PythonOperator(
        task_id="concat_data_and_load_temp", python_callable=concat_data_and_load_temp
    )
    merge_data = PostgresOperator(
        task_id="merge_table_and_drop_temp",
        postgres_conn_id="postgres_eng_server",
        sql="./sql_files/merge_query.sql",
        autocommit=True,
    )
    vacuum_task = PostgresOperator(
        task_id="vacuum_task",
        sql="VACUUM elipse.silver.oee_fperformance;",
        postgres_conn_id="postgres_eng_server",  # Certifique-se de que você tenha a conexão configurada no Airflow
        autocommit=True,  # Isso desabilita a transação para permitir o VACUUM
    )
    analyze_task = PostgresOperator(
        task_id="analyze_task",
        sql="ANALYZE elipse.silver.oee_fperformance;",
        postgres_conn_id="postgres_eng_server",
        autocommit=True,
    )  # Usando TriggerDagRunOperator para acionar a DAG2 após a execução de DAG1
    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="GOLD_F_PERFORMANCE_SIMON",  # Nome da DAG a ser acionada
    )

(
    extraction
    >> concat_and_load
    >> vacuum_task
    >> analyze_task
    >> merge_data
    >> trigger_dag
)
