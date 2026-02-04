import json
import logging
import os
from datetime import datetime
from typing import Literal

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from global_modules.functions import getLocalConfig
from sqlalchemy import create_engine
from urllib3 import filepost

ENVIROMENT = "PROD"

DAG_ID = "silver/SILVER_F_ELIPSE_DISPONIBILIDADE"

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


database_id = (
    os.environ.get("DATABASE_DEV")
    if ENVIROMENT == "DEV"
    else os.environ.get("DATABASE_PROD")
)
dw_conn = "postgres_eng_server_dev" if ENVIROMENT == "DEV" else "postgres_eng_server"

# Conexão com o Teams
TEAMS_WEBHOOK_URL = Variable.get("WEBHOOK_TEAMS")

# Dependências
yaml_data = getLocalConfig(DAG_ID)

dw_truncate = yaml_data["dw_commands"]["truncate_table"]
dw_merge = yaml_data["dw_commands"]["merge_paradas"]


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
def notify_teams_on_failure(context, database_id):
    message = f"""
    Ocurred an error in the following data pipeline:
    Dag_id:{context["dag"].dag_id}
    Run_id:{context["dag_run"].run_id}
    task_id = {context.get("task_instance").task_id}
    Status: Failure
    Event_date:{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
    Enviroment:{database_id}
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
    "on_failure_callback": lambda context: notify_teams_on_failure(
        context, database_id=database_id
    ),
}

extraction_sql = "extract_paradas.sql"


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
        df["ID_Grupo"] = df["ID_Grupo"].astype("Int64")
        df["Cracha_Operador"] = df["Cracha_Operador"].astype("Int64")
        df["Cracha_Preparador"] = df["Cracha_Preparador"].astype("Int64")
        df["Cracha_Lider"] = df["Cracha_Lider"].astype("Int64")
        df["Cracha_Apoio"] = df["Cracha_Apoio"].astype("Int64")
        df.to_parquet(file_path)

    except Exception as e:
        logging.error(f"FALHA NA CONEXÃO COM O BANCO DE DADOS: {str(e)}")
        send_teams_message(
            f"Erro de conexão com o banco de dados: {str(e)}", TEAMS_WEBHOOK_URL
        )
    return file_path


def concat_dataframes():
    file_path = os.path.join(PASTA_ATUAL, "concat_dataframes.parquet")
    file_sob = get_parquet_path("sob")
    file_for = get_parquet_path("for")
    file_cra = get_parquet_path("cra")

    df_sob = pd.read_parquet(file_sob)
    df_for = pd.read_parquet(file_for)
    df_cra = pd.read_parquet(file_cra)

    df = pd.concat([df_sob, df_cra, df_for], ignore_index=True)

    # df.to_parquet(
    #     f"/datalake/bronze/bronze_elipse_paradas/bronze_elipse_paradas{data_hora_atual}.parquet",
    #     index=False,
    # )

    df = df.drop(columns=["linha"])

    df = df.sort_values("E3TimeStamp").drop_duplicates(
        subset=["Id", "id_estabelecimento"], keep="last"
    )

    df.to_parquet(file_path)

    return file_path


def load_stage(**kwargs):
    ti = kwargs["ti"]
    df_path = ti.xcom_pull(task_ids="concat_dataframes")

    df = pd.read_parquet(df_path)

    hook = PostgresHook(postgres_conn_id=dw_conn)

    # Definindo o caminho do arquivo
    csv_file = os.path.join(PASTA_ATUAL, "temp_paradas.csv")

    df.to_csv(csv_file, index=False, header=False, sep=";", encoding="utf-8")

    conn = hook.get_conn()
    cursor = conn.cursor()

    with open(csv_file, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            f"COPY {database_id}.stage.stage_paradas FROM STDIN WITH CSV HEADER DELIMITER ';'",
            f,
        )

    conn.commit()
    cursor.close()
    conn.close()
    os.remove(csv_file)


with DAG(
    "SILVER_F_DISPONIBILIDADE_SIMON",
    default_args=default_args,
    schedule="30 9,18 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["elipse", "disponibilidade", "silver"],
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
    concat = PythonOperator(
        task_id="concat_dataframes", python_callable=concat_dataframes
    )

    load = PythonOperator(task_id="load_stage", python_callable=load_stage)

    truncate_table = PostgresOperator(
        task_id="truncate_table",
        sql=dw_truncate["sql"],
        postgres_conn_id=dw_conn,
        params={"source": dw_truncate["target"], "database_id": database_id},
    )

    merge_data = PostgresOperator(
        task_id="merge_stage_silver",
        postgres_conn_id=dw_conn,
        sql="./sql_files/merge_query.sql",
        params={
            "source": dw_merge["source"],
            "target": dw_merge["target"],
            "database_id": database_id,
        },
        autocommit=True,
    )

    vacuum_task = PostgresOperator(
        task_id="vacuum_task",
        sql=f"VACUUM {database_id}.silver.oee_fparadas;",
        postgres_conn_id=dw_conn,  # Certifique-se de que você tenha a conexão configurada no Airflow
        autocommit=True,  # Isso desabilita a transação para permitir o VACUUM
    )

    analyze_task = PostgresOperator(
        task_id="analyze_task",
        sql=f"ANALYZE {database_id}.silver.oee_fparadas;",
        postgres_conn_id=dw_conn,
        autocommit=True,
    )  # Usando TriggerDagRunOperator para acionar a DAG2 após a execução de DAG1

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="GOLD_F_DISPONIBILIDADE_SIMON",  # Nome da DAG a ser acionada
    )

(
    truncate_table
    >> extraction
    >> concat
    >> load
    >> vacuum_task
    >> analyze_task
    >> merge_data
    >> trigger_dag
)
