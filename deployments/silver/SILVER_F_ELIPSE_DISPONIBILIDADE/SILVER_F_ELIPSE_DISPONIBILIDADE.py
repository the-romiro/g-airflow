import logging
import os
from datetime import datetime
from typing import Literal

import pandas as pd
from airflow import DAG
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine

from global_modules.functions import get_local_config
from global_modules.ms_teams import notify_teams_on_failure, send_teams_message

ENVIRONMENT = "PROD"

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
    os.environ.get("DATABASE_DEV") if ENVIRONMENT == "DEV" else os.environ.get("DATABASE_PROD")
)
dw_conn = "postgres_eng_server_dev" if ENVIRONMENT == "DEV" else "postgres_eng_server"

# Dependências
yaml_data = get_local_config("silver/SILVER_F_ELIPSE_DISPONIBILIDADE")

dw_truncate = yaml_data["dw_commands"]["truncate_table"]
dw_merge = yaml_data["dw_commands"]["merge_paradas"]


# reads the sql file and returns the query
def read_sql_file(file_path: str):
    _dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(_dir, f"sql_files/{file_path}")

    with open(sql_dir, "r") as file:
        query = file.read()
    return query


default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
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
        send_teams_message(f"Erro de conexão com o banco de dados: {str(e)}")
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

    # TODO: Usar with para instruções abaixo.
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


gold_dataset = Dataset("elipse://gold/f_disponibilidade_simon")

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

    concat = PythonOperator(task_id="concat_dataframes", python_callable=concat_dataframes)

    load = PythonOperator(task_id="load_stage", python_callable=load_stage)

    truncate_table = SQLExecuteQueryOperator(
        task_id="truncate_table",
        sql=dw_truncate["sql"],
        conn_id=dw_conn,
        params={"source": dw_truncate["target"], "database_id": database_id},
    )

    merge_data = SQLExecuteQueryOperator(
        task_id="merge_stage_silver",
        conn_id=dw_conn,
        sql="./sql_files/merge_query.sql",
        params={
            "source": dw_merge["source"],
            "target": dw_merge["target"],
            "database_id": database_id,
        },
        autocommit=True,
        outlets=[gold_dataset],
    )

    vacuum_task = SQLExecuteQueryOperator(
        task_id="vacuum_task",
        sql=f"VACUUM {database_id}.silver.oee_fparadas;",
        conn_id=dw_conn,
        autocommit=True,  # Isso desabilita a transação para permitir o VACUUM
    )

    analyze_task = SQLExecuteQueryOperator(
        task_id="analyze_task",
        sql=f"ANALYZE {database_id}.silver.oee_fparadas;",
        conn_id=dw_conn,
        autocommit=True,
    )

_ = truncate_table >> extraction >> concat >> load >> vacuum_task >> analyze_task >> merge_data
