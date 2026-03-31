import logging
import os
from datetime import datetime
from typing import Literal

import pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

from global_modules.ms_teams import notify_teams_on_failure

# Pegando a pasta onde o script está
PASTA_ATUAL = os.path.dirname(os.path.abspath(__file__))

# Nome da conexão definida no Airflow Connections
connection_id_sob = "elipse_sob"
connection_id_for = "elipse_for"
connection_id_cra = "elipse_cra"

type Estabelecimento = Literal["sob", "cra", "for"]


def get_parquet_path(estab: Estabelecimento):
    file_path = os.path.join(PASTA_ATUAL, f"extract_data_{str(estab)}.parquet")
    return file_path


# reads the sql file and returns the query
def read_sql_file(file_path):
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

extraction_sql = "extract_query.sql"


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
        notify_teams_on_failure(f"Erro de conexão com o banco de dados: {str(e)}")
    return file_path


def concat_df_and_load_temp():

    file_path = os.path.join(PASTA_ATUAL, "concat_dataframes.parquet")
    file_sob = get_parquet_path("sob")
    file_for = get_parquet_path("for")
    file_cra = get_parquet_path("cra")

    df_sob = pd.read_parquet(file_sob)
    df_for = pd.read_parquet(file_for)
    df_cra = pd.read_parquet(file_cra)

    df = pd.concat([df_sob, df_cra, df_for], ignore_index=True)
    hook = PostgresHook(postgres_conn_id="postgres_eng_server")
    df.to_sql(
        "temp_fabricas",
        hook.get_sqlalchemy_engine({"executemany_mode": "values"}),
        schema="silver",
        if_exists="replace",
        index=False,
        chunksize=1000,
    )

    df.to_parquet(file_path)

    return file_path


with DAG(
    "SILVER_D_FABRICAS_SIMON",
    default_args=default_args,
    schedule="10 9 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    extract_sob = PythonOperator(
        task_id="extract_data_sob", python_callable=extract_data, op_args=[20]
    )
    extract_for = PythonOperator(
        task_id="extract_data_for", python_callable=extract_data, op_args=[21]
    )
    extract_cra = PythonOperator(
        task_id="extract_data_cra", python_callable=extract_data, op_args=[40]
    )

    concat_and_load_temp = PythonOperator(
        task_id="concat_data_and_load_temp", python_callable=concat_df_and_load_temp
    )
    merge_data = SQLExecuteQueryOperator(
        task_id="merge_table_and_drop_temp",
        conn_id="postgres_eng_server",
        sql="./sql_files/merge_query.sql",
    )

    _ = [extract_sob, extract_for, extract_cra] >> concat_and_load_temp >> merge_data
