import os
from datetime import datetime

import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from sqlalchemy import text

default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2024, 6, 30, 7, 0),
}


# reads the sql file and returns the query
def read_sql_file(file_path):
    _dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(_dir, file_path)

    with open(sql_dir, "r") as file:
        query = file.read()
    return query


@task
def populate_f_segmentacao_simon(logical_date: datetime | None = None):
    engine = MsSqlHook(mssql_conn_id="db_engenharia").get_sqlalchemy_engine()

    df = pd.read_sql_query(
        read_sql_file("sql_files/DBO.SIM_EQUIPAMENTOS.SQL"),
        engine,
    )

    merge_query = read_sql_file("sql_files/insert_DBO.FSEGMENTACAO_SIMON.sql")

    params_list = df.to_dict(orient="records")

    now = pendulum.now("America/Sao_Paulo")

    for row in params_list:
        row["data"] = logical_date.strftime("%Y-%m-%d")  # pyright: ignore
        row["updated_at"] = now

    with engine.begin() as conn:
        conn.execute(text(merge_query), params_list)  # pyright: ignore[reportCallIssue]


with DAG(
    "F_SIMON_SEGMENTACAO",
    default_args=default_args,
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    populate_f_segmentacao_simon()
