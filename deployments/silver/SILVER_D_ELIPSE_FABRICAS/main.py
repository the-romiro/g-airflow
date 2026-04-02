from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from global_modules.database import Estabelecimento, get_elipse_conn, get_estab_code
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import get_parquet_file, read_sql_file

default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
}

extraction_sql = "extract_query.sql"

parquet_all = get_parquet_file('all', __file__)
parquet_sob = get_parquet_file("sob", __file__)
parquet_for = get_parquet_file("for", __file__)
parquet_cra = get_parquet_file("cra", __file__)


@task
def extract_data(estab: Estabelecimento):
    parquet_file = get_parquet_file(estab, __file__)

    if parquet_file.exists():
        return

    conn = get_elipse_conn(estab)
    params = (get_estab_code(estab),)  # id_estabelecimento
    sql = read_sql_file(extraction_sql, __file__)
    df = pd.read_sql_query(sql, conn, params=params)
    df.to_parquet(parquet_file)


@task
def concat_df_and_load_temp():

    if parquet_all.exists():
        return

    df_sob = pd.read_parquet(parquet_sob)
    df_for = pd.read_parquet(parquet_for)
    df_cra = pd.read_parquet(parquet_cra)

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

    df.to_parquet(parquet_all)


@task
def clear_cache():
    for f in [parquet_all, parquet_cra, parquet_for, parquet_sob]:
        f.unlink(missing_ok=True)


@dag(
    dag_id="SILVER_D_FABRICAS_SIMON",
    default_args=default_args,
    schedule="10 9 * * *",
    catchup=False,
    max_active_runs=1,
)
def dag_factory():
    sob_data = extract_data.override(task_id="extract_data_sob")("sob")
    for_data = extract_data.override(task_id="extract_data_for")("for")
    cra_data = extract_data.override(task_id="extract_data_cra")("cra")

    concat = concat_df_and_load_temp()

    merge = SQLExecuteQueryOperator(
        task_id="merge_table_and_drop_temp",
        conn_id="postgres_eng_server",
        sql="./sql_files/merge_query.sql",
    )

    clear = clear_cache()

    # dependencies
    _ = [sob_data, for_data, cra_data] >> concat >> merge >> clear


dag_factory()
