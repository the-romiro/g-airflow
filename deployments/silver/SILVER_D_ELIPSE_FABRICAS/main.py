from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

from global_modules.database import Estabelecimento, get_elipse_conn, get_estab_code
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import get_parquet_file, read_sql_file

log = LoggingMixin().log

default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

extraction_sql = "extract_query.sql"


def _get_parquet_all() -> Path:
    return get_parquet_file("all", __file__)


def _get_parquet_map() -> dict[Estabelecimento, Path]:
    return {
        "sob": get_parquet_file("sob", __file__),
        "for": get_parquet_file("for", __file__),
        "cra": get_parquet_file("cra", __file__),
    }


@task(
    retries=3,
    retry_delay=timedelta(minutes=2),
    multiple_outputs=False,
)
def extract_data(estab: Estabelecimento):
    parquet_map = _get_parquet_map()
    parquet_file = parquet_map[estab]

    if parquet_file.exists():
        log.info(f"[SKIP] {estab} já processado")

    log.info(f"[START] Extraindo {estab}")

    with get_elipse_conn(estab) as conn:  # pyright: ignore[reportGeneralTypeIssues]
        params = (get_estab_code(estab),)
        sql = read_sql_file(extraction_sql, __file__)
        df = pd.read_sql_query(sql, conn, params=params)

    # evita arquivo corrompido em retry
    temp_file = parquet_file.with_suffix(".tmp")
    df.to_parquet(temp_file)
    temp_file.rename(parquet_file)

    log.info(f"[DONE] {estab} -> {len(df)} linhas")


@task(retries=2, retry_delay=timedelta(minutes=3))
def concat_df_and_load_temp():
    parquet_all = _get_parquet_all()

    if parquet_all.exists():
        log.info("[SKIP] parquet_all já existe")
        return

    log.info("[START] Concatenando dataframes")

    dfs = [pd.read_parquet(p) for p in _get_parquet_map().values()]
    df = pd.concat(dfs, ignore_index=True)

    log.info(f"[INFO] Total linhas: {len(df)}")

    hook = PostgresHook(postgres_conn_id="postgres_eng_server")

    df.to_sql(
        "temp_fabricas",
        hook.get_sqlalchemy_engine({"executemany_mode": "values"}),
        schema="silver",
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi",
    )

    df.to_parquet(parquet_all)

    log.info("[DONE] Carga finalizada")


@task()
def clear_cache():
    parquet_all = _get_parquet_all()
    parquet_map = _get_parquet_map()

    log.info("[CLEANUP] Limpando arquivos")

    for f in [parquet_all, *parquet_map.values()]:
        f.unlink(missing_ok=True)


@dag(
    dag_id="SILVER_D_FABRICAS_SIMON",
    default_args=default_args,
    schedule="10 9 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
)
def dag_factory():
    sob_data = extract_data.override(task_id="extract_data_sob")("sob")
    for_data = extract_data.override(task_id="extract_data_for")("for")
    cra_data = extract_data.override(task_id="extract_data_cra")("cra")

    concat = concat_df_and_load_temp()

    merge = SQLExecuteQueryOperator(
        task_id="merge_table_and_drop_temp",
        conn_id="postgres_eng_server",
        sql=read_sql_file("merge_query.sql", __file__),
    )

    clear = clear_cache()

    # dependencies
    _ = [sob_data, for_data, cra_data] >> concat >> merge >> clear


dag_factory()
