import os
from datetime import datetime, timedelta
from pathlib import Path

import connectorx as cx
import duckdb as ddb
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from global_modules.database import Estabelecimento, get_cx_conn, get_duckdb_conn, get_estab_code
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import get_parquet_file, read_sql_file

log = LoggingMixin().log

default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
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

    sql = read_sql_file(extraction_sql, __file__)
    estab_code = get_estab_code(estab)

    sql_with_params = sql.format(estab=estab_code)

    df = cx.read_sql(
        get_cx_conn(estab),
        sql_with_params,
        return_type="arrow",
    )

    log.info(f"[DONE QUERY] recuperados {df.shape}, {( df.nbytes / (1024**2) ):.2}MB")

    # evita arquivo corrompido em retry
    temp_file = parquet_file.with_suffix(".tmp")

    with ddb.connect() as con:
        con.register('df', df)

        con.execute(f"COPY (SELECT * FROM df) TO '{temp_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    temp_file.rename(parquet_file)

    log.info(f"[DONE] {estab} -> {len(df)} linhas")


@task(retries=2, retry_delay=timedelta(minutes=1))
def concat_df_and_load_temp():
    parquet_map = _get_parquet_map()

    if not parquet_map:
        log.warning("[SKIP] Nenhum parquet encontrado em parquet_map")
        return

    parquet_paths = [str(p) for p in parquet_map.values()]
    paths_glob = ", ".join(f"'{p}'" for p in parquet_paths)
    pg_con_str = get_duckdb_conn("pg")
    tmp_table = 'silver.temp_fabricas'

    with ddb.connect(
        config={
            "threads": (os.cpu_count() or 2) // 2,
            "preserve_insertion_order": False,
            "memory_limit": "2GB",
        }
    ) as con:
        # con.execute("INSTALL postgres; LOAD postgres;") # já faz auto loading
        con.execute(f"ATTACH '{pg_con_str}' AS pg (TYPE POSTGRES);")

        check_table_query = f"SELECT to_regclass('{tmp_table}') IS NOT NULL"

        tmp_table_exists = con.sql(
            f"SELECT * FROM postgres_query('pg', $${check_table_query}$$)"
        ).fetchone()

        if tmp_table_exists is not None and tmp_table_exists[0]:
            log.info(f"[SKIP] '{tmp_table}' já existe")
            return

        log.info("[START] Loading .parquet")

        con.execute(
            f"CREATE TEMP VIEW v_source AS SELECT * FROM read_parquet([{paths_glob}], union_by_name=true)"
        )

        # Cria tabela baseada no schema do parquet (sem dados)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE pg.{tmp_table} AS SELECT * FROM v_source LIMIT 0
        """
        )

        # Insert em batch
        result = con.execute(f"INSERT INTO pg.{tmp_table} SELECT * FROM v_source")

        total_rows = result.fetchone()

        log.info(f"[DONE COPY] Total linhas: {total_rows}")

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
    tags=["silver", "simon"],
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
