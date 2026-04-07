from datetime import datetime, timedelta
from pathlib import Path

import connectorx as cx
import duckdb as ddb
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from global_modules.database import Estabelecimento, get_cx_conn, get_duckdb_conn, get_estab_code
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import DUCKDB_THREADS, get_parquet_file, read_sql_file

log = LoggingMixin().log

gold_dataset = Dataset("elipse://gold/fapoio")

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
STAGE_TABLE = "stage.stage_apoio"


def _get_parquet_map() -> dict[Estabelecimento, Path]:
    return {
        "sob": get_parquet_file("sob", __file__),
        "for": get_parquet_file("for", __file__),
        "cra": get_parquet_file("cra", __file__),
    }


@task(retries=3, retry_delay=timedelta(minutes=2))
def extract_data(estab: Estabelecimento):
    parquet_file = _get_parquet_map()[estab]

    if parquet_file.exists():
        log.info(f"[SKIP] {estab} já processado")
        return

    log.info(f"[START] Extraindo {estab}")

    sql = read_sql_file(extraction_sql, __file__)
    estab_code = get_estab_code(estab)
    sql_with_params = sql.format(estab=estab_code)

    df = cx.read_sql(get_cx_conn(estab), sql_with_params, return_type="arrow")

    log.info(f"[DONE QUERY] recuperados {df.shape[0]} linhas, {(df.nbytes / (1024**2)):.2f}MB")

    temp_file = parquet_file.with_suffix(".tmp")

    with ddb.connect(
        config={
            "threads": DUCKDB_THREADS,
            "preserve_insertion_order": False,
            "memory_limit": "2GB",
        }
    ) as con:
        con.register("df", df)
        con.execute(f"COPY (SELECT * FROM df) TO '{temp_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    del df

    temp_file.rename(parquet_file)

    log.info(f"[DONE] {estab} -> parquet gravado")


@task(retries=2, retry_delay=timedelta(minutes=1))
def concat_and_load_stage():
    parquet_map = _get_parquet_map()
    parquet_paths = list(parquet_map.values())
    paths_glob = ", ".join(f"'{p}'" for p in parquet_paths)
    pg_con_str = get_duckdb_conn("pg")

    with ddb.connect(
        config={
            "threads": DUCKDB_THREADS,
            "preserve_insertion_order": False,
            "memory_limit": "2GB",
        }
    ) as con:
        con.execute(f"ATTACH '{pg_con_str}' AS pg (TYPE POSTGRES);")

        already_loaded = con.sql(f"SELECT COUNT(1) FROM pg.{STAGE_TABLE}").fetchone()

        if already_loaded and already_loaded[0] > 0:
            log.info(f"[SKIP] '{STAGE_TABLE}' já carregada")
            return

        size_mb = con.sql(
            f"SELECT SUM(total_uncompressed_size) / (1024*1024.0) FROM parquet_metadata([{paths_glob}])"
        ).fetchone() or (0,)

        log.info(f"[START] Carregando parquets ({size_mb[0]:.2f}MB)")

        total_rows = con.execute(
            f"INSERT INTO pg.{STAGE_TABLE} SELECT * FROM read_parquet([{paths_glob}], union_by_name=true)"
        ).fetchone()

        log.info(f"[DONE] Total linhas inseridas: {total_rows}")


@task
def clear_cache():
    log.info("[CLEANUP] Limpando arquivos")
    for parquet in _get_parquet_map().values():
        parquet.unlink(missing_ok=True)


@dag(
    dag_id="SILVER_ELIPSE_FAPOIO",
    default_args=default_args,
    schedule="20 9,18 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    tags=["elipse", "apoio", "silver"],
)
def dag_factory():
    sob_data = extract_data.override(task_id="extract_data_sob")("sob")
    for_data = extract_data.override(task_id="extract_data_for")("for")
    cra_data = extract_data.override(task_id="extract_data_cra")("cra")

    concat = concat_and_load_stage()

    insert_silver = SQLExecuteQueryOperator(
        task_id="insert_stage_silver",
        conn_id="postgres_eng_server",
        sql=read_sql_file("insert_stage_silver_fapoio.sql", __file__),
        outlets=[gold_dataset],
    )

    truncate = SQLExecuteQueryOperator(
        task_id="truncate_stage",
        conn_id="postgres_eng_server",
        sql=read_sql_file("truncate_stage.sql", __file__),
    )

    clear = clear_cache()

    _ = [sob_data, for_data, cra_data] >> concat >> insert_silver >> truncate >> clear


dag_factory()
