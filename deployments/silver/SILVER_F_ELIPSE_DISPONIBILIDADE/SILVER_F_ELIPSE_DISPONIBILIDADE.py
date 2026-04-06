import os
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
from global_modules.utils import get_parquet_file, read_sql_file

log = LoggingMixin().log

DATABASE_ID = os.environ.get("DATABASE_PROD")
DW_CONN = "postgres_eng_server"

STAGE_TABLE = "stage.stage_paradas"
extraction_sql = "extract_paradas.sql"

gold_dataset = Dataset("elipse://gold/f_disponibilidade_simon")

default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


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

    log.info(f"[DONE QUERY] recuperados {df.shape}, {(df.nbytes / (1024**2)):.2f}MB")

    temp_file = parquet_file.with_suffix(".tmp")

    with ddb.connect() as con:
        con.register("df", df)
        con.execute(f"COPY (SELECT * FROM df) TO '{temp_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    temp_file.rename(parquet_file)

    log.info(f"[DONE] {estab} -> {df.shape[0]} linhas")

    del df


@task(retries=2, retry_delay=timedelta(minutes=1))
def concat_and_load_stage():
    parquet_map = _get_parquet_map()
    parquet_paths = list(parquet_map.values())
    paths_glob = ", ".join(f"'{p}'" for p in parquet_paths)

    pg_conn_str = get_duckdb_conn("pg")

    with ddb.connect(
        config={
            "threads": (os.cpu_count() or 2) // 2,
            "preserve_insertion_order": False,
            "memory_limit": "2GB",
        }
    ) as con:
        con.execute(f"ATTACH '{pg_conn_str}' AS pg (TYPE POSTGRES);")

        already_loaded = con.sql(
            f"SELECT COUNT(*) FROM postgres_query('pg', 'SELECT 1 FROM {STAGE_TABLE} LIMIT 1')"
        ).fetchone()

        if already_loaded and already_loaded[0] > 0:
            log.info(f"[SKIP] '{STAGE_TABLE}' já carregada")
            return

        size_mb = con.sql(
            f"SELECT SUM(total_uncompressed_size) / (1024*1024.0) FROM parquet_metadata([{paths_glob}])"
        ).fetchone() or (0,)

        log.info(f"[START] Carregando parquets ({size_mb[0]:.2f}MB)")

        con.execute(
            f"""
            CREATE TEMP VIEW v_source AS
            SELECT * FROM read_parquet([{paths_glob}], union_by_name=true)
            """
        )

        log.info("[CHECK] Removendo duplicadas por 'Id' e 'id_estabelecimento'.")

        # Evitar duplicidade dos dados.
        con.execute(
            """
            CREATE TEMP VIEW v_deduped AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY "Id", id_estabelecimento
                        ORDER BY "E3TimeStamp" DESC
                    ) AS rn
                FROM v_source
            )
            WHERE rn = 1
        """
        )

        log.info("[DUCKDB] Iniciando bulk insert.")
        total_rows = con.execute(
            f"INSERT INTO pg.{STAGE_TABLE} SELECT * FROM v_deduped"
        ).fetchone() or (0,)

        log.info(f"[DONE] Total linhas inseridas: {total_rows[0]}")


@task
def clear_cache():
    log.info("[CLEANUP] Limpando arquivos")
    for parquet in _get_parquet_map().values():
        parquet.unlink(missing_ok=True)


@dag(
    dag_id="SILVER_F_DISPONIBILIDADE_SIMON",
    default_args=default_args,
    schedule="30 9,18 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    tags=["elipse", "silver", "disponibilidade"],
)
def dag_factory():
    truncate_stage = SQLExecuteQueryOperator(
        task_id="truncate_stage",
        conn_id=DW_CONN,
        sql="sql_files/truncate_stage_paradas.sql",
        params={"source": STAGE_TABLE, "database_id": DATABASE_ID},
    )

    sob_data = extract_data.override(task_id="extract_data_sob")("sob")
    for_data = extract_data.override(task_id="extract_data_for")("for")
    cra_data = extract_data.override(task_id="extract_data_cra")("cra")

    concat_load = concat_and_load_stage()

    vacuum_task = SQLExecuteQueryOperator(
        task_id="vacuum_task",
        sql=f"VACUUM {DATABASE_ID}.silver.oee_fparadas;",
        conn_id=DW_CONN,
        autocommit=True,
    )

    analyze_task = SQLExecuteQueryOperator(
        task_id="analyze_task",
        sql=f"ANALYZE {DATABASE_ID}.silver.oee_fparadas;",
        conn_id=DW_CONN,
        autocommit=True,
    )

    merge_data = SQLExecuteQueryOperator(
        task_id="merge_stage_silver",
        conn_id=DW_CONN,
        sql="sql_files/merge_query.sql",
        params={
            "source": STAGE_TABLE,
            "target": "silver.oee_fparadas",
            "database_id": DATABASE_ID,
        },
        autocommit=True,
        outlets=[gold_dataset],
    )

    clear = clear_cache()

    _ = (
        truncate_stage
        >> [sob_data, for_data, cra_data]
        >> concat_load
        >> vacuum_task
        >> analyze_task
        >> merge_data
        >> clear
    )


dag_factory()
