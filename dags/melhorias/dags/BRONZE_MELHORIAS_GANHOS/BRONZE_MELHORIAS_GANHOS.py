from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
from airflow import DAG
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from global_modules.database import exec_query_eng_db, get_duckdb_conn
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.sharepoint.sharepoint import fetch_sharepoint_items_with_graph_api
from global_modules.utils import read_sql_file
from melhorias.dags.BRONZE_MELHORIAS_GANHOS.fields import LIST_FIELDS

log = LoggingMixin().log

HERE = Path(__file__).parent

GANHOS_FILE = Path(HERE, "ganhos.parquet")
STORE_FINISHED_FILE = Path(HERE, "store.finished")
MERGE_FINISHED_FILE = Path(HERE, "merge.finished")


@task(
    retries=1,
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=30),
)
def extract_sharepoint():
    if GANHOS_FILE.exists():
        log.info("[SKIP] Parquet de ganhos já existe, pulando extração")
        return

    log.info("[START] Buscando itens de Melhorias no SharePoint")

    df = fetch_sharepoint_items_with_graph_api(
        list_fields=LIST_FIELDS,
        site_url="https://grendenecombr.sharepoint.com/sites/dados_industriais/sis_melhorias",
        list_name="Melhorias",
        start_date=datetime.now(timezone.utc) - timedelta(days=30),
    )

    log.info(f"[INFO] {len(df)} registros extraídos do SharePoint")

    tmp = GANHOS_FILE.with_suffix(".tmp")
    df.to_parquet(tmp)
    tmp.rename(GANHOS_FILE)

    log.info(f"[DONE] Parquet gravado → {GANHOS_FILE.name}")


@task
def store():
    if STORE_FINISHED_FILE.exists():
        log.info("[SKIP] store já concluído")
        return

    if not GANHOS_FILE.exists():
        log.warning("[WARN] Parquet não encontrado, pulando store")
        return

    log.info("[START] Carregando parquet para stg_mel_ganhos")

    conn_str = get_duckdb_conn("dbengenharia")

    with duckdb.connect() as con:
        con.execute("INSTALL mssql FROM community;")
        con.execute("LOAD mssql;")
        con.execute(f"ATTACH '{conn_str}' AS db (TYPE mssql);")
        con.execute(
            (
                f"COPY (SELECT * FROM read_parquet('{GANHOS_FILE}')) "
                "TO 'db.dbo.stg_mel_ganhos' (FORMAT 'bcp', REPLACE true);"
            )
        )

    log.info("[DONE] Dados inseridos em stg_mel_ganhos")
    STORE_FINISHED_FILE.touch()


@task
def merge_data():
    if MERGE_FINISHED_FILE.exists():
        log.info("[SKIP] merge_data já concluído")
        return

    log.info("[START] Executando merge_ganhos.sql")

    exec_query_eng_db(read_sql_file("merge_ganhos.sql", __file__))

    MERGE_FINISHED_FILE.touch()
    log.info("[DONE] Merge concluído")


@task
def delete_cache():
    log.info("[CLEANUP] Removendo parquet e sentinelas")

    GANHOS_FILE.unlink(missing_ok=True)
    STORE_FINISHED_FILE.unlink(missing_ok=True)
    MERGE_FINISHED_FILE.unlink(missing_ok=True)

    log.info("[DONE] Cache limpo")


default_args = {
    "owner": "Antonio Albuquerque",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="BRONZE_MELHORIA_GANHOS",
    default_args=default_args,
    schedule="10 10 * * *",  # 07:10 para UTC-3
    catchup=False,
    max_active_runs=1,
    tags=["melhorias", "bronze"],
) as dag:
    _ = extract_sharepoint() >> store() >> merge_data() >> delete_cache()
