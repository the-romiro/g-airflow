from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from global_modules.database import exec_query_eng_db, get_eng_conn
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.sharepoint.logs import log_message
from global_modules.sharepoint.sharepoint import fetch_sharepoint_items_with_graph_api
from global_modules.utils import read_sql_file
from melhorias.dags.BRONZE_MELHORIAS_GANHOS.fields import LIST_FIELDS
from sqlalchemy.types import TypeEngine

HERE = Path(__file__).parent

APROVACAO_FILE = Path(HERE, "ganhos.parquet")
STORE_FINISHED_FILE = Path(HERE, "store.finished")
MERGE_FINISHED_FILE = Path(HERE, "merge.finished")


def insert_into_dbengenharia(df: pd.DataFrame, dtype: dict[str, TypeEngine]):
    table_name = "stg_mel_ganhos"
    df.to_sql(
        name=table_name,
        con=get_eng_conn(),
        if_exists="replace",
        index=False,
        dtype=dtype,  # type: ignore
        chunksize=1_000,
    )
    log_message(f"✅ Dados inseridos com sucesso na {table_name}.")


@task(
    retries=1,
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=30),
)
def extract_sharepoint():
    if APROVACAO_FILE.exists():
        return
    df = fetch_sharepoint_items_with_graph_api(
        list_fields=LIST_FIELDS,
        site_url="https://grendenecombr.sharepoint.com/sites/dados_industriais/sis_melhorias",
        list_name="Melhorias",
        start_date=datetime.now(timezone.utc) - timedelta(days=30),
    )
    df.to_parquet(APROVACAO_FILE)


@task
def store():
    if STORE_FINISHED_FILE.exists():
        return

    df = pd.read_parquet(APROVACAO_FILE)
    insert_into_dbengenharia(df, LIST_FIELDS)
    STORE_FINISHED_FILE.touch()


@task
def merge_data():
    if MERGE_FINISHED_FILE.exists():
        return

    # exec_query_eng_db(read_sql_file("merge_aprovacoes.sql", __file__))

    MERGE_FINISHED_FILE.touch()


@task
def delete_cache():
    APROVACAO_FILE.unlink(missing_ok=True)
    STORE_FINISHED_FILE.unlink(missing_ok=True)
    MERGE_FINISHED_FILE.unlink(missing_ok=True)


default_args = {
    "owner": "Antonio Albuquerque",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    # "retries": 3,
    # "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="BRONZE_MELHORIA_GANHOS",
    default_args=default_args,
    schedule="10 10 * * *",  # 07:10 para UTC-3
    catchup=False,
    max_active_runs=1,
    tags=["melhorias"],
) as dag:
    _ = extract_sharepoint() >> store() >> merge_data() >> delete_cache()
