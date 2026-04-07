from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from sqlalchemy.types import TypeEngine

from deployments.silver.SILVER_F_MELHORIAS_APROVACOES.fields import (
    DATETIME_WITH_TIMEZONE_FIELDS,
    EXPAND_FIELDS,
    LIST_FIELDS,
)
from global_modules.database import exec_merge, get_eng_conn
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.sharepoint.logs import log_message
from global_modules.sharepoint.sharepoint import fetch_sharepoint_items

HERE = Path(__file__).parent

APROVACAO_FILE = Path(HERE, "aprovacao.parquet")
STORE_FINISHED_FILE = Path(HERE, "store.finished")
MERGE_FINISHED_FILE = Path(HERE, "merge.finished")


def insert_into_dbengenharia(df: pd.DataFrame, dtype: dict[str, TypeEngine]):
    table_name = "tmp_melhorias_aprovacao"
    df.to_sql(
        name=table_name,
        con=get_eng_conn(fast_executemany=True),
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
    df = fetch_sharepoint_items(
        list_fields=LIST_FIELDS,
        expand_fields=EXPAND_FIELDS,
        datetime_columns=DATETIME_WITH_TIMEZONE_FIELDS,
        retrieve=10_000,
    )
    df.to_parquet(APROVACAO_FILE)


@task
def store():
    if MERGE_FINISHED_FILE.exists():
        return

    df = pd.read_parquet(APROVACAO_FILE)
    insert_into_dbengenharia(df, LIST_FIELDS)
    MERGE_FINISHED_FILE.touch()


@task
def merge_data():
    if STORE_FINISHED_FILE.exists():
        return

    exec_merge("sp_merge_melhorias_aprovacao")

    STORE_FINISHED_FILE.touch()


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
    dag_id="SILVER_F_MELHORIA_APROVACAO",
    default_args=default_args,
    schedule="10 10 * * *",  # 07:10 para UTC-3
    catchup=False,
    max_active_runs=1,
    tags=["melhorias"],
) as dag:
    _ = extract_sharepoint() >> store() >> merge_data() >> delete_cache()
