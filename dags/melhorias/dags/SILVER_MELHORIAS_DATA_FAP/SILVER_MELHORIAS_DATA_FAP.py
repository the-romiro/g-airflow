from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from global_modules.database import exec_query_eng_db
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import read_sql_file

log = LoggingMixin().log

MEL_APROVACAO_DATASET = Dataset("melhorias://bronze/melhoria_aprovacao")
MEL_GANHOS_DATASET = Dataset("melhorias://bronze/melhoria_ganhos")


@task
def update_dt_fap() -> None:
    log.info("[START] Atualizando dt_fap nas melhorias aprovadas")
    exec_query_eng_db(read_sql_file("update_fap.sql", __file__))
    log.info("[DONE] dt_fap atualizado")


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
    dag_id="BRONZE_MELHORIA_DATA_FAP",
    default_args=default_args,
    schedule=[MEL_APROVACAO_DATASET, MEL_GANHOS_DATASET],
    catchup=False,
    max_active_runs=1,
    tags=["melhorias", "bronze"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    _ = start >> update_dt_fap() >> end
