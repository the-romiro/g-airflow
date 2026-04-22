from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import read_sql_file

gold_dataset = Dataset("elipse://gold/fapoio")

default_args = {
    "owner": "yan_arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="GOLD_ELIPSE_FAPOIO",
    default_args=default_args,
    schedule=[gold_dataset],
    catchup=False,
    max_active_runs=1,
    tags=["elipse", "self_service", "gold"],
)
def dag_factory():
    transform = SQLExecuteQueryOperator(
        task_id="transform_silver_into_gold",
        conn_id="postgres_eng_server",
        sql=read_sql_file("insert_silver_gold_fapoio.sql", __file__),
    )

    vacuum_analyze = SQLExecuteQueryOperator(
        task_id="vacuum_analyze_task",
        sql="VACUUM ANALYZE elipse.gold.elipse_fapoio;",
        conn_id="postgres_eng_server",
        autocommit=True,
    )

    _ = transform >> vacuum_analyze


dag_factory()
