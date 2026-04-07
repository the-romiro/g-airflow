from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import read_sql_file

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


@dag(
    dag_id="GOLD_F_DISPONIBILIDADE_SIMON",
    default_args=default_args,
    schedule=[gold_dataset],
    catchup=False,
    max_active_runs=1,
    tags=["elipse", "self_service", "disponibilidade", "gold"],
)
def dag_factory():
    vacuum = SQLExecuteQueryOperator(
        task_id="vacuum_task",
        sql="VACUUM elipse.gold.oee_fparadas;",
        conn_id="postgres_eng_server",
        autocommit=True,
    )

    analyze = SQLExecuteQueryOperator(
        task_id="analyze_task",
        sql="ANALYZE elipse.gold.oee_fparadas;",
        conn_id="postgres_eng_server",
        autocommit=True,
    )

    transform = SQLExecuteQueryOperator(
        task_id="transform_silver_into_gold",
        conn_id="postgres_eng_server",
        sql=read_sql_file("transform_query.sql", __file__),
    )

    _ = vacuum >> analyze >> transform


dag_factory()
