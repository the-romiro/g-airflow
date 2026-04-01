from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from global_modules.ms_teams import notify_teams_on_failure

DAG_ID = "GOLD_ELIPSE_FAPOIO"

tz = pendulum.timezone("America/Fortaleza")

start_date = pendulum.datetime(2025, 1, 26, 6, 5, tz=tz)

default_args = {
    "owner": "yan_arcanjo",
    "start_date": start_date,
    "on_failure_callback": notify_teams_on_failure,
    "retry_delay": timedelta(minutes=5),
    "retries": 1,
}

gold_dataset = Dataset("elipse://gold/fapoio")

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule=[gold_dataset],
    catchup=False,
    tags=["elipse", "self_service", "gold"],
    max_active_runs=1,
) as dag:
    transform_data = SQLExecuteQueryOperator(
        task_id="transform_silver_into_gold",
        conn_id="postgres_eng_server",
        sql="./sql_files/insert_silver_gold_fapoio.sql",
    )
    vacuum_task = SQLExecuteQueryOperator(
        task_id="vacuum_task",
        sql="VACUUM elipse.gold.elipse_fapoio;",
        conn_id="postgres_eng_server",
        autocommit=True,  # Isso desabilita a transação para permitir o VACUUM
    )
    analyze_task = SQLExecuteQueryOperator(
        task_id="analyze_task",
        sql="ANALYZE elipse.gold.elipse_fapoio;",
        conn_id="postgres_eng_server",
        autocommit=True,
    )

    _ = vacuum_task >> analyze_task >> transform_data
