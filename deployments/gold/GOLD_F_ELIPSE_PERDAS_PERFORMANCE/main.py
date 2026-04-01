from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from global_modules.ms_teams import notify_teams_on_failure

default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
}

gold_dataset = Dataset("elipse://gold/f_performance_simon")

with DAG(
    "GOLD_F_PERFORMANCE_SIMON",
    default_args=default_args,
    schedule=[gold_dataset],
    catchup=False,
    max_active_runs=1,
) as dag:
    transform_data = SQLExecuteQueryOperator(
        task_id="transform_silver_into_gold",
        conn_id="postgres_eng_server",
        sql="./sql_files/transform_performance.sql",
    )
    vacuum_task = SQLExecuteQueryOperator(
        task_id="vacuum_task",
        sql="VACUUM elipse.gold.oee_fperformance;",
        conn_id="postgres_eng_server",
        autocommit=True,  # Isso desabilita a transação para permitir o VACUUM
    )
    analyze_task = SQLExecuteQueryOperator(
        task_id="analyze_task",
        sql="ANALYZE elipse.gold.oee_fperformance;",
        conn_id="postgres_eng_server",
        autocommit=True,
    )

_ = vacuum_task >> analyze_task >> transform_data
