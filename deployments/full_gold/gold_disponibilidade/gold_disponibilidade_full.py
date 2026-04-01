from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from global_modules.ms_teams import notify_teams_on_failure

# Ambiente
server = "prod"
DB = "elipsedev" if server == "dev" else "elipse"
DW_CONN = "postgres_eng_server_dev" if server == "dev" else "postgres_eng_server"

# Adicionar o range da carga
data_inicio = "2024-10-01 05:24:00"
data_fim = "2025-01-01 06:00:00"

# Nome da conexão definida no Airflow Connections
connection_id_sob = "elipse_sob"
connection_id_for = "elipse_for"
connection_id_cra = "elipse_cra"


default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
}

gold_dataset = Dataset("elipse://gold/f_disponibilidade_simon_full")

with DAG(
    "GOLD_F_DISPONIBILIDADE_SIMON_FULL",
    default_args=default_args,
    schedule=[gold_dataset],
    catchup=False,
    max_active_runs=1,
) as dag:
    transform_data = SQLExecuteQueryOperator(
        task_id="transform_silver_into_gold",
        conn_id=DW_CONN,
        sql="./sql_files/transform_query_full.sql",
        params={"hora_inicio": data_inicio, "hora_fim": data_fim, "database": DB},
    )
    vacuum_task = SQLExecuteQueryOperator(
        task_id="vacuum_task",
        sql=f"VACUUM {DB}.gold.oee_fparadas;",
        conn_id=DW_CONN,
        autocommit=True,  # Isso desabilita a transação para permitir o VACUUM
    )
    analyze_task = SQLExecuteQueryOperator(
        task_id="analyze_task",
        sql=f"ANALYZE {DB}.gold.oee_fparadas;",
        conn_id=DW_CONN,
        autocommit=True,
    )

_ = vacuum_task >> analyze_task >> transform_data
