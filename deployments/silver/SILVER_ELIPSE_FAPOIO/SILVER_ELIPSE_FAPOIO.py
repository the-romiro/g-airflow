import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from global_modules.operators import SqlServerOperator

DAG_ID = "SILVER_ELIPSE_FAPOIO"

tz = pendulum.timezone("America/Fortaleza")

start_date = pendulum.datetime(2025, 1, 26, 6, 5, tz=tz)

default_args = {
    "owner": "yan arcanjo",
    "start_date": start_date,
}

gold_dataset = Dataset("elipse://gold/fapoio")

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule="20 9,18 * * *",
    catchup=False,
    tags=["elipse", "apoio", "silver"],
    max_active_runs=1,
) as dag:
    truncate_stage = SQLExecuteQueryOperator(
        task_id="truncate_stage",
        conn_id="postgres_eng_server",
        sql="sql_files/truncate_stage.sql",
    )

    extract_query_file = "sql_files/extract_query.sql"
    target_table = "elipse.stage.stage_apoio"

    with TaskGroup("extract_all") as extraction:
        extract_sob = SqlServerOperator(
            task_id="extract_sob",
            source_conn_id="elipse_sob",
            sql_path=extract_query_file,
            estab=20,
            filename="apoio_sob.csv",
            target_conn_id="postgres_eng_server",
            target_table=target_table,
        )
        extract_cra = SqlServerOperator(
            task_id="extract_cra",
            source_conn_id="elipse_cra",
            sql_path=extract_query_file,
            estab=40,
            filename="apoio_cra.csv",
            target_conn_id="postgres_eng_server",
            target_table=target_table,
        )
        extract_for = SqlServerOperator(
            task_id="extract_for",
            source_conn_id="elipse_for",
            sql_path=extract_query_file,
            estab=21,
            filename="apoio_for.csv",
            target_conn_id="postgres_eng_server",
            target_table=target_table,
        )

    insert_stage_silver = SQLExecuteQueryOperator(
        task_id="insert_stage_silver",
        conn_id="postgres_eng_server",
        sql="sql_files/insert_stage_silver_fapoio.sql",
        outlets=[gold_dataset],
    )

    _ = truncate_stage >> extraction >> insert_stage_silver
