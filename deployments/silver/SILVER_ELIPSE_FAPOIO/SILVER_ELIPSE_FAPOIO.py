import json
import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from global_modules.functions import getLocalConfig
from global_modules.operators import SqlServerOperator

DAG_ID = "SILVER_ELIPSE_FAPOIO"

default_args = {"owner": "yan arcanjo", "start_date": datetime(2025, 1, 26, 6, 5)}

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule="20 9,18 * * *",
    catchup=False,
    tags=["elipse", "apoio", "silver"],
    max_active_runs=1,
) as dag:
    truncate_stage = PostgresOperator(
        task_id="truncate_stage",
        sql="sql_files/truncate_stage.sql",
        postgres_conn_id="postgres_eng_server",
        # params={'source': dw_truncate['target'], 'database_id': database_id}
    )

    with TaskGroup("extract_all") as extraction:
        extract_sob = SqlServerOperator(
            task_id="extract_sob",
            source_conn_id="elipse_sob",
            sql_path="sql_files/extract_query.sql",
            estab=20,
            filename="apoio_sob.csv",
            target_conn_id="postgres_eng_server",
            target_table="elipse.stage.stage_apoio",
        )
        extract_cra = SqlServerOperator(
            task_id="extract_cra",
            source_conn_id="elipse_cra",
            sql_path="sql_files/extract_query.sql",
            estab=40,
            filename="apoio_cra.csv",
            target_conn_id="postgres_eng_server",
            target_table="elipse.stage.stage_apoio",
        )
        extract_for = SqlServerOperator(
            task_id="extract_for",
            source_conn_id="elipse_for",
            sql_path="sql_files/extract_query.sql",
            estab=21,
            filename="apoio_for.csv",
            target_conn_id="postgres_eng_server",
            target_table="elipse.stage.stage_apoio",
        )

        [extract_sob, extract_for, extract_cra]

    insert_stage_silver = PostgresOperator(
        task_id="insert_stage_silver",
        sql="sql_files/insert_stage_silver_fapoio.sql",
        postgres_conn_id="postgres_eng_server",
        trigger_rule="all_done",
        # params={'source': dw_truncate['target'], 'database_id': database_id}
    )

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="GOLD_ELIPSE_FAPOIO",  # Nome da DAG a ser acionada
    )

    (truncate_stage >> extraction >> insert_stage_silver >> trigger_dag)
