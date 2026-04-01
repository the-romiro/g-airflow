import os
from datetime import datetime, timedelta, timezone
from typing import Literal

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine

from global_modules.ms_teams import notify_teams_on_failure

# Obter data e hora atual
data_hora_atual = (datetime.now()).strftime("%Y%m%d%H%M%S")

HERE = os.path.dirname(os.path.abspath(__file__))
PARQUET_FOLDER = os.path.join(HERE, "parquet_files")

NUM_FILIAL = {"elipse_sob": 20, "elipse_for": 21, "elipse_cra": 40}

elipse_conn = Literal["elipse_sob", "elipse_for", "elipse_cra"]


# reads the sql file and returns the query
def read_sql_file(file_path: str):
    _dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(_dir, f"sql_files/{file_path}")

    with open(sql_dir, "r") as file:
        query = file.read()
    return query


default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
}


def get_url(filial: elipse_conn):
    conn = BaseHook.get_connection(filial)
    url = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    return url


def get_conn(url):
    return create_engine(
        url,
        pool_pre_ping=True,  # checa se a conexão está viva antes de usar
        pool_recycle=1800,  # recicla conexões inativas após 30 min
        pool_timeout=30,  # espera até 30s por uma conexão
    )


def get_sql_ciclos(filial: elipse_conn):
    url = get_url(filial)

    params = (NUM_FILIAL[filial],)  # id_estabelecimento
    conn = get_conn(url)

    print(f"Pegando SQL de {params}")

    df = pd.read_sql_query(read_sql_file("extract_ciclos.sql"), conn, params=params)

    print(f"SQL OK {df.shape}")

    return str(df.iloc[0, 0])


def extract_data_sob(parquet_name, sql):
    full_path = os.path.join(PARQUET_FOLDER, parquet_name)

    if os.path.exists(full_path):
        return False

    url = get_url("elipse_sob")

    conn = get_conn(url)
    df = pd.read_sql_query(sql, conn)

    print(parquet_name, df.shape)

    df.to_parquet(full_path)

    return True


def extract_data_for(parquet_name, sql):
    full_path = os.path.join(PARQUET_FOLDER, parquet_name)

    if os.path.exists(full_path):
        return False

    url = get_url("elipse_for")

    conn = get_conn(url)
    df = pd.read_sql_query(sql, conn)

    print(parquet_name, df.shape)

    df.to_parquet(full_path)

    return True


def extract_data_cra(parquet_name, sql):
    full_path = os.path.join(PARQUET_FOLDER, parquet_name)

    if os.path.exists(full_path):
        return False

    url = get_url("elipse_cra")

    conn = get_conn(url)
    df = pd.read_sql_query(sql, conn)

    print(parquet_name, df.shape)

    df.to_parquet(full_path)

    return True


def concat_data_and_load_temp(
    if_exists: Literal["replace", "append"],
    parquet_files,
    **kwargs,
):
    # ti = kwargs["ti"]
    # df_sob = ti.xcom_pull(task_ids=f"extract_all_{suffix}.extract_data_sob_{suffix}")
    # df_cra = ti.xcom_pull(task_ids=f"extract_all_{suffix}.extract_data_cra_{suffix}")
    # df_for = ti.xcom_pull(task_ids=f"extract_all_{suffix}.extract_data_for_{suffix}")

    df = pd.concat(
        [pd.read_parquet(os.path.join(PARQUET_FOLDER, p)) for p in parquet_files],
        ignore_index=True,
    )

    # df.to_parquet(
    #     f"/datalake/bronze/bronze_elipse_paradas/bronze_elipse_paradas{data_hora_atual}.parquet", index=False
    # )

    # df.drop(columns=['linha'])

    # df = df.sort_values("E3TimeStamp").drop_duplicates(subset=["Id", "id_estabelecimento"], keep="last")

    # hook = PostgresHook(postgres_conn_id="postgres_eng_server")

    # Antonio 19/06/2025, retirei 'PostgresHook' porque não estava resolvendo o ip do postgres.
    pg_conn = get_conn(Variable.get("PG_CONN"))

    # if_exists = "replace"
    # for f in parquet_files:
    # df = pd.read_parquet(os.path.join(PARQUET_FOLDER, f))

    print("concat_data_and_load_temp:", df.shape)

    df.to_sql(
        "temp_ciclos",
        pg_conn,
        schema="silver",
        chunksize=50_000,
        if_exists=if_exists,
        index=False,
    )

    # if_exists = "append"

    print("finalizado.")


@task
def delete_pq_records(dt_delete):
    pg_conn = get_conn(Variable.get("PG_CONN"))

    df_last_date = pd.read_sql(read_sql_file("ultimo_dia.sql"), pg_conn)

    last_date_fim = df_last_date.iloc[0, 1]

    sql_delete_first_day = (
        f"DELETE FROM silver.temp_ciclos WHERE \"E3TimeStamp\" < '{last_date_fim}'"
    )
    sql_delete_last_days = f"DELETE FROM silver.temp_ciclos WHERE \"E3TimeStamp\" >= '{dt_delete}'"

    print(sql_delete_first_day)

    df = pd.read_sql(sql_delete_first_day, pg_conn)
    print("first day", df)

    df = pd.read_sql(sql_delete_last_days, pg_conn)
    print("last days", df)

    # df = pd.read_sql("REINDEX TABLE temp_ciclos;", pg_conn)
    # print("REINDEX", df)


@task
def delete_parquet_files(dt_delete):
    ...
    # for f in Path(PARQUET_FOLDER).iterdir():


with DAG(
    "SILVER_F_CICLOS_SIMON",
    default_args=default_args,
    schedule="20 9,18 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["elipse", "ciclos", "silver"],
) as dag:
    dt = datetime.now(timezone.utc) + timedelta(hours=3)

    days = 7
    step = 1  # math.ceil(days / 31)

    previous_task = None

    # sql_sob = get_sql_ciclos("elipse_sob")
    # sql_for = get_sql_ciclos("elipse_for")
    # sql_cra = get_sql_ciclos("elipse_cra")

    for day_ini in range(days, 0, -step):
        dt_inicio = (dt - timedelta(days=day_ini)).strftime("%Y-%m-%d")
        dt_fim = (dt - timedelta(days=day_ini - step)).strftime("%Y-%m-%d")

        is_first = previous_task is None

        with TaskGroup(f"extract_all_{dt_inicio}") as extraction:
            sob_parquet_name = f"extract_data_sob_{dt_inicio}"
            # extract_sob = PythonOperator(
            #     task_id=sob_parquet_name,
            #     python_callable=extract_data_sob,
            #     op_kwargs={
            #         "sql": sql_sob.format(dt_inicio=dt_inicio, dt_fim=dt_fim),
            #         "parquet_name": f"{sob_parquet_name}.parquet",
            #     },
            # )

            for_parquet_name = f"extract_data_for_{dt_inicio}"
            # extract_for = PythonOperator(
            #     task_id=for_parquet_name,
            #     python_callable=extract_data_for,
            #     op_kwargs={
            #         "sql": sql_for.format(dt_inicio=dt_inicio, dt_fim=dt_fim),
            #         "parquet_name": f"{for_parquet_name}.parquet",
            #     },
            # )

            cra_parquet_name = f"extract_data_cra_{dt_inicio}"
            # extract_cra = PythonOperator(
            #     task_id=cra_parquet_name,
            #     python_callable=extract_data_cra,
            #     op_kwargs={
            #         "sql": sql_cra.format(dt_inicio=dt_inicio, dt_fim=dt_fim),
            #         "parquet_name": f"{cra_parquet_name}.parquet",
            #     },
            # )

        parquet_files = (
            f"{sob_parquet_name}.parquet",
            f"{for_parquet_name}.parquet",
            f"{cra_parquet_name}.parquet",
        )

        concat_and_load = PythonOperator(
            task_id=f"concat_data_and_load_temp_{day_ini}",
            python_callable=concat_data_and_load_temp,
            op_kwargs={
                # "if_exists": "replace" if is_first else "append",
                "if_exists": "append",
                "parquet_files": parquet_files,
            },
        )

        # merge_data = PostgresOperator(
        #     task_id="merge_table_and_drop_temp",
        #     postgres_conn_id="postgres_eng_server",
        #     sql="./sql_files/merge_query.sql",
        #     autocommit=True
        # )
        # vacuum_task = PostgresOperator(
        #     task_id="vacuum_task",
        #     sql="VACUUM elipse.silver.oee_fparadas;",
        #     postgres_conn_id="postgres_eng_server",  # Certifique-se de que você tenha a conexão configurada no Airflow
        #     autocommit=True,  # Isso desabilita a transação para permitir o VACUUM
        # )
        # analyze_task = PostgresOperator(
        #     task_id="analyze_task",
        #     sql="ANALYZE elipse.silver.oee_fparadas;",
        #     postgres_conn_id="postgres_eng_server",
        #     autocommit=True,
        # )  # Usando TriggerDagRunOperator para acionar a DAG2 após a execução de DAG1
        # trigger_dag = TriggerDagRunOperator(
        #     task_id="trigger_gold_dag",
        #     trigger_dag_id="GOLD_F_DISPONIBILIDADE_SIMON",  # Nome da DAG a ser acionada
        # )

        if is_first:
            # >> EmptyOperator(task_id=f"t{day_ini}")  # >> concat_and_load
            # extraction >> EmptyOperator(task_id=f"t{day_ini}")
            concat_and_load >> EmptyOperator(task_id=f"t{day_ini}")

        # Se houver uma task anterior, conecta ela à task atual
        if previous_task:
            # previous_task >> extraction
            previous_task >> concat_and_load

        # previous_task = extraction
        previous_task = concat_and_load

    # >> vacuum_task
    # >> analyze_task
    # >> merge_data
    # >> trigger_dag
