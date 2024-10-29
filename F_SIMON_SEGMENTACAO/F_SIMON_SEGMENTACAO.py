from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql     import MsSqlHook
from airflow.operators.python                          import PythonOperator
from datetime import datetime 
import pandas as pd
import os


default_args = {
    'owner': 'yan arcanjo',
    'start_date': datetime(2024, 1, 1, 7, 0),
}

def read_sql_file(file_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(dir, file_path)

    with open(sql_dir, 'r')  as file:
        query = file.read()
    return query

def get_machine_registration_table(**kwargs):
    hook = MsSqlHook(mssql_conn_id='db_engenharia')


    df = pd.read_sql_query(read_sql_file('sql_files/DBO.SIM_EQUIPAMENTOS.SQL'), hook.get_sqlalchemy_engine())

    print(df)

with DAG(
    'F_SIMON_SEGMENTACAO',
    default_args=default_args, 
    schedule='0 7 * * *',
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='get_machine_table',
        python_callable=get_machine_registration_table
    )




# with DAG(
#     'example_postgres_dag', 
#     default_args=default_args, 
#     schedule='0 7 * * *',
#     catchup=False
# ) as dag:
#     run_query = MsSqlOperator(
#         task_id='run_sql',
#         mssql_conn_id='db_engenharia',
#         sql="SELECT 1;"
#     )

#     run_query