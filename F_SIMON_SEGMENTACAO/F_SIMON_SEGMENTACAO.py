from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python                          import PythonOperator
from datetime import datetime 
import pandas as pd

default_args = {
    'owner': 'yan arcanjo',
    'start_date': datetime(2024, 1, 1, 7, 0),
}

def get_machine_registration_table(**kwargs):
    hook = MsSqlOperator.get_db_hook('db_engenharia')

    query = 'SELECT 1 AS TESTE;'

    df = pd.read_sql_query(query, hook.get_conn())

    print(df)

with DAG(
    'F_SIMON_SEGMENTACAO',
    default_args=default_args, 
    schedule='0 7 * * *',
    catchup=False
) as dag:
    task = PythonOperator(
        'test_run',
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