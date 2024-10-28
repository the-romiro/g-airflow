from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime 

default_args = {
    'owner': 'yan arcanjo',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'example_postgres_dag', 
    default_args=default_args, 
    schedule_interval='@daily',
    catchup=False
) as dag:
    run_query = MsSqlOperator(
        task_id='run_sql',
        mssql_conn_id='db_engenharia',
        sql="SELECT 1;"
    )


    run_query