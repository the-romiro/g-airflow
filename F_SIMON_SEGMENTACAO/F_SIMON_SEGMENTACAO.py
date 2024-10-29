from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql     import MsSqlHook
from airflow.operators.python                          import PythonOperator
from datetime import datetime 
import pandas as pd
from sqlalchemy import text
import os

default_args = {
    'owner': 'yan arcanjo',
    'start_date': datetime(2024, 1, 1, 7, 0),
}

#reads the sql file and returns the query
def read_sql_file(file_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(dir, file_path)

    with open(sql_dir, 'r')  as file:
        query = file.read()
    return query

#Get the machine tables and returns a dataframe
def get_machine_registration_table(**kwargs):
    hook = MsSqlHook(mssql_conn_id='db_engenharia')
    df = pd.read_sql_query(read_sql_file('sql_files/DBO.SIM_EQUIPAMENTOS.SQL'), hook.get_sqlalchemy_engine())

    return df

def populate_fSegmentacao_simon(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='get_machine_table')
    hook = MsSqlHook(mssql_conn_id='db_engenharia')
    merge_query = read_sql_file('sql_files/insert_DBO.FSEGMENTACAO_SIMON.sql')

    with hook.get_sqlalchemy_engine().connect() as connection:
        for _, row in df.iterrows():
            params = {
                'data': kwargs['execution_date'].strftime('%Y-%m-%d'), 
                'equipamento': row['equipamento'], 
                'segmentacao': row['segmentacao'], 
                'updated_at': kwargs['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
            }
            
            connection.execute(text(merge_query), params)

        connection.commit()

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
    task2 = PythonOperator(
        task_id='populate_table_fSegmentacao_simon',
        python_callable=populate_fSegmentacao_simon
    )

    task >> task2


