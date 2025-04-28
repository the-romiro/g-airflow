import json
import os
from datetime import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Conexão com o Teams
TEAMS_WEBHOOK_URL = Variable.get("WEBHOOK_TEAMS")

def send_teams_message(message: str, webhook_url: str):
    """
    Envia uma mensagem para um canal do Microsoft Teams usando o Webhook.

    :param message: A mensagem a ser enviada.
    :param webhook_url: A URL do webhook do Microsoft Teams.
    """
    headers = {"Content-Type": "application/json"}
    payload = {
        "text": message
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        raise ValueError(f"Failed to send message: {response.status_code}, {response.text}")
    
# Função para enviar a mensagem
def notify_teams_on_failure(context):
    message = f"""
    Ocurred an error in the following data pipeline:
    Dag_id:{context['dag'].dag_id}
    Run_id:{context['dag_run'].run_id}
    task_id = {context.get('task_instance').task_id}
    Status: Failure
    Event_date:{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
    """
    send_teams_message(message, TEAMS_WEBHOOK_URL)


default_args = {
    'owner': 'yan arcanjo',
    'start_date': datetime(2025, 1, 26, 6, 5),
    'on_failure_callback': notify_teams_on_failure
}

with DAG(
    'GOLD_F_QUALIDADE_SIMON',
    default_args=default_args, 
    schedule=None,
    catchup=False,
    max_active_runs=1
) as dag:
    transform_data = PostgresOperator(
        task_id="transform_silver_into_gold",
        postgres_conn_id = "postgres_eng_server",
        sql="./sql_files/transform_qualidade.sql"
    )
    vacuum_task = PostgresOperator(
    task_id='vacuum_task',
    sql="VACUUM elipse.gold.oee_fqualidade;",
    postgres_conn_id="postgres_eng_server",  # Certifique-se de que você tenha a conexão configurada no Airflow
    autocommit=True  # Isso desabilita a transação para permitir o VACUUM
    )
    analyze_task = PostgresOperator(
    task_id='analyze_task',
    sql="ANALYZE elipse.gold.oee_fqualidade;",
    postgres_conn_id="postgres_eng_server",
    autocommit=True
    )

vacuum_task >> analyze_task >> transform_data