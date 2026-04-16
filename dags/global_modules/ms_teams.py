import json
from datetime import datetime

import requests
from airflow.models import Variable

# Conexão com o Teams
TEAMS_WEBHOOK_URL = Variable.get("WEBHOOK_TEAMS", None)


def send_teams_message(message: str):
    """
    Envia uma mensagem para um canal do Microsoft Teams usando o Webhook.

    :param message: A mensagem a ser enviada.
    :param webhook_url: A URL do webhook do Microsoft Teams.
    """
    if TEAMS_WEBHOOK_URL is None:
        raise ValueError("Variável de ambiente 'TEAMS_WEBHOOK_URL' não definida.")

    headers = {"Content-Type": "application/json"}
    payload = {"text": message}
    response = requests.post(TEAMS_WEBHOOK_URL, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        raise ValueError(f"Failed to send message: {response.status_code}, {response.text}")


# Função para enviar a mensagem
def notify_teams_on_failure(context):
    message = f"""
    🚨 Pipeline failure:\n\n
    Dag_id: {context["dag"].dag_id}
    Run_id: {context["dag_run"].run_id}
    task_id: {context.get("task_instance").task_id}
    Status: ❌ Failure
    Event_date: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
    """
    send_teams_message(message)
