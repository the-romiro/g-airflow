from datetime import date, datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from global_modules.ms_teams import notify_teams_on_failure, send_teams_message

MEL_DATA_FAP_DATASET = Dataset("melhorias://silver/data_fap")

SECRET_EXPIRY_MONTHS = 6
NOTIFY_DAYS_THRESHOLD = 14

default_args = {
    "owner": "Antonio Albuquerque",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="NOTIFY_SHAREPOINT_SECRET_EXPIRY",
    default_args=default_args,
    schedule=[MEL_DATA_FAP_DATASET],
    catchup=False,
    max_active_runs=1,
    tags=["melhorias", "notify"],
)
def dag_factory():
    @task
    def check_and_notify() -> None:
        generated_str = Variable.get("SHAREPOINT_CLIENT_GENERATED")
        generated = datetime.strptime(generated_str, "%Y-%m-%d").date()
        expiry = generated + relativedelta(months=SECRET_EXPIRY_MONTHS)
        days_remaining = (expiry - date.today()).days

        if days_remaining <= NOTIFY_DAYS_THRESHOLD:
            send_teams_message(
                f"Atenção: SHAREPOINT_CLIENT_SECRET expira em {days_remaining} dia(s) "
                f"({expiry.strftime('%d/%m/%Y')}). "
                f"Renove a secret no Azure AD e atualize a variável "
                "SHAREPOINT_CLIENT_GENERATED no Airflow."
            )

    check_and_notify()


dag_factory()
