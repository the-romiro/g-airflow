from typing import Literal

from airflow.models import Variable

type EnvOptions = Literal[
    "SHAREPOINT_SITE_URL",
    "SHAREPOINT_CLIENT_ID",
    "SHAREPOINT_CLIENT_SECRET",
    "SHAREPOINT_LIST_NAME",
    "SHAREPOINT_TENANT_ID",
    "ENG_DATABASE_URL",
    "SHAREPOINT_USERNAME",
    "SHAREPOINT_USER_PASSWORD",
    "SHAREPOINT_SEARCH_DAYS",
]


def env(env_name: EnvOptions) -> str:
    value = Variable.get(env_name, None)

    if value is None:
        raise ValueError(f"Variável do Airflow '{env_name}' não definida.")

    return value
