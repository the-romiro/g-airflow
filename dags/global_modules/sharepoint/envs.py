from os import getenv
from typing import Literal

type EnvOptions = Literal[
    "SHAREPOINT_SITE_URL",
    "SHAREPOINT_USERNAME",
    "SHAREPOINT_USER_PASSWORD",
    "SHAREPOINT_LIST_NAME",
    "ENG_DATABASE_URL",
]


def env(env_name: EnvOptions):
    value = getenv(env_name, None)

    if value is None:
        raise ValueError(f"Variável de ambiente '{env_name}' não definida.")

    return value
