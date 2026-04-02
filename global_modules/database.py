from typing import Literal

from airflow.models import Variable
from sqlalchemy import create_engine, text

from global_modules.sharepoint.logs import log_message

ENG_DATABASE_URL = Variable.get("ENG_DATABASE_URL", None)

type Estabelecimento = Literal["sob", "cra", "for"]

CONNECTIONS_VAR: dict[Estabelecimento, str] = {
    "sob": "elipse_sob",
    "for": "elipse_for",
    "cra": "elipse_cra",
}
ESTAB_CODE: dict[Estabelecimento, int] = {
    "sob": 20,
    "for": 21,
    "cra": 40,
}


def get_estab_code(estab: Estabelecimento):
    return ESTAB_CODE[estab]


def get_elipse_conn(estab: Estabelecimento, fast_executemany=False):
    string_conn = Variable.get(CONNECTIONS_VAR[estab], None)
    if string_conn is None:
        raise ValueError(f"Variável do airflow '{CONNECTIONS_VAR[estab]}' não foi definida.")

    return create_engine(string_conn, fast_executemany=fast_executemany)


def get_eng_conn(fast_executemany=False):
    if ENG_DATABASE_URL is None:
        raise ValueError("Variável do airflow 'ENG_DATABASE_URL' não foi definida.")

    return create_engine(ENG_DATABASE_URL, fast_executemany=fast_executemany)


def exec_merge(store_procedure: str):
    with get_eng_conn().begin() as conn:  # type: ignore
        conn.execute(text(f"EXEC {store_procedure}"))  # type: ignore
        # conn.commit()
    log_message(f"✅ Merge concluído para '{store_procedure}'.")
