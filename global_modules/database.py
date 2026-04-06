from functools import cache
from typing import Literal

from airflow.models import Variable
from sqlalchemy import create_engine, text

from global_modules.sharepoint.logs import log_message

ENG_DATABASE_URL = Variable.get("ENG_DATABASE_URL", None)

type Estabelecimento = Literal["sob", "cra", "for"]
type ConnectorxVars = Literal["sob", "cra", "for", "pg"]
type DuckdbVars = Literal["pg"]

CONNECTIONS_VAR: dict[Estabelecimento, str] = {
    "sob": "elipse_sob",
    "for": "elipse_for",
    "cra": "elipse_cra",
}


CX_CONNECTIONS_VAR: dict[ConnectorxVars, str] = {
    "sob": "CX_ELIPSE_SOB",
    "for": "CX_ELIPSE_FOR",
    "cra": "CX_ELIPSE_CRA",
    "pg": "CX_ELIPSE_PG",
}

DUCKDB_CONNECTIONS_VAR: dict[DuckdbVars, str] = {
    "pg": "DDB_PG_CONN",
}

ESTAB_CODE: dict[Estabelecimento, int] = {
    "sob": 20,
    "for": 21,
    "cra": 40,
}


@cache
def _get_var(conn: str) -> str:
    string_conn = Variable.get(conn, None)
    if string_conn is None:
        raise ValueError(f"Variável do airflow '{conn}' não foi definida.")

    return string_conn


def get_estab_code(estab: Estabelecimento):
    return ESTAB_CODE[estab]


def get_elipse_conn(estab: Estabelecimento, fast_executemany=False):
    string_conn = _get_var(CONNECTIONS_VAR[estab])

    return create_engine(string_conn, fast_executemany=fast_executemany)


def get_cx_conn(conn: ConnectorxVars):
    string_conn = _get_var(CX_CONNECTIONS_VAR[conn])

    return string_conn


def get_duckdb_conn(conn: DuckdbVars):
    string_conn = _get_var(DUCKDB_CONNECTIONS_VAR[conn])

    return string_conn


def get_eng_conn(fast_executemany=False):
    if ENG_DATABASE_URL is None:
        raise ValueError("Variável do airflow 'ENG_DATABASE_URL' não foi definida.")

    return create_engine(ENG_DATABASE_URL, fast_executemany=fast_executemany)


def exec_merge(store_procedure: str):
    with get_eng_conn().begin() as conn:  # type: ignore
        conn.execute(text(f"EXEC {store_procedure}"))  # type: ignore
        # conn.commit()
    log_message(f"✅ Merge concluído para '{store_procedure}'.")
