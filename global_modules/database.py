from airflow.models import Variable
from sqlalchemy import create_engine, text

from global_modules.sharepoint.logs import log_message

ENG_DATABASE_URL = Variable.get("ENG_DATABASE_URL", None)


def get_eng_conn(fast_executemany=False):
    if ENG_DATABASE_URL is None:
        raise ValueError("Variável do airflow 'ENG_DATABASE_URL' não foi definida.")

    return create_engine(ENG_DATABASE_URL, fast_executemany=fast_executemany)


def exec_merge(store_procedure: str):
    with get_eng_conn().begin() as conn:  # type: ignore
        conn.execute(text(f"EXEC {store_procedure}"))  # type: ignore
        # conn.commit()
    log_message(f"✅ Merge concluído para '{store_procedure}'.")
