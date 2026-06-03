"""Orquestra o dbt de Melhorias (seed + build).

Disparada pelos Datasets bronze (aprovacao + ganhos), roda o dbt CLI sobre o
projeto em melhorias/dbt e emite o Dataset gold. Credencial vem da Airflow
Variable ENG_DATABASE_URL, quebrada em env vars DBT_* (ADR 0002).
"""

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from global_modules.ms_teams import notify_teams_on_failure
from sqlalchemy.engine import make_url

log = LoggingMixin().log

MEL_APROVACAO_DATASET = Dataset("melhorias://bronze/melhoria_aprovacao")
MEL_GANHOS_DATASET = Dataset("melhorias://bronze/melhoria_ganhos")
MEL_GOLD_DATASET = Dataset("melhorias://gold/melhorias")

# melhorias/dags/GOLD_MELHORIAS_DBT/ -> parents[2] = melhorias
DBT_PROJECT_DIR = Path(__file__).parents[2] / "dbt"


def _dbt_env() -> dict[str, str]:
    """Quebra ENG_DATABASE_URL (URL SQLAlchemy) nas env vars que o profiles.yml lê."""
    raw = Variable.get("ENG_DATABASE_URL", None)
    if raw is None:
        raise ValueError("Variável do airflow 'ENG_DATABASE_URL' não foi definida.")

    url = make_url(raw)
    return {
        **os.environ,
        "DBT_HOST": url.host or "",
        "DBT_PORT": str(url.port or 1433),
        "DBT_DB": url.database or "dbengenharia",
        "DBT_USER": url.username or "",
        "DBT_PWD": url.password or "",
    }


def _run_dbt(command: list[str]) -> None:
    env = _dbt_env()
    full = [
        "dbt",
        *command,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
    ]
    log.info(f"[dbt] {' '.join(command)}")
    # Streama o stdout/stderr do dbt linha a linha pro logger do Airflow (aparece ao
    # vivo no log da task). subprocess.run nao capturava o fd do subprocesso, entao o
    # output do dbt nao entrava no task log.
    process = subprocess.Popen(  # noqa: S603
        full,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        log.info(line.rstrip())
    returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, full)


@task
def dbt_deps():
    _run_dbt(["deps"])


@task
def dbt_seed():
    _run_dbt(["seed"])


@task(outlets=[MEL_GOLD_DATASET])
def dbt_build():
    _run_dbt(["build"])


default_args = {
    "owner": "Antonio Albuquerque",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="GOLD_MELHORIAS_DBT",
    default_args=default_args,
    schedule=[MEL_APROVACAO_DATASET, MEL_GANHOS_DATASET],
    catchup=False,
    max_active_runs=1,
    tags=["melhorias", "gold", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    _ = start >> dbt_deps() >> dbt_seed() >> dbt_build() >> end
