from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from global_modules.ms_teams import notify_teams_on_failure
from global_modules.operators import CustomSqlSensor
from global_modules.utils import GLOBAL_FILES_PATH, read_sql_file

_WAIT_DEPS_SQL = read_sql_file("wait_dependencies.sql", str(GLOBAL_FILES_PATH))

default_args = {
    "owner": "yan.arcanjo",
    "start_date": datetime(2022, 8, 18, 7, 0),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="GOLD_ELIPSE_DMAQUINAS",
    description="Carga de dados GOLD_D_MAQUINAS_SIMON",
    default_args=default_args,
    schedule="10 6 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["elipse", "self_service", "gold"],
)
def dag_factory():
    wait = CustomSqlSensor(
        task_id="wait_dag_dependencies",
        conn_id="airflow_db",
        sql=_WAIT_DEPS_SQL,
        timeout=60 * 60 * 1,
        mode="reschedule",
        poke_interval=60 * 2,
        params={
            "execute_timedelta": {
                "dag_dependencies": [
                    "SILVER_D_SETORES_SIMON",
                    "SILVER_D_MAQUINAS_SIMON",
                    "SILVER_D_FABRICAS_SIMON",
                ],
                "execution_delta": [360, 360, 360],
            }
        },
    )

    merge = SQLExecuteQueryOperator(
        task_id="merge_stage_silver",
        conn_id="postgres_eng_server",
        sql=read_sql_file("merge_gold_dElipse_Maquinas.sql", __file__),
        autocommit=True,
    )

    _ = wait >> merge


dag_factory()
