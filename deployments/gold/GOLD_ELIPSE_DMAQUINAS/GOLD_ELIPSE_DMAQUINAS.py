import os
from datetime import timedelta
import pendulum
from airflow import DAG
from global_modules.functions import getGlobalConfig, getLocalConfig
from global_modules.operators import CustomSqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator


DAG_ID = "GOLD_ELIPSE_DMAQUINAS"
TEMPLATE_SEARCH_PATH = f'{os.environ["SEARCH_PATH"]}/gold/{DAG_ID}/'
TEMPLATE_SEARCH_PATH_GLOBAL = f'{os.environ["SEARCH_PATH_GLOBAL"]}/'

yaml_dependencies = getGlobalConfig()
dependencies = yaml_dependencies["wait_dependencies"]

#Dependências
yaml_data = getLocalConfig(f'gold/{DAG_ID}')
dw_merge_maquinas = yaml_data["dw_commands"]["merge_maquinas"]

tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "owner": "yan.arcanjo",
    "start_date": pendulum.datetime(year=2022, month=8, day=18).astimezone(tz),
    "email_on_failure": False,
    "email_on_success": False,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "retries": 1,
}

with DAG(
    DAG_ID,
    description="Carga de dados GOLD_D_MAQUINAS_SIMON",
    schedule="10 6 * * *",
    default_args=default_args,
    catchup=False,
    tags=["elipse","self_service", "gold"],
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=[TEMPLATE_SEARCH_PATH, TEMPLATE_SEARCH_PATH_GLOBAL],
) as dag:

    wait_dag_dependencies = CustomSqlSensor(
        task_id="wait_dag_dependencies",
        conn_id="airflow_db",
        sql=dependencies["sql"],
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

    merge_data = PostgresOperator(
        task_id="merge_stage_silver",
        postgres_conn_id='postgres_eng_server',
        sql=dw_merge_maquinas['sql'],
        #params={'source': dw_merge['source'],'target': dw_merge['target'], 'database_id': database_id},
        autocommit=True
    )


    wait_dag_dependencies >> merge_data
    
