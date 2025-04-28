from datetime import datetime, timezone

import yaml


def getGlobalConfig():

    with open("/opt/airflow/dags/global_files/global_config.yml") as stream:
        return yaml.safe_load(stream)


def getLocalConfig(dag_name, dag=None):
    """
    Extract data fusion config file for a specific DAG.
    Args:
        dag_name: DAG name that's using this function.
    Returns:
        datafusion json DAG config file.
    """
    if dag is not None:
        if dag.latest_execution_date == None:
            conf = dag.get_dagrun(execution_date=datetime.now().replace(tzinfo=timezone.utc))
        else:
            conf = dag.get_dagrun(execution_date=dag.latest_execution_date).conf
        if conf:
            return conf
        else:
            with open(f"/opt/airflow/dags/deployments/{dag_name}/config.yml") as stream:
                return yaml.safe_load(stream)

    else:
        with open(f"/opt/airflow/dags/deployments/{dag_name}/config.yml") as stream:
            return yaml.safe_load(stream)
