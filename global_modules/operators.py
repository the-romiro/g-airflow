from airflow.sensors.sql_sensor import SqlSensor
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from global_modules.sqlserver_hook import SqlServerHook
from typing import Any
import time
import csv
import os

class CustomSqlSensor(SqlSensor):
    """
    Override function poke() to show results from a specific query and validate:

    Show query results and validate a count of states and end date. 
    
    - state = 0 no result was found
    - end_date = None, the target dag has not ended yet.
    """
    def poke(self, context: Any):
        hook = self._get_hook()
        self.log.info("Poking: %s (with parameters %s)", self.sql, self.parameters)
        records = hook.get_records(self.sql, self.parameters)
        validation = [results for results in records if 0 in results]
        wait_for_completion = [results for results in records if None in results]
        self.log.info("Results from query: %s", records)
        if not records:
            if self.fail_on_empty:
                raise AirflowException("No rows returned, raising as per fail_on_empty flag")
            else:
                return False
        if self.failure is not None:
            if callable(self.failure):
                if self.failure(records):
                    raise AirflowException(f"Failure criteria met. self.failure({records}) returned True")
            else:
                raise AirflowException(f"self.failure is present, but not callable -> {self.failure}")
        if self.success is not None:
            if callable(self.success):
                return self.success(records)
            else:
                raise AirflowException(f"self.success is present, but not callable -> {self.success}")
        if not wait_for_completion:
            if not validation:
                return True
            else :
                raise AirflowException(f'The dag(s) {validation} were not executed in the specific period.')
        else:
            return False
        
class SqlServerOperator(BaseOperator):
    def __init__ (self, task_id: str, source_conn_id: str, sql_path: str, estab, filename, target_conn_id, target_table, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.conn_id = source_conn_id
        self.sql_path = sql_path
        self.estab = estab
        self.filename = filename
        self.target_conn_id = target_conn_id
        self.target_table = target_table

    def execute(self, context: Any):
        hook = SqlServerHook(self.conn_id, self.estab)

        # Pega o caminho da DAG que chamou esse hook
        dag_file_path = context['dag'].fileloc
        dag_dir = os.path.dirname(dag_file_path)

        # Constrói o caminho absoluto do arquivo SQL
        full_sql_path = os.path.join(dag_dir, self.sql_path)

        with open(full_sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()

        rows, columns = hook.extract(sql)

        self.save_to_csv(rows, columns)

        self.copy_table()
        
    def save_to_csv(self, rows, columns, batch_size=10000):
        # Salvando no CSV

        path = "/datalake/extraction_routine/"
        file_name = self.filename
        file_path = os.path.join(path, self.filename)
        start_time = time.time()

        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(columns)  # Escreve os cabeçalhos

            for i in range(0, len(rows), batch_size):
                writer.writerows(rows[i:i+batch_size])
       
        print(f'---Arquivo {file_name} salvo com sucesso em {file_path}! -> Tempo decorido: {round(time.time() - start_time,2)} segundos')
        
    

    def copy_table(self):

        start_time = time.time()

        path = "/datalake/extraction_routine/"
        file_name = self.filename
        file_path = os.path.join(path, self.filename)

        hook = PostgresHook(postgres_conn_id=self.target_conn_id)
        
        conn = hook.get_conn()
        cursor = conn.cursor()

        with open(file_path, "r", encoding="utf-8") as f:
            cursor.copy_expert(f"COPY {self.target_table} FROM STDIN WITH CSV HEADER DELIMITER ','", f)

        print(f'---Arquivo {file_name} copiado com sucesso para a tabela {self.target_table}! -> Tempo decorido: {round(time.time() - start_time,2)} segundos')

        conn.commit()
        cursor.close()
        conn.close()
        os.remove(file_path)