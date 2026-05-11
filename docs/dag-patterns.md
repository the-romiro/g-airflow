# DAG Patterns Reference

## Idempotency

Tasks that process large data use two sentinel mechanisms from `global_modules/utils.py`:

- **Parquet sentinel** (`extract_data_{estab}.parquet`): If present, extraction is skipped. Created atomically via `.tmp` → rename (`get_parquet_file()`).
- **Step sentinel** (`{step_name}.finished`): If present, that step is skipped. Created via `sentinel.touch()` (`get_sentinel_file()`).

A final `clear_cache()` task always deletes parquets and sentinels, resetting state for the next run.

## Legacy Dependency Pattern (`CustomSqlSensor`)

Older DAGs use `CustomSqlSensor` with `wait_dependencies.sql` from `global_files/sql_files/`. The sensor checks `start_date` in the Airflow metadata DB via a Jinja-templated query, polling every 2 minutes with a 1-hour timeout.

New DAGs should use Dataset-based scheduling instead.

## Dataset-Based Scheduling

Silver DAGs declare a `Dataset` outlet:

```python
from airflow.datasets import Dataset

MY_DATASET = Dataset("my_dataset_uri")

@dag(..., outlets=[MY_DATASET])
def dag_factory():
    ...
```

Gold DAGs consume it:

```python
@dag(..., schedule=[MY_DATASET])
def dag_factory():
    ...
```

This removes the need for polling sensors and makes dependencies explicit in the Airflow UI.
