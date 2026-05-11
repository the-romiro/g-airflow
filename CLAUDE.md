# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All tasks are run via [taskipy](https://github.com/taskipy/taskipy) through `uv`:

```bash
uv run task lint        # ruff check
uv run task format      # ruff check --fix && ruff format
uv run task lint-sql    # sqlfluff lint dags/
uv run task format-sql  # sqlfluff format dags/
uv run task test        # pytest -s -x --cov=./ -vv (runs lint first; generates htmlcov/)
uv run task docker      # docker compose up -d (detached)
uv run task dw          # docker compose up --watch (rebuilds on file change; runs build first)
uv run task build       # docker build -t airflow-gre:2.10.5 .
uv run task push        # build + docker push
```

Run a single test file:
```bash
uv run pytest -s -x -vv path/to/test_file.py
```

Initial setup:
```bash
cp .env.example .env   # fill in AIRFLOW_UID (id -u), DRIVER_PATH, DOCKER_REGISTRY, SharePoint creds
uv sync --group dev
```

## Architecture

CeleryExecutor + Redis + PostgreSQL, via Docker Compose (`docker-compose.yml`). Custom image (`Dockerfile`) on `apache/airflow:2.10.5` with MS SQL Server ODBC drivers (`msodbcsql17`) and TLS downgrade (`openssl.cnf`) for SQL Server 2008. Timezone: `America/Fortaleza`. UI: `http://localhost:8080`.

DAGs implement **Bronze → Silver → Gold** medallion architecture for **SiMOn** (Sistema de Monitoramento Online):

| Layer | Paths | Description |
|---|---|---|
| Bronze | `dags/flakeflow/`, `dags/melhorias/dags/BRONZE_*` | Raw ingestion |
| Silver | `dags/simon/silver/`, `dags/simon/full_silver/`, `dags/melhorias/dags/SILVER_*` | Cleaning/transformation |
| Gold | `dags/simon/gold/`, `dags/simon/full_gold/` | Business aggregations |

`full_*` = truncate+reload; others = incremental/merge.

## DAG Folder Convention

```
dags/<domain>/dags/<DAG_NAME>/
    <DAG_NAME>.py
    sql_files/
        *.sql
```

Load SQL via `read_sql_file(filename, __file__)`. SQL must always live in `sql_files/` co-located with the DAG file.

## DAG Patterns

**Incremental** — `@dag` decorator + `dag_factory()` called at module level:

```python
@dag(dag_id="...", default_args=default_args, ...)
def dag_factory():
    ...

dag_factory()
```

**Full-reload** (`full_silver/`, `full_gold/`) — `with DAG(...)` context manager. Do not mix styles.

Standard `default_args`: `on_failure_callback: notify_teams_on_failure`, `retries=2`, `retry_delay=timedelta(minutes=2)`, `retry_exponential_backoff=True`, `max_retry_delay=timedelta(minutes=30)`.

**Scheduling**: Gold DAGs use `schedule=[<Dataset>]` triggered by Silver DAG outlets — not cron. New DAGs should use Datasets, not `CustomSqlSensor`.

## Global Modules (`dags/global_modules/`)

| Module | Key exports |
|---|---|
| `database.py` | `get_cx_conn()`, `get_duckdb_conn()`, `get_eng_conn()`, `get_elipse_conn()`, `exec_merge()`, `get_ciclos_search_window()` |
| `utils.py` | `read_sql_file()`, `get_parquet_file()`, `get_sentinel_file()`, `DUCKDB_THREADS`, `DUCKDB_SAVE_PARQUET_CONFIG` |
| `operators.py` | `CustomSqlSensor`, `SqlServerOperator` |
| `ms_teams.py` | `notify_teams_on_failure()` |
| `functions.py` | `get_global_config()` (reads `global_files/global_config.yml`), `get_local_config()` |
| `sharepoint/` | SharePoint integration via Graph API |

## ETL Stack

- **ConnectorX** (`cx.read_sql`): SQL → Arrow extraction from source DBs
- **DuckDB**: In-process processing; ATTACH PostgreSQL for bulk loads via `INSERT INTO pg.*`
- **SQLAlchemy / psycopg2**: Merge stored procedures, direct Postgres ops
- **Polars/Pandas**: Dataframe ops (imported per DAG as needed)

## Further Reference

- [`docs/configuration.md`](docs/configuration.md) — Airflow variables, connections, establishment codes
- [`docs/dag-patterns.md`](docs/dag-patterns.md) — Idempotency sentinels, legacy `CustomSqlSensor`
