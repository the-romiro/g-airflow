# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All tasks are run via [taskipy](https://github.com/taskipy/taskipy) through `uv`:

```bash
uv run task lint        # ruff check
uv run task format      # ruff check --fix && ruff format
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

SQL linting:
```bash
uv run sqlfluff lint dags/
uv run sqlfluff fix dags/
```

## Architecture

### Infrastructure

CeleryExecutor with Redis as broker and PostgreSQL as metadata DB. Airflow runs via Docker Compose (`docker-compose.yml`) using a custom image (`Dockerfile`) built on `apache/airflow:2.10.5`. The image adds MS SQL Server ODBC drivers (`msodbcsql17`) for connecting to SQL Server 2008, with TLS downgrade in `openssl.cnf` to handle the legacy server. Timezone is `America/Fortaleza`.

### Medallion Data Layers

DAGs implement a Bronze → Silver → Gold medallion architecture:

- **Bronze** (`dags/flakeflow/`): Raw ingestion from Flakeflow source system into PostgreSQL
- **Silver** (`dags/simon/silver/`, `dags/simon/full_silver/`, `dags/melhorias/`): Cleaning/transformation, loaded into intermediate tables
- **Gold** (`dags/simon/gold/`, `dags/simon/full_gold/`): Business aggregations and dimensional models

The `full_*` variants under `simon/` are full-reload versions (truncate + reload) vs incremental/merge variants.

### DAG Folder Convention

Each DAG lives in its own folder:
```
dags/<domain>/dags/<DAG_NAME>/
    <DAG_NAME>.py          # DAG definition
    sql_files/             # SQL files for this DAG
        extract_query.sql
        merge_query.sql
        ...
```

DAGs use `read_sql_file(filename, __file__)` to load SQL relative to the DAG file. SQL files must always be inside a `sql_files/` subfolder co-located with the DAG.

### DAG Pattern

All DAGs use the `@dag` decorator and a `dag_factory()` function called at module level:

```python
@dag(dag_id="...", default_args=default_args, ...)
def dag_factory():
    ...

dag_factory()
```

Standard `default_args` includes `on_failure_callback: notify_teams_on_failure` for MS Teams alerts on failure.

### Idempotency Pattern

Tasks that process large data use two idempotency mechanisms managed via `global_modules/utils.py`:

- **Parquet sentinel** (`extract_data_{estab}.parquet`): If present, extraction is skipped. Created atomically via `.tmp` → rename.
- **Step sentinel** (`{step_name}.finished`): If present, that step is skipped. Created via `sentinel.touch()`.

A final `clear_cache()` task always deletes parquets and sentinels, resetting state for the next run.

### Global Modules (`dags/global_modules/`)

Shared utilities imported by all DAGs:

| Module | Purpose |
|---|---|
| `database.py` | Connection factories for Elipse (sob/for/cra), ConnectorX, DuckDB, and the engineering Postgres (`ENG_DATABASE_URL`) |
| `utils.py` | `read_sql_file()`, `get_parquet_file()`, `get_sentinel_file()` |
| `operators.py` | `CustomSqlSensor` (waits for upstream DAGs), `SqlServerOperator` (extract to CSV → COPY to Postgres) |
| `ms_teams.py` | `notify_teams_on_failure()` callback |
| `functions.py` | `get_global_config()` (reads `global_files/global_config.yml`) |
| `sqlserver_hook.py` | Custom hook for SQL Server via pyodbc |
| `sharepoint/` | SharePoint integration utilities |

### Establishment Codes

Elipse source data comes from three establishments:

| Key | Code | Airflow conn var |
|---|---|---|
| `sob` | 20 | `elipse_sob` / `CX_ELIPSE_SOB` |
| `for` | 21 | `elipse_for` / `CX_ELIPSE_FOR` |
| `cra` | 40 | `elipse_cra` / `CX_ELIPSE_CRA` |

### Airflow Variables Required

These must be set in Airflow's Variable store:

| Variable | Used for |
|---|---|
| `CX_ELIPSE_SOB`, `CX_ELIPSE_FOR`, `CX_ELIPSE_CRA` | ConnectorX connection strings for Elipse establishments |
| `CX_ELIPSE_PG` | ConnectorX connection to Postgres |
| `CX_FLAKEFLOW_CONN` | ConnectorX connection to Flakeflow source |
| `DDB_PG_CONN` | DuckDB ATTACH string for engineering Postgres |
| `DDB_PG_FLAKEFLOW_CONN` | DuckDB ATTACH string for Flakeflow Postgres |
| `ENG_DATABASE_URL` | SQLAlchemy URL for engineering Postgres |
| `WEBHOOK_TEAMS` | MS Teams webhook URL for failure alerts |
| `SILVER_F_CICLOS_DAYS_TO_SEARCH` | Integer lookback window for ciclos pipeline |

### Dependency Waiting

Gold DAGs waiting on Silver DAGs use `CustomSqlSensor` with `wait_dependencies.sql` from `global_files/sql_files/`. The sensor checks `start_date` in the Airflow metadata DB and polls every 2 minutes with a 1-hour timeout.

### ETL Technology Choices

- **ConnectorX** (`cx.read_sql`): High-performance SQL → Arrow extraction from source databases
- **DuckDB**: In-process processing and writing Parquet files; also used to ATTACH PostgreSQL and bulk-load via `INSERT INTO pg.*`
- **SQLAlchemy / psycopg2**: Used for merge stored procedures and direct Postgres operations
- **Polars/Pandas**: Available for dataframe operations (imported per DAG as needed)
