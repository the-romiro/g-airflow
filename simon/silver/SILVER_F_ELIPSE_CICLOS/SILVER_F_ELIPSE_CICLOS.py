from datetime import datetime, timedelta
from pathlib import Path

import connectorx as cx
import duckdb as ddb
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from global_modules.database import (
    Estabelecimento,
    get_ciclos_search_window,
    get_cx_conn,
    get_duckdb_conn,
    get_estab_code,
)
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import get_parquet_file, get_sentinel_file, read_sql_file

log = LoggingMixin().log

default_args = {
    "owner": "yan arcanjo",
    "start_date": datetime(2025, 1, 26, 6, 5),
    "on_failure_callback": notify_teams_on_failure,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _get_parquet_map() -> dict[Estabelecimento, Path]:
    return {
        "sob": get_parquet_file("sob", __file__),
        "for": get_parquet_file("for", __file__),
        "cra": get_parquet_file("cra", __file__),
    }


def _get_merged_parquet() -> Path:
    return get_parquet_file("merged", __file__)


def _get_sentinel(name: str) -> Path:
    return get_sentinel_file(name, __file__)


@task(retries=3, retry_delay=timedelta(minutes=2))
def extract_data(estab: Estabelecimento):
    parquet_file = _get_parquet_map()[estab]

    if parquet_file.exists():
        log.info(f"[SKIP] {estab} já processado")
        return

    log.info(f"[START] Descobrindo tabelas Ciclo em {estab}")

    # Step 1: descobrir tabelas Ciclo disponíveis
    tables = cx.read_sql(
        get_cx_conn(estab),
        read_sql_file("discover_tables.sql", __file__),
        return_type="arrow",
    )
    table_names = tables["TABLE_NAME"].to_pylist()

    log.info(f"[INFO] {len(table_names)} tabelas encontradas em {estab}")

    if not table_names:
        log.warning(f"[WARN] Nenhuma tabela Ciclo encontrada em {estab}, abortando")
        return

    # Step 2: janela de dias (fallback para máquinas novas)
    dt_fim = datetime.now()
    dt_inicio = dt_fim - timedelta(days=get_ciclos_search_window())

    estab_code = get_estab_code(estab)

    # Step 3: buscar último registro por máquina em silver.oee_fciclos
    silver_df = cx.read_sql(
        get_cx_conn("pg"),
        read_sql_file("silver_max_por_maquina.sql", __file__).format(estab_code=estab_code),
        return_type="arrow",
    )
    silver_maxima: dict[str, datetime] = {
        str(equip): ts
        for equip, ts in zip(
            silver_df["id_equipamento"].to_pylist(),
            silver_df["ultima_data_hora"].to_pylist(),
        )
        if ts is not None
    }

    log.info(f"[INFO] {len(silver_maxima)} máquinas com registros em silver para {estab}")

    # Step 4: buscar MAX(E3TimeStamp) no SQL Server para todas as tabelas em lote
    source_max_template = read_sql_file("source_max_por_maquina.sql", __file__)
    max_parts = [
        source_max_template.format(id_equipamento=name[6:], table_name=name) for name in table_names
    ]
    source_df = cx.read_sql(
        get_cx_conn(estab),
        "\nUNION ALL\n".join(max_parts),
        return_type="arrow",
    )
    source_maxima: dict[str, datetime] = {
        str(equip): ts
        for equip, ts in zip(
            source_df["id_equipamento"].to_pylist(),
            source_df["max_ts"].to_pylist(),
        )
        if ts is not None
    }

    # Step 5: filtrar máquinas com novos ciclos
    tables_with_new_data = []
    for name in table_names:
        id_equip = name[6:]  # "Ciclo 123" → "123"
        source_max = source_maxima.get(id_equip)
        silver_max = silver_maxima.get(id_equip)

        if source_max is None:
            log.info(f"[SKIP] {name}: sem dados no SQL Server")
            continue

        if silver_max is None or source_max > silver_max:
            tables_with_new_data.append(name)
        else:
            log.info(f"[SKIP] {name}: sem novos ciclos (source={source_max}, silver={silver_max})")

    if not tables_with_new_data:
        log.info(f"[SKIP] {estab}: nenhuma máquina com novos ciclos")
        return

    log.info(
        f"[INFO] {len(tables_with_new_data)}/{len(table_names)} máquinas com novos ciclos em {estab}"
    )

    # Step 6: montar UNION ALL apenas para máquinas com novos dados
    extract_template = read_sql_file("extract_ciclos.sql", __file__)

    parts = [
        extract_template.format(
            estab=estab_code,
            id_equipamento=name[6:],  # "Ciclo 123" → "123"
            table_name=name,
            dt_inicio=dt_inicio.strftime("%Y-%m-%d %H:%M:%S"),
            dt_fim=dt_fim.strftime("%Y-%m-%d %H:%M:%S"),
        )
        for name in tables_with_new_data
    ]

    # Step 7: executar em paralelo (até 8 threads)
    n_threads = min(8, len(parts))
    chunk_size = -(-len(parts) // n_threads)  # ceiling division
    queries = [
        "\nUNION ALL\n".join(parts[i : i + chunk_size]) for i in range(0, len(parts), chunk_size)
    ]

    log.info(f"[INFO] Executando {len(queries)} queries em paralelo ({len(parts)} tabelas)")
    df = cx.read_sql(get_cx_conn(estab), queries, return_type="arrow")

    log.info(f"[DONE QUERY] {df.shape[0]} linhas, {(df.nbytes / (1024**2)):.2f}MB")

    # Step 8: gravar parquet ZSTD com write atômico
    temp_file = parquet_file.with_suffix(".tmp")

    with ddb.connect() as con:
        con.register("df", df)
        con.execute(f"COPY (SELECT * FROM df) TO '{temp_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    del df  # clear memory

    temp_file.rename(parquet_file)

    log.info(f"[DONE] {estab} -> parquet gravado")


@task
def merge_parquets():
    merged_file = _get_merged_parquet()

    if merged_file.exists():
        log.info(f"[SKIP] '{merged_file.name}' já processado")
        return

    parquet_paths = [p for p in _get_parquet_map().values() if p.exists()]

    if not parquet_paths:
        log.warning("[WARN] Nenhum parquet disponível para mesclar")
        return

    temp_file = merged_file.with_suffix(".tmp")
    paths_glob = ", ".join(f"'{p}'" for p in parquet_paths)

    with ddb.connect() as con:
        con.execute(
            f"COPY (SELECT * FROM read_parquet([{paths_glob}], union_by_name=true)) "
            f"TO '{temp_file}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    temp_file.rename(merged_file)
    log.info(f"[DONE] {len(parquet_paths)} parquets mesclados → {merged_file.name}")


@task(retries=2, retry_delay=timedelta(minutes=1))
def copy_to_stage():
    sentinel = _get_sentinel("copy_to_stage")

    if sentinel.exists():
        log.info("[SKIP] copy_to_stage já concluído")
        return

    merged_file = _get_merged_parquet()

    if not merged_file.exists():
        log.warning("[WARN] Parquet mesclado não encontrado, abortando")
        return

    pg_con_str = get_duckdb_conn("pg")

    with ddb.connect() as con:
        con.execute(f"ATTACH '{pg_con_str}' AS pg (TYPE POSTGRES);")
        con.execute(f"COPY pg.stage.stage_ciclos FROM '{merged_file}' (FORMAT PARQUET);")

    sentinel.touch()
    log.info("[DONE] COPY concluído → stage.stage_ciclos")


@task
def clear_cache():
    log.info("[CLEANUP] Limpando parquets e sentinelas.")

    for parquet in [*_get_parquet_map().values(), _get_merged_parquet()]:
        parquet.unlink(missing_ok=True)

    _get_sentinel("copy_to_stage").unlink(missing_ok=True)


@dag(
    dag_id="SILVER_F_CICLOS_SIMON",
    default_args=default_args,
    schedule="25 9,18 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=3,
    tags=["elipse", "ciclos", "silver"],
)
def dag_factory():
    sob_data = extract_data.override(task_id="extract_data_sob")("sob")
    for_data = extract_data.override(task_id="extract_data_for")("for")
    cra_data = extract_data.override(task_id="extract_data_cra")("cra")

    merge_parquet = merge_parquets()
    copy = copy_to_stage()

    merge_table = SQLExecuteQueryOperator(
        task_id="merge_stage_to_silver",
        conn_id="postgres_eng_server",
        sql=read_sql_file("merge_query.sql", __file__),
        autocommit=True,
    )

    truncate = SQLExecuteQueryOperator(
        task_id="truncate_stage",
        conn_id="postgres_eng_server",
        sql="TRUNCATE TABLE stage.stage_ciclos;",
    )

    clear = clear_cache()

    _ = [sob_data, for_data, cra_data] >> merge_parquet >> copy >> merge_table >> truncate >> clear


dag_factory()
