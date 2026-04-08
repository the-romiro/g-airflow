from datetime import datetime, timedelta
from pathlib import Path

import connectorx as cx
import duckdb as ddb
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from global_modules.database import Estabelecimento, get_cx_conn, get_duckdb_conn, get_estab_code
from global_modules.ms_teams import notify_teams_on_failure
from global_modules.utils import DUCKDB_THREADS, get_parquet_file, read_sql_file

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


@task(retries=3, retry_delay=timedelta(minutes=2))
def extract_data(estab: Estabelecimento):
    parquet_file = _get_parquet_map()[estab]

    if parquet_file.exists():
        log.info(f"[SKIP] {estab} já processado")
        return

    log.info(f"[START] Descobrindo tabelas Ciclo em {estab}")

    # Step 1: descobrir tabelas Ciclo disponíveis na planta
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

    # Step 2: janela de 10 dias (fallback para máquinas novas)
    dt_fim = datetime.now()
    dt_inicio = dt_fim - timedelta(days=10)

    estab_code = get_estab_code(estab)

    log.info(get_cx_conn("pg"))
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
        equip: ts
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

    # Step 7: executar via connectorx em paralelo (até 8 threads)
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

    with ddb.connect(
        config={
            "threads": DUCKDB_THREADS,
            "preserve_insertion_order": False,
            "memory_limit": "2GB",
        }
    ) as con:
        con.register("df", df)
        con.execute(f"COPY (SELECT * FROM df) TO '{temp_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    del df

    temp_file.rename(parquet_file)

    log.info(f"[DONE] {estab} -> parquet gravado")


@task(retries=2, retry_delay=timedelta(minutes=1))
def concat_and_load_stage():
    parquet_map = _get_parquet_map()
    # incluir apenas parquets que existem (planta pode não ter tabelas Ciclo)
    parquet_paths = [p for p in parquet_map.values() if p.exists()]

    if not parquet_paths:
        log.warning("[WARN] Nenhum parquet disponível para carregar")
        return

    paths_glob = ", ".join(f"'{p}'" for p in parquet_paths)
    pg_con_str = get_duckdb_conn("pg")
    tmp_table = "silver.temp_ciclos"

    with ddb.connect(
        config={
            "threads": DUCKDB_THREADS,
            "preserve_insertion_order": False,
            "memory_limit": "2GB",
        }
    ) as con:
        con.execute(f"ATTACH '{pg_con_str}' AS pg (TYPE POSTGRES);")

        check_table_query = f"SELECT to_regclass('{tmp_table}') IS NOT NULL"
        tmp_table_exists = con.sql(
            f"SELECT * FROM postgres_query('pg', $${check_table_query}$$)"
        ).fetchone()

        if tmp_table_exists is not None and tmp_table_exists[0]:
            log.info(f"[SKIP] '{tmp_table}' já existe")
            return

        size_mb = con.sql(
            f"SELECT SUM(total_uncompressed_size) / (1024*1024.0) FROM parquet_metadata([{paths_glob}])"
        ).fetchone() or (0,)

        log.info(f"[START] Carregando parquets ({size_mb[0]:.2f}MB)")

        con.execute(
            f"CREATE TEMP VIEW v_source AS SELECT * FROM read_parquet([{paths_glob}], union_by_name=true)"
        )

        con.execute(f"CREATE OR REPLACE TABLE pg.{tmp_table} AS SELECT * FROM v_source LIMIT 0")

        total_rows = con.execute(f"INSERT INTO pg.{tmp_table} SELECT * FROM v_source").fetchone()

        log.info(f"[DONE] Total linhas inseridas: {total_rows}")


@task
def clear_cache():
    log.info("[CLEANUP] Limpando arquivos")
    for parquet in _get_parquet_map().values():
        parquet.unlink(missing_ok=True)


@dag(
    dag_id="SILVER_F_CICLOS_SIMON",
    default_args=default_args,
    schedule="20 9,18 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=3,
    tags=["elipse", "ciclos", "silver"],
)
def dag_factory():
    sob_data = extract_data.override(task_id="extract_data_sob")("sob")
    for_data = extract_data.override(task_id="extract_data_for")("for")
    cra_data = extract_data.override(task_id="extract_data_cra")("cra")

    concat = concat_and_load_stage()

    merge = SQLExecuteQueryOperator(
        task_id="merge_table_and_drop_temp",
        conn_id="postgres_eng_server",
        sql=read_sql_file("merge_query.sql", __file__),
        autocommit=True,
    )

    clear = clear_cache()

    _ = [sob_data, for_data, cra_data] >> concat >> merge >> clear


dag_factory()
