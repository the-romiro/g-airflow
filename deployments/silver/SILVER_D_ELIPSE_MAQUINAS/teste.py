import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sha2, concat_ws, col, expr, row_number, trim, upper, when
from pyspark.sql.types import IntegerType
from google.cloud import storage
import argparse
import sys
from delta import *
#from google.cloud import bigquery
from pyspark.sql.window import Window

# process = sys.argv[1]

process = 'Elipse_fParadas_Sob_v2'
AMBIENTE = 'local'

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("ValidateDeltaTables") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
    .getOrCreate()

#.config("spark.jars.packages", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.1.jar") \

# Definição de variáveis de tempo
date_time_exec = datetime.now()
data_hora_inicio_file = date_time_exec.strftime("%Y%m%d_%H%M%S")#20240501_130511
data_hora_inicio_format_db = date_time_exec.strftime("%Y-%m-%d %H:%M:%S")#2024-05-01 13:05:11

logging.info(f'Inicio do processamento no Dataproc para {process} as {data_hora_inicio_format_db}')

################## DEFININDO AS VARIÁVEIS DE CAMINHOS ###########################
if AMBIENTE == 'local':
    # Caminhos dos arquivos no Local
    input_path = f"./grendene-core-dev/2-rz/{process}"
    output_path = f"./grendene-core-dev/3-sz/{process}"
    log_file_path = f"./logs_cluster_dataproc_mestre_core_dev/2-sz/{process}"
    
elif AMBIENTE == 'cloud':
    # Caminhos dos arquivos no Cloud Storage
    input_path = f"gs://grendene-core-dev/2-rz/{process}"
    output_path = f"gs://grendene-core-dev/3-sz/{process}"
    log_file_path = f"gs://logs_cluster_dataproc_mestre_core_dev/2-sz/{process}"
#################################################################################

#Colocar variaveis fixas aqui!---------------------------------------------------
condition = "is_active = true" # Apenas dados atuais da RZ
isIncremental = True
incremental_mode = 'DAY'
incremental_qtd = 60
incremental_cond = f"(Hora_Inicio >= current_date() - INTERVAL {incremental_qtd} {incremental_mode} \
    OR E3TimeStamp >= current_date() - INTERVAL {incremental_qtd} {incremental_mode})" # Filtro incremental das paradas

if isIncremental:
    condition += f" AND {incremental_cond}"   

# Estrutura do processo para logging
process_info = {
    "id_log_file_path":f'{log_file_path}_{data_hora_inicio_file}',
    "process_name": f"SZ_LOG_{process}",
    "date_ingestion" : f"{datetime.now().strftime('%Y-%m-%d')}",
    "start_time_script": data_hora_inicio_format_db,#
    "end_time_script": '',
    "isIncremental": 'true' if isIncremental else 'false',
    "incremental_mode": incremental_mode,
    "incremental_qtd": incremental_qtd,
    "status": "STARTED",
    "msg_retorno": '',
    "input_path": input_path,
    "output_path": output_path,
    "records_inserted": '0',
    "records_updated": '0',
    "start_file_reading": '',
    "end_time_reading": '',
    "start_select_calendar": "",
    "end_select_calendar": "",
    "start_filter_att_data": '',
    "end_filter_att_data": '',
    "start_filter_new_data": "",
    "end_filter_new_data": "",
    "start_transform_strings": '',
    "end_transform_strings": '',
    "start_add_column_log": '',
    "end_add_column_log": '',
    "start_export_data_delta": '',
    "end_export_data_delta": '',
}

try:
    # Leitura da Delta table e guarda ordem de colunas
    process_info["start_file_reading"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # LENDO DELTA RZ
    
    logging.info(f'Lendo arquivo: {input_path}')
    df_rz = DeltaTable.forPath(spark, input_path).toDF() \
        .where(condition) # Apenas registros ativos e filtro incremental
    
    initial_column_order = df_rz.columns 
    
    process_info["end_time_reading"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    ###########################################################################################################

    # APLICANDO CALENDÁRIO 
    
    # # Verifica se há colunas de timestamp e aplica o calendário correto
    # logging.info(f'Verificando se ha colunas de timestamp e aplica o calendario.')
    # process_info["start_select_calendar"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # timestamp_columns = [field.name for field in df_rz.schema.fields if str(field.dataType) == "TimestampType()"]
    # if any(df_rz.filter(col(col_name) < '1900-01-01').count() > 0 for col_name in timestamp_columns):
    #     calendar_spark = 'LEGACY'
    # else:
    #     calendar_spark = 'CORRECTED'

    # spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", calendar_spark)
    # spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", calendar_spark)
    # logging.info(f'Calendario selecionado: {calendar_spark}')
    # process_info["end_select_calendar"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ###########################################################################################################

    # OBTENDO DADOS MAIS RECENTES
    
    #Seleciona os registros mais recentes de cada hash chave
    # process_info["start_filter_att_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # logging.info('Selecionando os registros mais recentes de cada hash chave')
    # df_rz = df_rz.withColumn("dt_atualizacao", col("dt_atualizacao").cast("timestamp"))
    # window_spec = Window.partitionBy("hash_chave").orderBy(col("dt_atualizacao").desc())
    # df_rz = df_rz.withColumn("rank_hash_chave", row_number().over(window_spec)).filter(col("rank_hash_chave") == 1).drop("rank_hash_chave")
    
    # process_info["end_filter_att_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    
    ###########################################################################################################
    
    # Pega apenas as linhas que existem alguma alteração
    process_info["start_filter_new_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # OBTENDO APENAS OS DADOS NOVOS PARA SEREM INSERIDOS OU ATUALIZADOS

    if DeltaTable.isDeltaTable(spark, output_path):
        logging.info('Filtrando os dados que necessitam ser atualizados ou inseridos')
        df_sz_original = spark.read.format("delta").load(output_path)
        df_sz_new = df_rz.join(df_sz_original, df_rz.hash_columns == df_sz_original.hash_columns, "left_anti")   
    else: 
        df_sz_new = df_rz
        logging.info('Nova tabela, todos os dados serao inseridos.')
        
    df_sz_new = df_sz_new.withColumn("action", lit('I/U'))
    
    ###########################################################################################################

    # OBTENDO APENAS OS DADOS QUE NÃO EXISTEM MAIS NA FONTE

    if DeltaTable.isDeltaTable(spark, output_path):
        logging.info('Filtrando os dados que necessitam ser apagados')
        df_sz_original = spark.read.format("delta") \
                    .load(output_path)\
                    .alias("delta") \
                    .where(incremental_cond)
        df_removed = df_sz_original \
                    .join(df_rz, 
                        df_rz.hash_columns == df_sz_original.hash_columns, 
                          "left_anti")
    else:
        df_removed = spark.createDataFrame([], df_sz_new.schema)
    
    df_removed = df_removed.withColumn("action", lit(None))

    
    process_info["end_filter_new_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ###########################################################################################################

    # OBTENDO APENAS OS DADOS NOVOS PARA SEREM INSERIDOS

    # Identifica as colunas do tipo string e aplica a transformação
    process_info["start_transform_strings"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info('Aplicando tratamento de em colunas strings')
    string_cols = [field.name for field in df_sz_new.schema.fields if str(field.dataType) == "StringType()" and field.name not in ['hash_columns','hash_chave','data_atualizacao_bq']]
    for col_name in string_cols:
        df_sz_new = df_sz_new.withColumn(col_name, trim(upper(when(col(col_name) == '', None).otherwise(col(col_name)))))
        
    process_info["end_transform_strings"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    process_info["start_add_column_log"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    
    #df_sz_new = df_sz_new.select(*initial_column_order)
    process_info["end_add_column_log"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # exporta dados para SZ com merge
    logging.info(f'Salvando dados em Delta Table: {output_path}')
    process_info["start_export_data_delta"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ############# UNINDO DADOS NOVOS COM DADOS A SEREM REMOVIDOS ##########
    df_sz_new = df_sz_new.unionByName(df_removed)

    df_sz_new = df_sz_new.drop('key_log_file_path', 'dt_atualizacao','dt_ingestao','nm_arquivo_origem') # Removendo logs anteriores
    
    # Adiciona coluna de ID de log
    df_sz_new = df_sz_new.withColumns({
        "key_log_file_path": lit(f'{log_file_path}_{data_hora_inicio_file}'),
        "dt_atualizacao": lit(data_hora_inicio_format_db),
        "dt_ingestao": lit(datetime.now().strftime("%Y-%m-%d")),
        "nm_arquivo_origem": lit(input_path),
    })
    
    #client = storage.Client()
    if DeltaTable.isDeltaTable(spark, output_path):
        logging.info('Realizando metodo de MERGE para inserir e atualizar dados')
        df_sz_original = DeltaTable.forPath(spark, output_path) 
        # Realizando o merge e capturando as estatísticas
        df_sz_original.alias("delta") \
            .merge(
                df_sz_new.alias("stage"),
                "delta.hash_chave = stage.hash_chave"
            ) \
            .whenNotMatchedInsertAll(condition = "stage.action = 'I/U'") \
            .whenMatchedUpdateAll(condition = "stage.action = 'I/U'") \
            .whenMatchedDelete(condition = "stage.action = 'D'") \
            .execute()

        logging.info('MERGE finalizado, dados atualizados.')
        
        # Captura quantidade de linhas inseridas e atualizadas
        df_metrics = df_sz_original.history().select("version", "timestamp", "operation", "operationMetrics")
        # Obtenha a linha com o maior valor de versão
        max_version_row = df_metrics.orderBy(col("version").desc()).first()
        # Extraia numTargetRowsInserted e numTargetRowsUpdated de operationMetrics
        operation_metrics = max_version_row["operationMetrics"]
        num_target_rows_inserted = operation_metrics.get("numTargetRowsInserted", "0")
        num_target_rows_updated = operation_metrics.get("numTargetRowsUpdated", "0")
        process_info["records_inserted"] = str(f"{num_target_rows_inserted}")
        process_info["records_updated"] = str(f"{num_target_rows_updated}")
        #print(f"numTargetRowsInserted: {num_target_rows_inserted} | numTargetRowsUpdated: {num_target_rows_updated}")
    else:
        logging.info('Realizando metodo de insert, eh uma nova tabela.')
        df_sz_new.write.format("delta").mode("append").option("mergeSchema", "true").save(output_path)
        process_info["records_inserted"] = str(df_sz_new.count())
    process_info["end_export_data_delta"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    process_info["status"] = 'SUCESSO'
    process_info["msg_retorno"] = str(f'SUCESSO NA EXECUCAO')
    logging.info('SUCESSO NA EXECUCAO')
    
except Exception as e:
    process_info["msg_retorno"] = str(f'MSG ERRO -> {e}')
    process_info["status"] = 'ERRO'
    logging.error(f"Erro na execução: {e}")
finally:
    process_info["end_time_script"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info('-' * 30)
    logging.info('Resumo da execucao:')
    for key, value in process_info.items():
        logging.info(f' - {key}: {value}')
    df_log = spark.read.json(spark.sparkContext.parallelize([process_info]))
    #ordena colunas
    df_log = df_log.select(
    "id_log_file_path",
    "process_name",
    "date_ingestion",
    "start_time_script",
    "end_time_script",
    "status",
    "msg_retorno",
    "input_path",
    "output_path",
    "records_inserted",
    "records_updated",
    "start_file_reading",
    "end_time_reading",
    "start_select_calendar",
    "end_select_calendar",
    "start_filter_att_data",
    "end_filter_att_data",
    "start_filter_new_data",
    "end_filter_new_data",
    "start_transform_strings",
    "end_transform_strings",
    "start_add_column_log",
    "end_add_column_log",
    "start_export_data_delta",
    "end_export_data_delta",
    )
    df_log.write.format("delta").mode("append").option("mergeSchema", "true").save(log_file_path)
    logging.info(f'Log Salvo -> {log_file_path}')
    logging.info('FIM DA EXECUCAO!')