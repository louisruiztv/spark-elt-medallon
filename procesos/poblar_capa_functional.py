#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Functional (Enriquecimiento de datos)
Adaptado desde Hive SQL original con optimización de JOINs y particionamiento dinámico
"""

import sys
import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, broadcast, lit, count, when, avg, min, max
from pyspark.sql.functions import date_format, to_date, from_unixtime, unix_timestamp

# =============================================================================
# @section 1. Configuración de logging y parámetros
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Functional')
    parser.add_argument('--env', type=str, default='TopicosB', help='Entorno: DEV, QA, PROD')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument('--source_db', type=str, default='curated', help='Base de datos origen')
    parser.add_argument('--num-executors', type=int, default=8, help='Número de executors')
    parser.add_argument('--executor-memory', type=str, default='2g', help='Memoria por executor')
    parser.add_argument('--executor-cores', type=int, default=2, help='Cores por executor')
    parser.add_argument('--enable-broadcast', action='store_true', default=True, help='Usar broadcast join para tablas pequeñas')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession con tuning de recursos
# =============================================================================

def create_spark_session(args, app_name="ProcesoFunctional-LlanosBardales"):
    """
    Crea SparkSession con configuración optimizada equivalente a tuning Hive/MR
    """
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.exec.max.dynamic.partitions", "9999") \
        .config("hive.exec.max.dynamic.partitions.pernode", "9999") \
        .config("spark.executor.instances", args.num_executors) \
        .config("spark.executor.memory", args.executor_memory) \
        .config("spark.executor.cores", args.executor_cores) \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "2") \
        .config("spark.executor.memoryOverhead", "512m") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.files.maxPartitionBytes", "1073741824") \
        .config("spark.sql.files.openCostInBytes", "1073741824") \
        .config("spark.sql.parquet.writeLegacyFormat", "false") \
        .config("spark.sql.parquet.int96AsTimestamp", "false") \
        .config("hive.exec.compress.output", "true") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares
# =============================================================================

def crear_database(spark, env, username, base_path):
    """Crea la base de datos Functional si no existe"""
    db_name = f"{env}_functional".lower()
    # Nota: el script original usa ${hiveconf:ENV} en la ruta, no PARAM_USERNAME
    db_location = f"{base_path}/{env}/datalake/{db_name.upper()}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    logger.info(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def crear_tabla_parquet_functional(spark, db_name, table_name, schema, location, partitioned_by=None):
    """Crea tabla externa en Hive con formato Parquet para capa Functional"""
    
    columns_def = []

    for f in schema.fields:
        # 🔥 excluir columnas de partición del bloque principal
        if partitioned_by and f.name.upper() in [c.upper() for c in partitioned_by]:
            continue
        
        columns_def.append(f"{f.name} {f.dataType.simpleString().upper()}")

    partition_clause = ""
    if partitioned_by:
        partition_cols = ", ".join([f"{c} STRING" for c in partitioned_by])
        partition_clause = f"PARTITIONED BY ({partition_cols})"

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        {', '.join(columns_def)}
    )
    {partition_clause}
    STORED AS PARQUET
    LOCATION '{location}'
    TBLPROPERTIES (
        'store.charset'='ISO-8859-1',
        'retrieve.charset'='ISO-8859-1',
        'parquet.compression'='SNAPPY'
    )
    """
    spark.sql(create_sql)
    logger.info(f"✅ Tabla '{db_name}.{table_name}' registrada en Hive Metastore")

def registrar_vista_temporal(spark, df, view_name):
    """Registra un DataFrame como vista temporal para consultas SQL"""
    df.createOrReplaceTempView(view_name)
    logger.debug(f"📋 Vista temporal '{view_name}' registrada")

def limpiar_vistas_temporales(spark, vistas):
    """Limpia vistas temporales para liberar memoria"""
    for vista in vistas:
        try:
            spark.catalog.dropTempView(vista)
            logger.debug(f"🗑️  Vista temporal '{vista}' eliminada")
        except:
            pass

# =============================================================================
# @section 4. Esquemas para capa Functional
# =============================================================================

SCHEMA_TRANSACCION_ENRIQUECIDA = StructType([
    StructField("ID_PERSONA", IntegerType(), True),
    StructField("NOMBRE_PERSONA", StringType(), True),
    StructField("EDAD_PERSONA", IntegerType(), True),
    StructField("SALARIO_PERSONA", DoubleType(), True),
    StructField("TRABAJO_PERSONA", StringType(), True),  # Nombre de empresa donde trabaja
    StructField("MONTO_TRANSACCION", DoubleType(), True),
    StructField("EMPRESA_TRANSACCION", StringType(), True),  # Nombre de empresa de la transacción
    StructField("FECHA_TRANSACCION", StringType(), True)  # Columna de partición
])

# =============================================================================
# @section 5. Proceso de enriquecimiento (3 pasos de JOIN)
# =============================================================================

def paso_1_unir_transaccion_persona(spark, db_curated, enable_broadcast=True):
    """
    PASO 1: Unir TRANSACCION con PERSONA para obtener datos del cliente
    Equivalente a: TMP_TRANSACCION_ENRIQUECIDA_1
    """
    logger.info("🔄 PASO 1: Uniendo TRANSACCION + PERSONA")
    
    df_transaccion = spark.table(f"{db_curated}.TRANSACCION")
    df_persona = spark.table(f"{db_curated}.PERSONA")
    
    # Optimización: broadcast para tabla PERSONA si es pequeña
    if enable_broadcast:
        persona_stats = df_persona.count()
        if persona_stats < 100000:  # Umbral configurable
            logger.info(f"📡 Usando broadcast join para PERSONA ({persona_stats} registros)")
            df_persona = broadcast(df_persona)
    
    df_result = df_transaccion.alias("T") \
        .join(df_persona.alias("P"), col("T.ID_PERSONA") == col("P.ID"), "inner") \
        .select(
            col("T.ID_PERSONA").cast(IntegerType()).alias("ID_PERSONA"),
            col("P.NOMBRE").alias("NOMBRE_PERSONA"),
            col("P.EDAD").cast(IntegerType()).alias("EDAD_PERSONA"),
            col("P.SALARIO").cast(DoubleType()).alias("SALARIO_PERSONA"),
            col("P.ID_EMPRESA").alias("ID_EMPRESA_PERSONA"),
            col("T.MONTO").cast(DoubleType()).alias("MONTO_TRANSACCION"),
            col("T.FECHA").alias("FECHA_TRANSACCION"),
            col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION")
        )
    
    logger.info(f"✅ PASO 1 completado: {df_result.count()} registros")
    return df_result

def paso_2_agregar_empresa_trabajo(spark, db_curated, df_paso1, enable_broadcast=True):
    """
    PASO 2: Unir con EMPRESA para obtener nombre de empresa donde trabaja la persona
    Equivalente a: TMP_TRANSACCION_ENRIQUECIDA_2
    """
    logger.info("🔄 PASO 2: Agregando empresa de trabajo de la persona")
    
    df_empresa = spark.table(f"{db_curated}.EMPRESA")
    
    if enable_broadcast:
        empresa_stats = df_empresa.count()
        if empresa_stats < 100000:
            logger.info(f"📡 Usando broadcast join para EMPRESA ({empresa_stats} registros)")
            df_empresa = broadcast(df_empresa)
    
    df_result = df_paso1.alias("T") \
        .join(df_empresa.alias("E"), col("T.ID_EMPRESA_PERSONA") == col("E.ID"), "left") \
        .select(
            col("T.ID_PERSONA"),
            col("T.NOMBRE_PERSONA"),
            col("T.EDAD_PERSONA"),
            col("T.SALARIO_PERSONA"),
            col("E.NOMBRE").alias("TRABAJO_PERSONA"),  # Alias para claridad
            col("T.MONTO_TRANSACCION"),
            col("T.FECHA_TRANSACCION"),
            col("T.ID_EMPRESA_TRANSACCION")
        )
    
    logger.info(f"✅ PASO 2 completado: {df_result.count()} registros")
    return df_result

def paso_3_agregar_empresa_transaccion(spark, db_curated, df_paso2, enable_broadcast=True):
    """
    PASO 3: Unir con EMPRESA para obtener nombre de empresa donde se realizó la transacción
    Equivalente a: TMP_TRANSACCION_ENRIQUECIDA_3
    """
    logger.info("🔄 PASO 3: Agregando empresa de la transacción")
    
    df_empresa = spark.table(f"{db_curated}.EMPRESA")
    
    if enable_broadcast:
        df_empresa = broadcast(df_empresa)
    
    df_result = df_paso2.alias("T") \
        .join(df_empresa.alias("E"), col("T.ID_EMPRESA_TRANSACCION") == col("E.ID"), "left") \
        .select(
            col("T.ID_PERSONA"),
            col("T.NOMBRE_PERSONA"),
            col("T.EDAD_PERSONA"),
            col("T.SALARIO_PERSONA"),
            col("T.TRABAJO_PERSONA"),
            col("T.MONTO_TRANSACCION"),
            col("T.FECHA_TRANSACCION"),
            col("E.NOMBRE").alias("EMPRESA_TRANSACCION")
        )
    
    logger.info(f"✅ PASO 3 completado: {df_result.count()} registros")
    return df_result

def insertar_tabla_final(spark, db_functional, df_final, table_name, partition_col="FECHA_TRANSACCION"):
    """
    Inserta datos en tabla final con particionamiento dinámico
    Equivalente a: INSERT OVERWRITE ... PARTITION(FECHA_TRANSACCION)
    """
    logger.info(f"💾 Insertando datos en {db_functional}.{table_name} con partición por {partition_col}")
    
    # Registrar vista temporal para usar SQL con particionamiento explícito
    df_final.createOrReplaceTempView("df_final_enriquecido")
    
    # Usar SQL para mantener compatibilidad con particionamiento dinámico de Hive
    insert_sql = f"""
    INSERT OVERWRITE TABLE {db_functional}.{table_name}
    PARTITION ({partition_col})
    SELECT 
        ID_PERSONA,
        NOMBRE_PERSONA,
        EDAD_PERSONA,
        SALARIO_PERSONA,
        TRABAJO_PERSONA,
        MONTO_TRANSACCION,
        EMPRESA_TRANSACCION,
        FECHA_TRANSACCION
    FROM df_final_enriquecido
    """
    
    spark.sql(insert_sql)
    
    # Reparar metadatos de particiones en Hive
    spark.sql(f"MSCK REPAIR TABLE {db_functional}.{table_name}")
    
    total_registros = spark.sql(f"SELECT COUNT(*) FROM {db_functional}.{table_name}").first()[0]
    logger.info(f"✅ Datos insertados: {total_registros} registros totales")
    
    # Mostrar distribución de particiones
    particiones = spark.sql(f"SHOW PARTITIONS {db_functional}.{table_name}").collect()
    logger.info(f"📁 Particiones creadas: {len(particiones)}")
    
    return total_registros

# =============================================================================
# @section 6. Funciones de monitoreo y validación
# =============================================================================

def validar_integridad_enriquecimiento(spark, db_curated, db_functional):
    """Valida que el proceso de enriquecimiento no pierda registros inesperadamente"""
    logger.info("🔍 Validando integridad del enriquecimiento...")
    
    # Contar registros originales en TRANSACCION
    total_transacciones = spark.sql(f"SELECT COUNT(*) FROM {db_curated}.TRANSACCION").first()[0]
    
    # Contar registros enriquecidos
    total_enriquecidas = spark.sql(f"SELECT COUNT(*) FROM {db_functional}.TRANSACCION_ENRIQUECIDA").first()[0]
    
    # Verificar nulos en campos críticos
    nulos = spark.sql(f"""
        SELECT 
            COUNT(CASE WHEN NOMBRE_PERSONA IS NULL THEN 1 END) as nulos_nombre,
            COUNT(CASE WHEN TRABAJO_PERSONA IS NULL THEN 1 END) as nulos_trabajo,
            COUNT(CASE WHEN EMPRESA_TRANSACCION IS NULL THEN 1 END) as nulos_empresa_transaccion
        FROM {db_functional}.TRANSACCION_ENRIQUECIDA
    """).first()
    
    logger.info(f"📊 Validación:")
    logger.info(f"   • Transacciones origen: {total_transacciones}")
    logger.info(f"   • Transacciones enriquecidas: {total_enriquecidas}")
    logger.info(f"   • Nulos en NOMBRE_PERSONA: {nulos[0]}")
    logger.info(f"   • Nulos en TRABAJO_PERSONA (LEFT JOIN esperado): {nulos[1]}")
    logger.info(f"   • Nulos en EMPRESA_TRANSACCION (LEFT JOIN esperado): {nulos[2]}")
    
    # Alerta si hay pérdida significativa (más del 5% sin justificación de LEFT JOIN)
    if total_enriquecidas < total_transacciones * 0.95:
        logger.warning(f"⚠️  Pérdida significativa de registros: {(1-total_enriquecidas/total_transacciones)*100:.2f}%")
    
    return {
        "origen": total_transacciones,
        "enriquecidas": total_enriquecidas,
        "nulos_nombre": nulos[0],
        "nulos_trabajo": nulos[1],
        "nulos_empresa": nulos[2]
    }

def mostrar_metricas_rendimiento(df, etapa):
    """Muestra métricas básicas de calidad de datos por etapa"""
    logger.info(f"📈 Métricas - {etapa}:")
    df.select(
        count("*").alias("total_registros"),
        avg("MONTO_TRANSACCION").alias("monto_promedio"),
        min("MONTO_TRANSACCION").alias("monto_min"),
        max("MONTO_TRANSACCION").alias("monto_max")
    ).show()

# =============================================================================
# @section 7. Ejecución principal
# =============================================================================

def main():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    args = parse_arguments()
    
    logger.info(f"🚀 Iniciando proceso Functional | Env: {args.env.upper()} | Timestamp: {timestamp}")
    logger.info(f"👤 Usuario: {args.username} | Base path: {args.base_path}")
    logger.info(f"⚙️  Executors: {args.num_executors} | Memory: {args.executor_memory} | Cores: {args.executor_cores}")
    logger.info("-" * 80)
    
    spark = create_spark_session(args)
    
    try:
        # Configurar nombres de bases de datos
        env_lower = args.env.lower()
        db_functional = f"{env_lower}_functional"
        db_curated = f"{env_lower}_{args.source_db}"
        
        # 1. Crear database Functional
        crear_database(spark, env_lower, args.username, args.base_path)
        
        # 2. Crear tabla final TRANSACCION_ENRIQUECIDA
        location_final = f"{args.base_path}/{env_lower}/datalake/{db_functional.upper()}/TRANSACCION_ENRIQUECIDA"
        crear_tabla_parquet_functional(
            spark=spark,
            db_name=db_functional,
            table_name="TRANSACCION_ENRIQUECIDA",
            schema=SCHEMA_TRANSACCION_ENRIQUECIDA,
            location=location_final,
            partitioned_by=["FECHA_TRANSACCION"]
        )
        
        # 3. Ejecutar pipeline de enriquecimiento (3 pasos)
        logger.info("🔗 Iniciando pipeline de enriquecimiento...")
        
        # PASO 1: TRANSACCION + PERSONA
        df_paso1 = paso_1_unir_transaccion_persona(
            spark=spark,
            db_curated=db_curated,
            enable_broadcast=args.enable_broadcast
        )
        mostrar_metricas_rendimiento(df_paso1, "Post-PASO-1")
        
        # PASO 2: + EMPRESA (trabajo de la persona)
        df_paso2 = paso_2_agregar_empresa_trabajo(
            spark=spark,
            db_curated=db_curated,
            df_paso1=df_paso1,
            enable_broadcast=args.enable_broadcast
        )
        mostrar_metricas_rendimiento(df_paso2, "Post-PASO-2")
        
        # PASO 3: + EMPRESA (empresa de la transacción)
        df_paso3 = paso_3_agregar_empresa_transaccion(
            spark=spark,
            db_curated=db_curated,
            df_paso2=df_paso2,
            enable_broadcast=args.enable_broadcast
        )
        mostrar_metricas_rendimiento(df_paso3, "Post-PASO-3")
        
        # 4. Insertar en tabla final con particionamiento
        total_insertados = insertar_tabla_final(
            spark=spark,
            db_functional=db_functional,
            df_final=df_paso3,
            table_name="TRANSACCION_ENRIQUECIDA",
            partition_col="FECHA_TRANSACCION"
        )
        
        # 5. Validar integridad del proceso
        validar_integridad_enriquecimiento(spark, db_curated, db_functional)
        
        # 6. Muestra de resultados finales
        logger.info("🔍 Muestra de datos enriquecidos:")
        spark.sql(f"SELECT * FROM {db_functional}.TRANSACCION_ENRIQUECIDA LIMIT 10").show(truncate=False)
        
        # 7. Listar particiones
        logger.info("📁 Particiones disponibles:")
        spark.sql(f"SHOW PARTITIONS {db_functional}.TRANSACCION_ENRIQUECIDA").show(truncate=False)
        
        # 8. Resumen final
        logger.info("\n🎉 Proceso de Functional completado exitosamente!")
        logger.info(f"📊 Total registros enriquecidos: {total_insertados}")
        logger.info(f"💾 Ubicación HDFS: {location_final}")
        
        # 9. Estadísticas de almacenamiento
        try:
            result = spark.sql(f"DESCRIBE FORMATTED {db_functional}.TRANSACCION_ENRIQUECIDA")
            size_row = result.filter(col("col_name") == "totalSize").select("data_type").first()
            if size_row:
                logger.info(f"📦 Tamaño total en HDFS: {size_row[0]}")
        except Exception as e:
            logger.warning(f"⚠️  No se pudo obtener tamaño: {e}")
        
    except Exception as e:
        logger.error(f"❌ Error crítico en el proceso: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Limpieza de vistas temporales
        limpiar_vistas_temporales(spark, ["df_final_enriquecido"])
        spark.stop()
        logger.info("🔚 Sesión de Spark cerrada")

if __name__ == "__main__":
    main()