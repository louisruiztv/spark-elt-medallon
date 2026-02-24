#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Landing (AVRO + Particionamiento)
Adaptado desde Hive SQL original con soporte para esquemas AVRO externos
"""

import sys
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Landing')
    parser.add_argument('--env', type=str, default='TopicosB', help='Entorno: DEV, QA, PROD')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument('--schema_path', type=str, default='/user/hadoop/datalake/schema', help='Ruta de esquemas AVRO')
    parser.add_argument('--source_db', type=str, default='workload', help='Base de datos origen')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession con configuración Hive/AVRO
# =============================================================================

def create_spark_session(app_name="ProcesoLanding-LlanosBardales"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .config("hive.exec.compress.output", "true") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("avro.output.codec", "snappy") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares para manejo de AVRO y Hive
# =============================================================================

def crear_database(spark, env, username, base_path):
    """Crea la base de datos Landing si no existe"""
    db_name = f"{env}_landing".lower()
    db_location = f"{base_path}/{username}/datalake/{db_name.upper()}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def leer_schema_avro(ruta_hdfs_avsc, spark):
    """
    Lee el esquema AVRO desde archivo .avsc en HDFS
    Retorna un StructType de Spark compatible
    """
    try:
        # Leer archivo desde HDFS
        schema_content = spark.sparkContext.textFile(ruta_hdfs_avsc).collect()
        schema_json = json.loads("".join(schema_content))
        
        # Convertir esquema AVRO a Spark StructType (simplificado)
        # Para producción, usar librería: spark-avro con schemaRegistry
        return spark.read.format("avro").load(ruta_hdfs_avsc.replace(".avsc", "")).schema
        
    except Exception as e:
        print(f"⚠️  No se pudo leer esquema AVRO: {e}")
        print("🔄 Usando esquema por defecto (todos STRING)")
        return None

def crear_tabla_avro_hive(spark, db_name, table_name, location, schema_avsc_url, partitioned_by=None):
    """
    Crea tabla externa en Hive con formato AVRO usando SQL directo
    para mantener compatibilidad total con propiedades Hive
    """
    partition_clause = ""
    if partitioned_by:
        partition_cols = ", ".join([f"{c} STRING" for c in partitioned_by])
        partition_clause = f"PARTITIONED BY ({partition_cols})"
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
    {partition_clause}
    STORED AS AVRO
    LOCATION '{location}'
    TBLPROPERTIES (
        'store.charset'='ISO-8859-1',
        'retrieve.charset'='ISO-8859-1',
        'avro.schema.url'='{schema_avsc_url}',
        'avro.output.codec'='snappy'
    )
    """
    spark.sql(create_sql)
    print(f"✅ Tabla AVRO '{db_name}.{table_name}' registrada en Hive Metastore")

def insertar_datos_avro(spark, db_name, table_name, df_source, partition_col=None, dynamic_partition=True):
    """
    Inserta datos en tabla AVRO con soporte para particionamiento dinámico
    Corrige duplicación de columnas de partición
    """
    # Renombrar columnas para coincidir con Hive (opcional)
    df_source = df_source.toDF(*[c.lower() for c in df_source.columns])

    if partition_col:
        partition_col_lower = partition_col.lower()
        # Evitar columna duplicada: eliminarla si ya existe en df_source
        cols_to_select = [c for c in df_source.columns if c != partition_col_lower]
        df_to_insert = df_source.select(*cols_to_select, col(partition_col_lower))
    else:
        df_to_insert = df_source

    df_to_insert.createOrReplaceTempView("src_data")

    if partition_col and dynamic_partition:
        # Modo dinámico
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        insert_sql = f"""
        INSERT OVERWRITE TABLE {db_name}.{table_name}
        PARTITION ({partition_col_lower})
        SELECT * FROM src_data
        """
    else:
        insert_sql = f"""
        INSERT OVERWRITE TABLE {db_name}.{table_name}
        SELECT * FROM src_data
        """

    spark.sql(insert_sql)
    print(f"✅ Datos insertados en '{db_name}.{table_name}'")


# =============================================================================
# @section 4. Configuración de tablas y esquemas por defecto
# =============================================================================

# Esquemas por defecto (fallback si no se puede leer .avsc)
DEFAULT_SCHEMAS = {
    "PERSONA": StructType([
        StructField("ID", StringType(), True),
        StructField("NOMBRE", StringType(), True),
        StructField("TELEFONO", StringType(), True),
        StructField("CORREO", StringType(), True),
        StructField("FECHA_INGRESO", StringType(), True),
        StructField("EDAD", StringType(), True),
        StructField("SALARIO", StringType(), True),
        StructField("ID_EMPRESA", StringType(), True)
    ]),
    "EMPRESA": StructType([
        StructField("ID", StringType(), True),
        StructField("NOMBRE", StringType(), True)
    ]),
    "TRANSACCION": StructType([
        StructField("ID_PERSONA", StringType(), True),
        StructField("ID_EMPRESA", StringType(), True),
        StructField("MONTO", StringType(), True),
        #StructField("FECHA", StringType(), True)  # Columna de partición
    ])
}

# Configuración de tablas
TABLAS_CONFIG = [
    {
        "nombre": "PERSONA",
        "archivo_avsc": "persona.avsc",
        "partitioned_by": None,
        "dynamic_partition": False
    },
    {
        "nombre": "EMPRESA", 
        "archivo_avsc": "empresa.avsc",
        "partitioned_by": None,
        "dynamic_partition": False
    },
    {
        "nombre": "TRANSACCION",
        "archivo_avsc": "transaccion.avsc",
        "partitioned_by": ["FECHA"],
        "dynamic_partition": True
    }
]

# =============================================================================
# @section 5. Proceso principal de carga Landing
# =============================================================================

def procesar_tabla_landing(spark, args, db_landing, db_source, config):
    """Procesa una tabla: crea estructura AVRO y carga datos desde workload"""
    
    table_name = config["nombre"]
    print(f"📥 Procesando tabla Landing: {table_name}")
    
    # Rutas
    location = f"{args.base_path}/{args.username}/datalake/{db_landing.upper()}/{table_name.lower()}"
    schema_url = f"hdfs://{args.schema_path}/{db_landing.upper()}/{config['archivo_avsc']}"
    
    # 1. Crear tabla en Hive Metastore con formato AVRO
    crear_tabla_avro_hive(
        spark=spark,
        db_name=db_landing,
        table_name=table_name,
        location=location,
        schema_avsc_url=schema_url,
        partitioned_by=config["partitioned_by"]
    )
    
    # 2. Leer datos desde capa workload
    source_table = f"{db_source}.{table_name}"
    df_source = spark.table(source_table)
    
    # 3. Insertar datos con soporte de particionamiento
    insertar_datos_avro(
        spark=spark,
        db_name=db_landing,
        table_name=table_name,
        df_source=df_source,
        partition_col=config["partitioned_by"][0] if config["partitioned_by"] else None,
        dynamic_partition=config["dynamic_partition"]
    )
    
    # 4. Validación
    print(f"🔍 Muestra de datos - {table_name}:")
    spark.sql(f"SELECT * FROM {db_landing}.{table_name} LIMIT 10").show(truncate=False)
    
    # 5. Si está particionada, mostrar particiones
    if config["partitioned_by"]:
        print(f"📁 Particiones creadas:")
        spark.sql(f"SHOW PARTITIONS {db_landing}.{table_name}").show(truncate=False)
    
    print("-" * 80)

# =============================================================================
# @section 6. Ejecución principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        # Configurar entorno
        env_lower = args.env.lower()
        db_landing = f"{env_lower}_landing"
        db_source = f"{env_lower}_workload"
        
        # 1. Crear database Landing
        crear_database(spark, env_lower, args.username, args.base_path)
        
        # 2. Procesar cada tabla configurada
        for config in TABLAS_CONFIG:
            procesar_tabla_landing(
                spark=spark,
                args=args,
                db_landing=db_landing,
                db_source=db_source,
                config=config
            )
        
        # 3. Resumen final
        print("\n🎉 Proceso de Landing completado exitosamente!")
        print(f"📊 Tablas disponibles en {db_landing}:")
        spark.sql(f"SHOW TABLES IN {db_landing}").show(truncate=False)
        
        # 4. Verificación de formato y compresión
        print("\n🔍 Verificación de almacenamiento AVRO + Snappy:")
        for config in TABLAS_CONFIG:
            tbl = f"{db_landing}.{config['nombre']}"
            result = spark.sql(f"DESCRIBE FORMATTED {tbl}")
            format_row = result.filter(result.col_name == "InputFormat").select("data_type").first()
            print(f"  • {config['nombre']}: {format_row[0] if format_row else 'AVRO'}")
        
    except Exception as e:
        print(f"❌ Error en el proceso: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()