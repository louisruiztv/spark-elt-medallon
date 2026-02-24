#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Workload
Adaptado desde Hive SQL original
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Workload')
    parser.add_argument('--env', type=str, default='TopicosB', help='Entorno: DEV, QA, PROD')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument('--local_data_path', type=str, default='file:/home/hadoop/spark-elt-medallon/dataset', help='Ruta local de datos')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession con soporte Hive
# =============================================================================

def create_spark_session(app_name="Proceso_Carga_Workload-LlanosBardales"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .config("spark.sql.legacy.charVarcharCodegen", "true") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares
# =============================================================================

def crear_database(spark, env, username, base_path):
    """Crea la base de datos si no existe"""
    db_name = f"{env}_workload"
    db_location = f"{base_path}/{username}/datalake/{db_name}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def leer_archivo_csv(spark, ruta_local, encoding='iso-8859-1'):
    """
    Lee archivo CSV con delimitador pipe y encoding específico
    Nota: El encoding iso-8859-1 requiere Spark 3.0+
    """
    return spark.read.csv(
        ruta_local,
        sep='|',
        header=True,
        encoding=encoding,
        inferSchema=False,
        allStrings=True  # Mantiene todos los campos como String (como en el original)
    )

def crear_tabla_external(spark, db_name, table_name, df, location, spark_schema):
    """
    Crea tabla externa en Hive con formato TEXTFILE y propiedades específicas
    """
    # Registrar DataFrame como vista temporal
    df.createOrReplaceTempView(f"tmp_{table_name}")
    
    # Crear tabla externa usando SQL para mayor control de propiedades
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        {', '.join([f'{field.name} STRING' for field in spark_schema.fields])}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\\n'
    STORED AS TEXTFILE
    LOCATION '{location}'
    TBLPROPERTIES(
        'skip.header.line.count'='1',
        'store.charset'='ISO-8859-1',
        'retrieve.charset'='ISO-8859-1'
    )
    """
    spark.sql(create_table_sql)
    
    # Insertar datos desde la vista temporal
    spark.sql(f"""
        INSERT OVERWRITE TABLE {db_name}.{table_name}
        SELECT * FROM tmp_{table_name}
    """)
    
    print(f"✅ Tabla '{db_name}.{table_name}' desplegada en: {location}")

# =============================================================================
# @section 4. Definición de esquemas (explícitos para control total)
# =============================================================================

SCHEMAS = {
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
        StructField("FECHA", StringType(), True)
    ])
}

# =============================================================================
# @section 5. Proceso principal de carga
# =============================================================================

def procesar_tabla(spark, args, db_name, table_name, archivo_datos, esquema):
    """Procesa una tabla completa: lectura, creación y carga"""
    
    # Rutas
    ruta_local = f"{args.local_data_path}/{archivo_datos}"
    ruta_hdfs = f"{args.base_path}/{args.username}/datalake/{db_name}/{table_name.lower()}"
    
    print(f"📥 Procesando tabla: {table_name} | Archivo: {archivo_datos}")
    
    # Leer datos con esquema explícito
    df = spark.read.csv(
        ruta_local,
        schema=esquema,
        sep='|',
        header=True,
        encoding='iso-8859-1',
        nullValue='\\N',
        emptyValue=''
    )
    
    # Crear tabla externa y cargar datos
    crear_tabla_external(spark, db_name, table_name, df, ruta_hdfs, esquema)
    
    # Validación: mostrar primeras filas
    print(f"🔍 Muestra de datos - {table_name}:")
    spark.sql(f"SELECT * FROM {db_name}.{table_name} LIMIT 10").show(truncate=False)
    print("-" * 80)

# =============================================================================
# @section 6. Ejecución principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        # 1. Crear database
        db_name = crear_database(spark, args.env, args.username, args.base_path)
        
        # 2. Definir tablas a procesar
        tablas_config = [
            {"nombre": "PERSONA", "archivo": "persona.data", "schema": SCHEMAS["PERSONA"]},
            {"nombre": "EMPRESA", "archivo": "empresa.data", "schema": SCHEMAS["EMPRESA"]},
            {"nombre": "TRANSACCION", "archivo": "transacciones.data", "schema": SCHEMAS["TRANSACCION"]}
        ]
        
        # 3. Procesar cada tabla
        for config in tablas_config:
            procesar_tabla(
                spark=spark,
                args=args,
                db_name=db_name,
                table_name=config["nombre"],
                archivo_datos=config["archivo"],
                esquema=config["schema"]
            )
        
        # 4. Resumen final
        print("\n🎉 Proceso completado exitosamente!")
        print(f"📊 Tablas disponibles en {db_name}:")
        spark.sql(f"SHOW TABLES IN {db_name}").show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error en el proceso: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()