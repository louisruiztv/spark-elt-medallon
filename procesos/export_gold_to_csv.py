from pyspark.sql import SparkSession

# Crear sesión Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Export-Gold-To-CSV") \
    .enableHiveSupport() \
    .getOrCreate()

# Cambia por tu base y tabla
database = "topicosb_functional"
table = "transaccion_enriquecida"

# Leer tabla Hive
df = spark.table(f"{database}.{table}")

# Ruta dentro de tu proyecto (WSL)
output_path = "file:/home/hadoop/spark-elt-medallon/datalake/temp"

# Guardar como CSV
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(output_path)

print("Exportación completada")

spark.stop()