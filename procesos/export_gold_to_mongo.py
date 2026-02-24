from pyspark.sql import SparkSession

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("CSV_to_MongoDB_Gold") \
    .config("spark.mongodb.connection.uri", "mongodb://192.168.0.11:27017") \
    .config("spark.mongodb.database", "medallon") \
    .config("spark.mongodb.collection", "gold") \
    .getOrCreate()

# Leer CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("file:/home/hadoop/spark-elt-medallon/datalake/temp/part-*.csv")
    #.csv("file:/home/hadoop/spark-elt-medallon/datalake/gold.csv")

# Mostrar esquema
df.printSchema()

print("========== DEBUG ==========")
print("Registros en DF:", df.count())
df.show(5, truncate=False)
print("===========================")

# Escribir en MongoDB
df.write \
  .format("mongodb") \
  .mode("overwrite") \
  .option("spark.mongodb.connection.uri", "mongodb://192.168.0.11:27017") \
  .option("spark.mongodb.database", "medallon") \
  .option("spark.mongodb.collection", "gold") \
  .save()

spark.stop()