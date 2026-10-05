import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("CapaBatch_Lambda").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

esquema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("usuario", StringType(), True),
    StructField("accion", StringType(), True),
    StructField("tiempo_respuesta_ms", IntegerType(), True)
])

print("Leyendo todos los datos acumulados en Kafka...")

# Leer desde Kafka en modo Batch (sin readStream)
df_batch = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs-plataforma") \
    .load()

df_parsed = df_batch.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), esquema).alias("data")).select("data.*")

# Generar reporte: Conteo de logs por usuario
reporte_csv = df_parsed.groupBy("usuario").count().orderBy(col("count").desc())

# Mostrar un resumen en consola
print("Vista previa del reporte Batch:")
reporte_csv.show()

# Guardar en una carpeta CSV (Capa de Servicio)
ruta_salida = "reporte_batch_usuarios"
print(f"Guardando reporte en la carpeta: {ruta_salida}...")
reporte_csv.write.mode("overwrite").csv(ruta_salida, header=True)
print("¡Reporte generado exitosamente!")