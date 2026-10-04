import os
# Tip de resolución de problemas: Descarga automáticamente el conector de Kafka para Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Iniciar sesión de Spark
spark = SparkSession.builder.appName("CapaSpeed_Lambda").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # Ocultar logs innecesarios

# Definir la estructura de nuestros datos JSON
esquema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("usuario", StringType(), True),
    StructField("accion", StringType(), True),
    StructField("tiempo_respuesta_ms", IntegerType(), True)
])

print("Conectando a Kafka en tiempo real (Capa Speed)...")

# Leer desde Kafka en modo Streaming
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs-plataforma") \
    .load()

# Transformar los datos binarios a formato tabular
df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), esquema).alias("data")).select("data.*")

# Agrupar y contar las acciones de los usuarios en tiempo real
df_agrupado = df_parsed.groupBy("usuario").count()

# Mostrar resultados en la consola
query = df_agrupado.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()