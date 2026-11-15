import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as F

# --- 1. Configuración de Spark ---
spark = (SparkSession.builder
    .appName("AgroStreamingConsumer")
    .master("local[2]")
    # Dependencias para conectar con Kafka y PostgreSQL
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4")
    .config("spark.driver.memory", "2g")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# --- 2. Lectura del stream desde Kafka ---
kafka_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "agro_raw")
    .option("startingOffsets", "earliest") # Leer desde el principio para la demo
    .load())

# --- 3. Parseo del mensaje ---
# Los datos vienen en formato CSV (timestamp, valor)
# Ejemplo: "2015-02-20 12:00,-1.8,"
parsed_stream = (kafka_stream
    .selectExpr("CAST(value AS STRING) as csv_data")
    .withColumn("split_data", F.split(F.col("csv_data"), ","))
    .select(
        F.to_timestamp(F.col("split_data")[0], "yyyy-MM-dd HH:mm").alias("timestamp"),
        F.col("split_data")[1].cast("double").alias("valor")
    )
    .where(F.col("timestamp").isNotNull() & F.col("valor").isNotNull()))

# --- 4. Función para escribir en PostgreSQL ---
def write_to_postgres(batch_df, batch_id):
    print(f"--- Procesando micro-lote {batch_id} ---")
    try:
        (batch_df.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/agro")
            .option("dbtable", "streaming_humedad")
            .option("user", "agro")
            .option("password", "agro")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save())
        print(f"--- Lote {batch_id} guardado en PostgreSQL ---")
    except Exception as e:
        print(f"Error en lote {batch_id}: {e}")

# --- 5. Inicio del stream ---
query = (parsed_stream.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgres)
    .trigger(processingTime="10 seconds") # Procesa cada 10 segundos
    .start())

print("Consumidor de streaming iniciado. Esperando datos de Kafka...")
query.awaitTermination()