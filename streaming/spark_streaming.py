#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

print("🚀 Starting Fraud Detection Streaming (Fixed Ambiguity Mode)...")

# Initialize Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("FraudStreamKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Clean Schema - Using DoubleType for Class to prevent NULL parsing errors
schema = StructType([
    StructField("Amount", DoubleType()),
    StructField("Class", DoubleType()), 
    StructField("timestamp", DoubleType())
])

# 2. Read from Kafka using internal Docker network
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "credit-card-transactions") \
    .load()

# 3. Parse JSON and fix NULL/Case issues
# coalesce(col("Class"), lit(0)) ensures we never have a NULL status
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("valid_class", coalesce(col("Class"), lit(0)))

# 4. Add Fraud Logic - Maps numerical Class to 'fraud'/'normal' tags for Grafana
output = df_parsed.withColumn("current_time", current_timestamp()) \
    .withColumn("type", when(col("valid_class") >= 1, "fraud").otherwise("normal")) \
    .withColumn("fraud_flag", when(col("valid_class") >= 1, 1).otherwise(0))

# 5. Function to send data to InfluxDB using the 'transactions' bucket
def send_to_influx(batch_df, batch_id):
    # Ensure URL uses service name 'influxdb' for Docker communication
    client = InfluxDBClient(
        url="http://influxdb:8086", 
        token="fraud-detection-token-2024", 
        org="fraud-detection"
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    rows = batch_df.collect()
    for row in rows:
        point = Point("transactions") \
            .tag("type", row["type"]) \
            .field("amount", float(row["Amount"])) \
            .field("fraud", int(row["fraud_flag"]))
        
        # Writing specifically to the 'transactions' bucket
        write_api.write(bucket="transactions", record=point)
    client.close()

# 6. SINK A: Console (For Real-time Debugging of NULLs)
console_query = output.select("current_time", "Amount", "valid_class", "type").writeStream \
    .outputMode("append").format("console").start()

# 7. SINK B: InfluxDB
influx_query = output.writeStream \
    .foreachBatch(send_to_influx) \
    .start()

print("🟢 ACTIVE! Watch the console for 'fraud' labels...")
spark.streams.awaitAnyTermination()