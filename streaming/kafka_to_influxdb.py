#!/usr/bin/env python3
"""
Spark Structured Streaming: Kafka → Processing → InfluxDB → Grafana
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import urllib.request
from datetime import datetime

print("=" * 70)
print("🚀 KAFKA → SPARK → INFLUXDB → GRAFANA")
print("=" * 70)

# InfluxDB Configuration
INFLUXDB_HOST = "influxdb"
INFLUXDB_PORT = 8086
INFLUXDB_TOKEN = "fraud-detection-token-2024"
INFLUXDB_ORG = "fraud-detection"
INFLUXDB_BUCKET = "transactions"

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaFraudStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session created\n")

# Define transaction schema
transaction_schema = StructType([
    StructField("Time", DoubleType()),
    StructField("V1", DoubleType()),
    StructField("V2", DoubleType()),
    StructField("V3", DoubleType()),
    StructField("V4", DoubleType()),
    StructField("V5", DoubleType()),
    StructField("V6", DoubleType()),
    StructField("V7", DoubleType()),
    StructField("V8", DoubleType()),
    StructField("V9", DoubleType()),
    StructField("V10", DoubleType()),
    StructField("V11", DoubleType()),
    StructField("V12", DoubleType()),
    StructField("V13", DoubleType()),
    StructField("V14", DoubleType()),
    StructField("V15", DoubleType()),
    StructField("V16", DoubleType()),
    StructField("V17", DoubleType()),
    StructField("V18", DoubleType()),
    StructField("V19", DoubleType()),
    StructField("V20", DoubleType()),
    StructField("V21", DoubleType()),
    StructField("V22", DoubleType()),
    StructField("V23", DoubleType()),
    StructField("V24", DoubleType()),
    StructField("V25", DoubleType()),
    StructField("V26", DoubleType()),
    StructField("V27", DoubleType()),
    StructField("V28", DoubleType()),
    StructField("Amount", DoubleType()),
    StructField("Class", IntegerType()),
    StructField("timestamp", DoubleType())
])

# Read from Kafka
print("📥 Connecting to Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "credit-card-transactions") \
    .option("startingOffsets", "latest") \
    .load()

print("✅ Connected to Kafka topic: credit-card-transactions\n")

# Parse JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

# Add processing
processed_df = parsed_df \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("fraud_status", when(col("Class") == 1, "fraud").otherwise("normal"))

def write_to_influxdb(batch_df, batch_id):
    """Write batch to InfluxDB"""
    print(f"\n📊 Processing Batch {batch_id}...")
    
    rows = batch_df.collect()
    if len(rows) == 0:
        print("   Empty batch")
        return
    
    lines = []
    for row in rows:
        timestamp_ns = int(row['timestamp'] * 1e9) if row['timestamp'] else int(datetime.now().timestamp() * 1e9)
        
        line = (
            f"transactions,"
            f"type={row['fraud_status']} "
            f"amount={row['Amount']},"
            f"v1={row['V1']},"
            f"v2={row['V2']},"
            f"fraud={row['Class']} "
            f"{timestamp_ns}"
        )
        lines.append(line)
    
    data = "\n".join(lines)
    
    try:
        url = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}/api/v2/write?org={INFLUXDB_ORG}&bucket={INFLUXDB_BUCKET}&precision=ns"
        
        request = urllib.request.Request(
            url,
            data=data.encode('utf-8'),
            headers={
                'Authorization': f'Token {INFLUXDB_TOKEN}',
                'Content-Type': 'text/plain; charset=utf-8'
            },
            method='POST'
        )
        
        response = urllib.request.urlopen(request)
        
        if response.status == 204:
            fraud_count = sum(1 for r in rows if r['Class'] == 1)
            print(f"   ✅ Sent {len(rows)} transactions ({fraud_count} frauds) → InfluxDB")
        else:
            print(f"   ⚠️ Unexpected response: {response.status}")
    
    except Exception as e:
        print(f"   ❌ InfluxDB Error: {e}")

# Console output
console_query = processed_df.select(
    col("processing_time"),
    col("Amount").cast("decimal(10,2)").alias("Amount"),
    "Class",
    "fraud_status"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# InfluxDB output
influx_query = processed_df.writeStream \
    .foreachBatch(write_to_influxdb) \
    .trigger(processingTime="5 seconds") \
    .start()

print("=" * 70)
print("🟢 STREAMING ACTIVE!")
print("=" * 70)
print("📥 Source: Kafka (credit-card-transactions)")
print("⚙️  Processing: Spark Structured Streaming")
print("💾 Destination: InfluxDB")
print("📊 Visualization: Grafana (http://localhost:3000)")
print("🛑 Press Ctrl+C to stop")
print("=" * 70 + "\n")

try:
    console_query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n🛑 Stopping streaming...")
    console_query.stop()
    influx_query.stop()
    spark.stop()
    print("✅ Streaming stopped successfully")
