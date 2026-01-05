import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml import PipelineModel

# --- Configuration Générale ---
APP_NAME = "FraudDetectionStreamingKafka"
CHECKPOINT_LOCATION = "file:///home/hadoop/streaming_checkpoint_kafka" 
MODEL_PATH = "file:///tmp/spark_models/fraude_gbt_final_1767574836" 

# --- NOUVELLE Configuration Kafka ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  
KAFKA_TOPIC = "transactions_raw"            
# -------------------------------------

# --- Schéma des Données de Transaction ---
data_schema = StructType([
    StructField("Time", DoubleType(), True), StructField("V1", DoubleType(), True), StructField("V2", DoubleType(), True), 
    StructField("V3", DoubleType(), True), StructField("V4", DoubleType(), True), StructField("V5", DoubleType(), True), 
    StructField("V6", DoubleType(), True), StructField("V7", DoubleType(), True), StructField("V8", DoubleType(), True), 
    StructField("V9", DoubleType(), True), StructField("V10", DoubleType(), True), StructField("V11", DoubleType(), True), 
    StructField("V12", DoubleType(), True), StructField("V13", DoubleType(), True), StructField("V14", DoubleType(), True), 
    StructField("V15", DoubleType(), True), StructField("V16", DoubleType(), True), StructField("V17", DoubleType(), True), 
    StructField("V18", DoubleType(), True), StructField("V19", DoubleType(), True), StructField("V20", DoubleType(), True), 
    StructField("V21", DoubleType(), True), StructField("V22", DoubleType(), True), StructField("V23", DoubleType(), True), 
    StructField("V24", DoubleType(), True), StructField("V25", DoubleType(), True), StructField("V26", DoubleType(), True), 
    StructField("V27", DoubleType(), True), StructField("V28", DoubleType(), True), StructField("Amount", DoubleType(), True)
])


def start_streaming_job():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark Session (version {spark.version}) démarrée pour le streaming Kafka.")

    try:
        model = PipelineModel.load(MODEL_PATH) 
        print(f"Modèle ML chargé avec succès depuis {MODEL_PATH}")
    except Exception as e:
        print(f"Erreur FATALE lors du chargement du modèle. Vérifiez le chemin : {e}")
        sys.exit(1)
        
    # --- Démarrage de la lecture depuis KAFKA ---
    kafka_stream_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    print(f"Démarrage de la lecture en streaming à partir du Topic Kafka : {KAFKA_TOPIC}...")

    # Désérialisation du message Kafka (JSON)
    kafka_stream_string = kafka_stream_raw.selectExpr("CAST(value AS STRING) as json_payload")
    raw_stream = kafka_stream_string.select(from_json(col("json_payload"), data_schema).alias("data")) \
        .select("data.*")

    # Application du modèle 
    prediction_stream = model.transform(raw_stream)

    # Préparation du résultat
    output_stream = prediction_stream.withColumn(
        "fraud_status", 
        when(col("prediction") == 1.0, "🔴 FRAUDE DÉTECTÉE (KAFKA)")
        .otherwise("🟢 Légitime (KAFKA)")
    ).select(
        col("Time"), col("Amount"), col("prediction").alias("IsFraud"), col("fraud_status"),
        current_timestamp().alias("processing_time")
    )
    
    # Définir le Sink
    query = output_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime="1 second") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    print("\n---------------------------------------------------------------------")
    print(f"Pipeline de détection de fraude en temps réel (KAFKA) démarré.")
    print(f"Surveillance du Topic : {KAFKA_TOPIC}")
    print("Utilisez un producteur Kafka pour envoyer des messages JSON.")
    print("---------------------------------------------------------------------")
    
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming_job()
