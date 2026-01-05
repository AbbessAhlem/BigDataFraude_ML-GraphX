import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml import PipelineModel

# --- Configuration ---
APP_NAME = "FraudDetectionStreaming"
INPUT_DIR = "file:///home/hadoop/streaming_input"           # AJOUT DE file://
CHECKPOINT_LOCATION = "file:///home/hadoop/streaming_checkpoint" # AJOUT DE file://
MODEL_PATH = "file:///tmp/spark_models/fraude_gbt_final_1767574836" 


# CHEMIN FINAL CORRIGÉ ET VÉRIFIÉ DU MODÈLE
MODEL_PATH = "file:///tmp/spark_models/fraude_gbt_final_1767574836" 
# --- Schéma des Données de Transaction ---
data_schema = StructType([
    StructField("Time", DoubleType(), True),
    StructField("V1", DoubleType(), True),
    StructField("V2", DoubleType(), True),
    StructField("V3", DoubleType(), True),
    StructField("V4", DoubleType(), True),
    StructField("V5", DoubleType(), True),
    StructField("V6", DoubleType(), True),
    StructField("V7", DoubleType(), True),
    StructField("V8", DoubleType(), True),
    StructField("V9", DoubleType(), True),
    StructField("V10", DoubleType(), True),
    StructField("V11", DoubleType(), True),
    StructField("V12", DoubleType(), True),
    StructField("V13", DoubleType(), True),
    StructField("V14", DoubleType(), True),
    StructField("V15", DoubleType(), True),
    StructField("V16", DoubleType(), True),
    StructField("V17", DoubleType(), True),
    StructField("V18", DoubleType(), True),
    StructField("V19", DoubleType(), True),
    StructField("V20", DoubleType(), True),
    StructField("V21", DoubleType(), True),
    StructField("V22", DoubleType(), True),
    StructField("V23", DoubleType(), True),
    StructField("V24", DoubleType(), True),
    StructField("V25", DoubleType(), True),
    StructField("V26", DoubleType(), True),
    StructField("V27", DoubleType(), True),
    StructField("V28", DoubleType(), True),
    StructField("Amount", DoubleType(), True)
])


def start_streaming_job():
    """Démarre le job de Spark Structured Streaming pour la détection de fraude."""

    # Initialisation de la session Spark
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark Session (version {spark.version}) démarrée pour le streaming.")

    # 1. Charger le modèle ML entraîné
    try:
        # La vérification de placeholder a été retirée pour éviter l'erreur de syntaxe
        model = PipelineModel.load(MODEL_PATH) 
        print(f"Modèle ML chargé avec succès depuis {MODEL_PATH}")
    except Exception as e:
        print(f"Erreur FATALE lors du chargement du modèle. Vérifiez le chemin : {e}")
        sys.exit(1)
        
    # 2. Définir la source de streaming (Lecture de fichiers CSV entrants)
    raw_stream = spark.readStream.schema(data_schema)
    raw_stream = raw_stream.option("maxFilesPerTrigger", 1)
    raw_stream = raw_stream.option("header", "true")
    raw_stream = raw_stream.csv(INPUT_DIR)
        
    print(f"Démarrage de la lecture en streaming à partir de {INPUT_DIR}...")

    # 3. Application du modèle à la volée (le 'transform' est une opération de streaming valide)
    prediction_stream = model.transform(raw_stream)

    # 4. Préparer le résultat pour la sortie
    output_stream = prediction_stream.withColumn(
        "fraud_status", 
        when(col("prediction") == 1.0, "🔴 FRAUDE DÉTECTÉE")
        .otherwise("🟢 Légitime")
    ).select(
        col("Time"),
        col("Amount"),
        col("prediction").alias("IsFraud"),
        col("fraud_status"),
        current_timestamp().alias("processing_time")
    )
    
    # 5. Définir le Sink (destination : la console pour le monitoring)
    query = output_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    print("\n---------------------------------------------------------------------")
    print(f"Pipeline de détection de fraude en temps réel démarré, surveillant : {INPUT_DIR}")
    print("Pour simuler l'arrivée de données, copiez des lignes de votre CSV dans un nouveau fichier (.csv) dans ce répertoire.")
    print("Appuyez sur Ctrl+C pour arrêter le job de streaming.")
    print("---------------------------------------------------------------------")
    
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming_job()