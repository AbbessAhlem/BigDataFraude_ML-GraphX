from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
import logging

# --- 1. Initialisation Spark ---
spark = SparkSession.builder \
    .appName("Fraud_HDFS_Streaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- 2. Chemins HDFS ---
HDFS_STREAM_DIR = "hdfs://namenode:8020/user/fraud_detection/streaming"
CHECKPOINT_DIR = "hdfs://namenode:8020/user/fraud_detection/checkpoints"
OUTPUT_DIR = "hdfs://namenode:8020/user/fraud_detection/alerts"

# --- 3. Chargement du modèle ML (optionnel) ---
ML_MODEL_PATH = "hdfs://namenode:8020/fraud_models/best_classifier_pipeline"
try:
    ml_model = PipelineModel.load(ML_MODEL_PATH)
    logging.info("Modèle ML chargé avec succès.")
except Exception as e:
    logging.warning(f"Impossible de charger le modèle ML: {e}")
    ml_model = None

# --- 4. Schéma des transactions ---
cols_pca = [StructField(f"V{i}", DoubleType(), True) for i in range(1,29)]
schema = StructType([StructField("Transaction_ID", StringType(), True),
                     StructField("Time", DoubleType(), True),
                     StructField("Amount", DoubleType(), True)] + cols_pca + [StructField("Class", IntegerType(), True)])

# --- 5. Lecture streaming HDFS ---
raw_stream = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .csv(HDFS_STREAM_DIR)

# --- 6. Fonction de règles simples ---
def apply_rules(df):
    THRESHOLD_AMOUNT = 2000.0
    df = df.withColumn("Is_Alert", when(col("Amount") > THRESHOLD_AMOUNT, lit(True)).otherwise(lit(False)))
    df = df.withColumn("Decision", when(col("Is_Alert"), lit("ALERT_HIGH_AMOUNT")).otherwise(lit("PASS")))
    df = df.withColumn("Event_Time", current_timestamp())
    return df

stream_processed = apply_rules(raw_stream)

# --- 7. Optionnel : appliquer le modèle ML ---
if ml_model:
    stream_processed = ml_model.transform(stream_processed)  # ajoute 'prediction', 'probability', etc.
    # Exemple : générer une alerte ML
    stream_processed = stream_processed.withColumn(
        "ML_Alert",
        when(col("prediction") == 1, lit(True)).otherwise(lit(False))
    )

# --- 8. Fusion des alertes règles + ML ---
stream_processed = stream_processed.withColumn(
    "Final_Alert",
    when((col("Is_Alert") == True) | (col("ML_Alert") == True), lit(True)).otherwise(lit(False))
)

# --- 9. Écriture du flux vers HDFS ---
query = stream_processed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_DIR) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

query.awaitTermination()
