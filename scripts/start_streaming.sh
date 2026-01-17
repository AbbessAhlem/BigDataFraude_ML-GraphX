#!/bin/bash
# Script de démarrage du Spark Streaming

echo "=========================================="
echo "   DÉMARRAGE SPARK STREAMING FRAUD"
echo "=========================================="

# Variables
APP_DIR="/app/streaming"
CONFIG_FILE="$APP_DIR/streaming_config.yaml"
MASTER="spark://spark-master:7077"
JARS_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

echo "📁 Répertoire de l'application: $APP_DIR"
echo "⚙️  Fichier de configuration: $CONFIG_FILE"
echo "🎯 Spark Master: $MASTER"

# Vérifier que le fichier de configuration existe
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Fichier de configuration non trouvé: $CONFIG_FILE"
    exit 1
fi

echo "🚀 Lancement de l'application Spark Streaming..."

# Lancer l'application Spark
spark-submit \
  --master $MASTER \
  --packages $JARS_PACKAGES \
  --conf "spark.executor.memory=2g" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.sql.shuffle.partitions=2" \
  --conf "spark.streaming.backpressure.enabled=true" \
  --conf "spark.streaming.kafka.maxRatePerPartition=100" \
  --conf "spark.sql.streaming.checkpointLocation=/tmp/streaming_checkpoint" \
  $APP_DIR/spark_streaming_app.py \
  --config $CONFIG_FILE

# Vérifier le code de sortie
if [ $? -eq 0 ]; then
    echo "✅ Application Spark Streaming terminée avec succès"
else
    echo "❌ Erreur dans l'application Spark Streaming"
    exit 1
fi