from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, floor, rand, lit, when
from pyspark.ml import PipelineModel
import time

# ---------------------------
# Configuration Spark
# ---------------------------
spark = SparkSession.builder \
    .appName("FraudStreamingFull") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Chemins HDFS
# ---------------------------
predictions_path = "hdfs://namenode:8020/data/predictions"
model_path = "hdfs://namenode:8020/data/models/fraude_gbt_final_1768303385"
graphx_path = "hdfs://namenode:8020/data/graphx"

# ---------------------------
# Charger le modèle
# ---------------------------
model = PipelineModel.load(model_path)

# ---------------------------
# Configuration PostgreSQL
# ---------------------------
jdbc_url = "jdbc:postgresql://postgres:5432/fraud_db?ssl=false"
db_table = "transactions"
db_properties = {
    "user": "fraud_user",
    "password": "fraud_pass",
    "driver": "org.postgresql.Driver"
}

# Liste de commerçants simulés pour PageRank
merchants = ["Amazon", "Ebay", "Walmart", "Carrefour", "Fnac", "AliExpress"]

# Intervalle de streaming simulé
interval_sec = 10

# ---------------------------
# Boucle de streaming simulé
# ---------------------------
while True:
    # Lire les nouvelles transactions
    df = spark.read.parquet(predictions_path)

    # Prédiction avec le modèle
    df_pred = model.transform(df) \
        .withColumn("predicted_fraud", col("prediction").cast("integer")) \
        .withColumn("fraud_probability", col("probability")[1]) \
        .withColumn("amount", col("features")[29]) \
        .withColumn("ts", from_unixtime(col("features")[0])) \
        .withColumn("merchant", merchants[floor(rand() * len(merchants))])

    # Calcul métriques simples pour monitoring
    df_metrics = df_pred.withColumn(
        "accuracy",
        when(col("predicted_fraud") == col("label"), 1).otherwise(0)
    ).select("ts", "amount", "label", "predicted_fraud", "fraud_probability", "merchant", "accuracy")

    # Écriture dans PostgreSQL
    df_metrics.write.jdbc(
        url=jdbc_url,
        table=db_table,
        mode="append",
        properties=db_properties
    )

    print(f"✅ {df_metrics.count()} lignes envoyées vers PostgreSQL")

    # ---------------------------
    # PageRank pour commerçants les plus à risque (top N)
    # ---------------------------
    from graphframes import GraphFrame

    # Créer graph simple : nodes = merchants, edges = transactions frauduleuses
    vertices = df_pred.select(col("merchant").alias("id")).distinct()
    edges = df_pred.filter(col("predicted_fraud") == 1) \
        .select(col("merchant").alias("src"), col("merchant").alias("dst"), col("amount"))

    g = GraphFrame(vertices, edges)
    pr = g.pageRank(resetProbability=0.15, maxIter=5)
    pr_vertices = pr.vertices.select(col("id").alias("merchant"), col("pagerank").alias("PR_Score"))

    # Joindre avec montants frauduleux
    top_merchants = pr_vertices.join(
        df_pred.filter(col("predicted_fraud") == 1)
            .groupBy("merchant")
            .sum("amount")
            .withColumnRenamed("sum(amount)", "total_amount"),
        on="merchant"
    ).orderBy(col("PR_Score").desc()).limit(5)

    # Écrire top merchants dans PostgreSQL (table top_merchants)
    top_merchants.write.jdbc(
        url=jdbc_url,
        table="top_merchants",
        mode="overwrite",
        properties=db_properties
    )

    print("✅ Top 5 commerçants à risque mis à jour")

    # Pause avant la prochaine itération
    time.sleep(interval_sec)
