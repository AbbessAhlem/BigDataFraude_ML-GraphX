from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, rand, when

spark = SparkSession.builder \
    .appName("SparkToPostgres") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

# Générer des données fake (simulation streaming)
df = spark.range(0, 50) \
    .withColumn("ts", current_timestamp()) \
    .withColumn("amount", rand() * 1000) \
    .withColumn("location",
        when(rand() > 0.7, "US")
        .when(rand() > 0.4, "FR")
        .otherwise("TN")
    ) \
    .withColumn("fraud_probability", rand()) \
    .withColumn("is_fraud", rand() > 0.85) \
    .select("ts", "amount", "location", "is_fraud", "fraud_probability")

df.show(5)

# Écriture PostgreSQL
df.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://postgres:5432/fraud_db?ssl=false") \
  .option("dbtable", "transactions") \
  .option("user", "fraud_user") \
  .option("password", "fraud_pass") \
  .option("driver", "org.postgresql.Driver") \
  .mode("append") \
  .save()
