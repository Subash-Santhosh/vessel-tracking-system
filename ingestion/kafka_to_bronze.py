from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import os

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

bronze_path = "/tmp/delta/bronze/vessel_tracking"
checkpoint_path = "/tmp/delta/checkpoints/vessel_tracking"

spark = SparkSession.builder \
    .appName("vessel-tracking") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "io.delta:delta-spark_2.12:3.1.0"
    ) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

data = spark.readStream \
    .format("kafka") \
        .option("kafka.bootstrap.servers","localhost:9092") \
            .option("subscribe","vessel-tracking-system") \
                .option("startingOffsets","earliest") \
                    .load()

bronze_df = data.selectExpr("CAST(key as STRING) as key","CAST(value as STRING) as value").withColumn("createdAt",current_timestamp())

query = bronze_df.writeStream \
    .format("delta") \
        .outputMode("append") \
            .option("checkPointLocation", checkpoint_path) \
                .start(bronze_path)

query.awaitTermination()