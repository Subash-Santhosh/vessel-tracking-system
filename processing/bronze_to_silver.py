from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, create_map, lit, split, substring, when, length, broadcast, filter, abs, radians, sin, cos, sqrt, atan2, row_number, concat_ws
from pyspark.sql.window import Window
from itertools import chain
from pyspark.sql.types import *
import os

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

bronze_path = "/tmp/delta/bronze/vessel_tracking"
checkpoint_path = "/tmp/delta/checkpoints/vessel_tracking"
silver_path = "/tmp/delta/silver/vessel_tracking"

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

bronze_df = spark.readStream \
    .format("delta") \
        .load(bronze_path)

schema = StructType([
    StructField("imo",IntegerType(),True),
    StructField("vesselName", StringType(), True),
    StructField("position",StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("course", DoubleType(), True),
                StructField("speed", DoubleType(), True),
                StructField("navigationalStatus", IntegerType(), True),
                StructField("timeStamp", StringType(), True)
    ]), True),
    StructField("destinationFromAIS", StringType(), True),
    StructField("etaFromAIS", StringType(), True),
    StructField("destination", StringType(), True)
])

parsed_df = bronze_df.select(
    from_json(col("value"), schema).alias("data")).select("data.*")

df = parsed_df.select(col("imo"), col("vesselName"), col("position.latitude").alias("latitude"), col("position.longitude").alias("longitude"),
        col("position.course").alias("course"),col("position.speed").alias("speed"),col("position.navigationalStatus").alias("navigationalStatus"),
        col("position.timeStamp").alias("timeStamp"), col("destinationFromAIS"), col("etaFromAIS").alias("ETA"), col("destination"))

navigation_status = {
    0: "Under way using engine",
    1: "At anchor",
    2: "Not under command",
    3: "Restricted manoeuverability",
    4: "Constrained by her draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in Fishing",
    8: "Under way sailing",
    9: "Reserved for future amendment of Navigational Status for HSC",
    10: "Reserved for future amendment of Navigational Status for WIG",
    11: "Reserved for future use",
    12: "Reserved for future use",
    13: "Reserved for future use",
    14: "AIS-SART is active",
    15: "Not defined (default)",
}

mapping_expr = create_map([lit(i) for i in chain(*navigation_status.items())])

df = df.withColumn("navigationalStatus",mapping_expr[col("navigationalStatus")])

df = df.withColumn("clean_code",split(col("destinationFromAIS")," > ").getItem(0))

df = df.withColumn("country",split(col("clean_code"), " ").getItem(0))

df = df.withColumn("locode",split(col("clean_code"), " ").getItem(1))

df = df.withColumn("origin_country", col("country"))

df = df.withColumn("country", 
    when(col("locode").isNull() & (length(col("origin_country")) == 5),
        substring(col("origin_country"), 1,2))
        .otherwise(col("country"))
        )
df = df.withColumn("locode",
    when(col("locode").isNull() & (length(col("origin_country")) == 5),
        substring(col("origin_country"), 3,3))
        .otherwise(col("locode"))
        )

df = df.drop("origin_country")

look_up = spark.read.csv("/Users/VS Code/Python Projects/Forms UI/Pyspark/code-list.csv", header=True, inferSchema=True)

look_up = look_up.select(col("Country").alias("country"),col("Location").alias("locode"),col("Name").alias("destination_name"))

df = df.join(look_up,["country", "locode"], "left")

df = df.select(col("imo"),col("vesselName"),col("latitude"),col("longitude"),col("course"),col("speed"),col("navigationalStatus"),col("timeStamp"),col("ETA"),col("destination_name"))

port_df = spark.read.option("multiline", "true").json("/Users/VS Code/Python Projects/Forms UI/Pyspark/ports.json")

port_df = port_df.select(col("LATITUDE").alias("port_latitude"),col("LONGITUDE").alias("port_longitude"),col("CITY").alias("city"),col("COUNTRY").alias("current_country"))

def enrich_ports(df, batch_id):
    df = df.crossJoin(broadcast(port_df)) \
        .filter(
        (abs(col("latitude") - col("port_latitude")) < 1) &
        (abs(col("longitude") - col("port_longitude")) < 1)
    )

    R = 6371  # km

    df = df.withColumn("dlat", radians(col("port_latitude") - col("latitude"))) \
        .withColumn("dlon", radians(col("port_longitude") - col("longitude"))) \
        .withColumn("a",
            sin(col("dlat")/2)**2 +
            cos(radians(col("latitude"))) *
            cos(radians(col("port_latitude"))) *
            sin(col("dlon")/2)**2
        ) \
        .withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1-col("a")))) \
        .withColumn("distance_km", col("c") * R)

    w = Window.partitionBy("imo").orderBy("distance_km")

    df = df.withColumn("rank", row_number().over(w)) \
        .filter(col("rank") == 1) \
            .drop("rank")

    df = df.drop("dlat", "dlon", "a", "c")

    df = df.withColumn("current_port", concat_ws(" - ",col("city"), col("current_country"))).drop("current_country","city","port_latitude","port_longitude")

    df = df.withColumn("current_port", 
        when(col("navigationalStatus") == "Under way using engine", lit(None))
        .otherwise(col("current_port"))
    )

    df = df.drop("distance_km")

    df = df.withColumnRenamed("destination_name", "destination")

    df.write \
        .format("delta") \
        .mode("append") \
        .save(silver_path)


query = df.writeStream \
    .foreachBatch(enrich_ports) \
    .start()

query.awaitTermination()