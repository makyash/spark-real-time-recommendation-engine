from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("kafkaStreaming")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

# Spark doesn’t allow actions like collect() on streaming DataFrames, 
# so I separate schema inference using batch reads and then apply it to the streaming pipeline.
# instead on spark.readStream, use spark.read to get the schema 

sample_df = spark.read\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_events") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# extract sample JSON
sample_json = sample_df.selectExpr("CAST(value AS STRING)").limit(1).collect()[0][0]

# infer schema 
schema = spark.read.json(spark.sparkContext.parallelize([sample_json])).schema

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_events") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# simulate users
with_user = parsed.withColumn("user_id", expr("hash(symbol, timestamp) % 1000")) 

# convert timestamp *very important 
with_user = with_user.withColumn("event_time",to_timestamp(col("timestamp")))

# waatermark - to handle late data
with_watermark = with_user.withWatermark("event_time","10 minutes")

# window aggregation (REAL MAGIC)
features = with_watermark\
            .groupBy( window(col("event_time"), "5 minutes"),col("user_id"))\
            .agg(
                count("*").alias("trade_count_5min"),
                avg(col("price").cast("double")).alias("avg_price_5min")
            ) 

query =features.writeStream\
        .outputMode("update")\
        .format("console")\
        .option("Truncate",False)\
        .start()

query.awaitTermination()