from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("RideSharingTask3") \
    .getOrCreate()

# Step 2: Define schema
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Step 3: Read streaming data
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 4: Parse JSON
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Step 5: Convert timestamp and add watermark
parsed_stream = parsed_stream.withColumn("event_time", to_timestamp("timestamp"))

# Step 6: Windowed aggregation (5-min window, 1-min slide)
windowed_stream = parsed_stream.withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "5 minutes", "1 minute")) \
    .agg(_sum("fare_amount").alias("sum_fare")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sum_fare")
    )

# Step 7: Write each batch to CSV
def write_window_to_csv(batch_df, batch_id):
    batch_df.coalesce(1).write.mode("append").csv(f"outputs/task3", header=True)

query = windowed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_window_to_csv) \
    .option("checkpointLocation", "/tmp/checkpoints_task3") \
    .start()

query.awaitTermination()