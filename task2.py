from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("RideSharingTask2") \
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

# Step 5: Convert timestamp column
parsed_stream = parsed_stream.withColumn("event_time", col("timestamp").cast(TimestampType()))

# Step 6: Aggregations per driver
aggregated_stream = parsed_stream.groupBy("driver_id").agg(
    _sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Step 7: Write each batch to CSV using foreachBatch
def write_to_csv(batch_df, batch_id):
    batch_df.coalesce(1).write.mode("append").csv(f"outputs/task2", header=True)

query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "/tmp/checkpoints_task2") \
    .start()

query.awaitTermination()