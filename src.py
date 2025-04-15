from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hour, to_timestamp, desc, dense_rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Data Analysis") \
    .getOrCreate()

# Task 1: Load & Basic Exploration
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Create Temp View
df.createOrReplaceTempView("sensor_readings")

# Show first 5 rows
df.show(5)

# Count total number of records
total_count = df.count()
print(f"Total records: {total_count}")

# Distinct locations
df.select("location").distinct().show()

# Save first 5 rows to task1_output.csv
df.limit(5).write.csv("task1_output.csv", header=True, mode="overwrite")

# Task 2: Filtering & Aggregations

# Filter rows where temperature <18 or >30
out_of_range_df = df.filter((col("temperature") < 18) | (col("temperature") > 30))
in_range_df = df.filter((col("temperature") >= 18) & (col("temperature") <= 30))

print(f"Out-of-range count: {out_of_range_df.count()}")
print(f"In-range count: {in_range_df.count()}")

# Group by location and calculate average temperature and humidity
agg_df = df.groupBy("location").agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
).orderBy(desc("avg_temperature"))

agg_df.show()

# Save to task2_output.csv
agg_df.write.csv("task2_output.csv", header=True, mode="overwrite")

# Task 3: Time-Based Analysis

# Convert timestamp string to TimestampType
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Extract hour of day
df = df.withColumn("hour_of_day", hour("timestamp"))
df.createOrReplaceTempView("sensor_readings")  # Update temp view

# Group by hour and get average temperature
hourly_avg_df = df.groupBy("hour_of_day").agg(
    avg("temperature").alias("avg_temp")
).orderBy("hour_of_day")

hourly_avg_df.show()

# Save to task3_output.csv
hourly_avg_df.write.csv("task3_output.csv", header=True, mode="overwrite")

# Task 4: Window Function - Rank sensors by average temperature
sensor_avg_df = df.groupBy("sensor_id").agg(
    avg("temperature").alias("avg_temp")
)

windowSpec = Window.orderBy(desc("avg_temp"))
ranked_df = sensor_avg_df.withColumn("rank_temp", dense_rank().over(windowSpec))

# Show top 5
ranked_df.orderBy("rank_temp").show(5)

# Save to task4_output.csv
ranked_df.orderBy("rank_temp").write.csv("task4_output.csv", header=True, mode="overwrite")

# Task 5: Pivot by hour and location

# Reuse df with 'hour_of_day'
pivot_df = df.groupBy("location").pivot("hour_of_day").agg(avg("temperature"))

pivot_df.show()

# Save to task5_output.csv
pivot_df.write.csv("task5_output.csv", header=True, mode="overwrite")

# Find the hottest (location, hour)
from pyspark.sql.functions import array, struct, posexplode

# Convert pivot table to long format for finding max
melted = pivot_df.selectExpr("location", "stack(24, " +
    ",".join([f"'{i}', `{i}`" for i in range(24)]) +
    ") as (hour, avg_temp)")

hottest = melted.orderBy(desc("avg_temp")).limit(1)
hottest.show()

