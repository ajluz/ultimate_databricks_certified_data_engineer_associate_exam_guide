# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Advanced Concepts in Spark Structured Streaming

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_08"

# COMMAND ----------

generate_and_write_to_volume("08")

# COMMAND ----------

stream_path = "/Volumes/workspace/default/chapter_08/api_stream_data/"

static = spark.read.json(stream_path)
schema = static.schema

df_stream = (
    spark.readStream
         .format("json")
         .schema(schema)
         .option('maxFilesPerTrigger', 1)
         .load(stream_path)
)

# COMMAND ----------

# MAGIC %md ### Tumbling Window

# COMMAND ----------

dbutils.fs.rm("/Volumes/workspace/default/chapter_08/api_stream_data/_checkpoint/tumbling_window_example",True)

from pyspark.sql.functions import window, col, date_format

checkpointLocation = "/Volumes/workspace/default/chapter_08/api_stream_data/_checkpoint/tumbling_window_example"

df_tumbling_win = (
  df_stream
    .withColumn("access_date", col("access_date").cast("date"))
    .groupBy(window("access_date", "1 day"))
    .count()
    .select(
      date_format(col("window.start"), "yyyy-MM-dd").alias("window_date"),
      col("count")
    )
    .writeStream
    .format("memory")
    .option("checkpointLocation",checkpointLocation)
    .trigger(availableNow=True)
    .outputMode("complete")
    .queryName("tumbling_win")
    .start()
)

# COMMAND ----------

import time

for i in range(3):
    print(f"--- Execution {i+1} ---")
    spark.sql("""
      SELECT * FROM tumbling_win
      ORDER BY window_date ASC LIMIT 1
    """).show()
    if i < 2:
        time.sleep(2)

# COMMAND ----------

# MAGIC %md ### Sliding Window

# COMMAND ----------

dbutils.fs.rm("/Volumes/workspace/default/chapter_08/api_stream_data/_checkpoint/sliding_window_example",True)

from pyspark.sql.functions import window, col, date_format

checkpointLocation = "/Volumes/workspace/default/chapter_08/api_stream_data/_checkpoint/sliding_window_example"

df_sliding_win = (
  df_stream
    .withColumn("access_date", col("access_date").cast("timestamp"))
    .groupBy(window("access_date", "10 minutes", "5 minutes"))
    .count()
    .writeStream
    .format("memory")
    .option("checkpointLocation",checkpointLocation)
    .trigger(availableNow=True)
    .outputMode("complete")
    .queryName("sliding_win")
    .start()
)

# COMMAND ----------

import time

for i in range(3):
    print(f"--- Execution {i+1} ---")
    spark.sql("""
      SELECT * FROM sliding_win
      ORDER BY window.start ASC LIMIT 1
    """).show(truncate=False)
    if i < 2:
        time.sleep(2)

# COMMAND ----------

# MAGIC %md ### Session Window

# COMMAND ----------

from pyspark.sql.functions import session_window

df_session_win = (
  df_stream
    .groupBy(session_window("access_date", "30 minutes"), "ip_address")
    .count()
)

# COMMAND ----------

# MAGIC %md ### Writing Data to Streaming Sinks

# COMMAND ----------

checkpointLocation = "/Volumes/workspace/default/chapter_07/_checkpoint/transformation_1"
# dbutils.fs.rm(checkpointLocation, True)

(
  df_transformed.writeStream
      .format("memory")
      .option("checkpointLocation",checkpointLocation)
      .trigger(availableNow=True)
      .outputMode("append")
      .queryName("transformation_1")
      .start()
).awaitTermination()

# COMMAND ----------

spark.sql("""
  SELECT
    access_date,
    access_point,
    ip_address
  FROM transformation_1 LIMIT 5
""").show()

# COMMAND ----------

for stream in spark.streams.active:
    print(stream.lastProgress)

# COMMAND ----------

# MAGIC %md ### Schema Inference and Evolution on Streaming

# COMMAND ----------

schemaLocation = '/Volumes/workspace/default/chapter_07/autoloader/test_1'
# dbutils.fs.rm(schemaLocation, True)

autoLoaderDf = (
    spark.readStream
         .format('CloudFiles')
         .option('cloudFiles.maxFilesPerTrigger', 1)
         .option('cloudFiles.format', 'json')
         .option('cloudFiles.schemaLocation', schemaLocation)
         .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
         .option('cloudFiles.inferColumnTypes', True)
         .load(stream_path)
)

# COMMAND ----------

cleanup_resources()
