# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %md
# MAGIC # Introduction to Spark Structured Streaming

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_07"

# COMMAND ----------

generate_and_write_to_volume("07")

# COMMAND ----------

# MAGIC %md ### Kafka Source

# COMMAND ----------

# df_stream = (
#     spark.readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", "broker:9092")
#         .option("subscribe", "orders")
#         .option("startingOffsets", "earliest")
#         .load()
# )

# COMMAND ----------

# MAGIC %md ### File Source

# COMMAND ----------

stream_path = "/Volumes/workspace/default/chapter_07/api_stream_data/"

df_stream = (
    spark.readStream
         .format("json")
         .option('maxFilesPerTrigger', 1)
         .load(stream_path)
)

# COMMAND ----------

display(
  df_stream
  # this "checkpointLocation" parameter is necessary when we execution actions on stream within Databricks Free Edition.
  ,checkpointLocation = "/Volumes/workspace/default/chapter_07/api_stream_data/_checkpoint/exemple_1"
)

# COMMAND ----------

static = spark.read.json(stream_path)
schema = static.schema

print(schema)

# COMMAND ----------

# dbutils.fs.rm("/Volumes/workspace/default/chapter_07/api_stream_data/_checkpoint/exemple_1",True)

df_stream = (
    spark.readStream
         .format("json")
         .schema(schema)
         .option('maxFilesPerTrigger', 1)
         .load(stream_path)
)

display(
  df_stream
  # this "checkpointLocation" parameter is necessary when we execution actions on stream within Databricks Free Edition.
  ,checkpointLocation = "/Volumes/workspace/default/chapter_07/api_stream_data/_checkpoint/exemple_1"
)

# COMMAND ----------

df_stream.isStreaming

# COMMAND ----------

# MAGIC %md ### Delta Table Source

# COMMAND ----------

# dbutils.fs.rm("/Volumes/workspace/default/chapter_07/api_stream_data/_checkpoint/exemple_2",True)

df_stream_delta = (
    spark.readStream
         .table("tb_api_stream_data")
)

display(
  df_stream_delta
  # this "checkpointLocation" parameter is necessary when we execution actions on stream within Databricks Free Edition.
  ,checkpointLocation = "/Volumes/workspace/default/chapter_07/api_stream_data/_checkpoint/exemple_2"
)

# COMMAND ----------

# MAGIC %md ### Transformations in Structured Streaming

# COMMAND ----------

from pyspark.sql.functions import col, sum

df_transformed = (
  (
    df_stream
      .withColumn("payment_info",col('payload').payment_info)
      .withColumn("discount",col('payment_info').discount)
      .withColumn("final_price",col('payment_info').final_price)
      .withColumn("installment_value",col('payment_info').installment_value)
      .withColumn("installments",col('payment_info').installments)
      .withColumn("payment_method",col('payment_info').payment_method)
      .where("final_price IS NOT NULL")
      .drop('payload', 'payment_info'))
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

autoLoaderDf = (
    spark.readStream
         .format('CloudFiles')
         .option('cloudFiles.maxFilesPerTrigger', 1)
         .option('cloudFiles.format', 'json')
         .option('cloudFiles.schemaLocation', '/Volumes/workspace/default/chapter_07/autoloader/test_1')
         .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
         .option('cloudFiles.inferColumnTypes', True)
         .load(stream_path)
)

# COMMAND ----------

cleanup_resources()
