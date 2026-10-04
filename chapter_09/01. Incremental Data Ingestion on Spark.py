# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Incremental Data Ingestion on Spark

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_09"

# COMMAND ----------

generate_and_write_to_volume("09")

# COMMAND ----------

# MAGIC %md ### Insert-Only MERGE for Stream Deduplication

# COMMAND ----------

stream_path = "/Volumes/workspace/default/chapter_09/ip_access_data/"

schema = spark.read.json(stream_path).schema

df_stream_ips = (
    spark.readStream
         .format("json")
         .schema(schema)
         .option('maxFilesPerTrigger', 1)
         .load(stream_path)
)

# COMMAND ----------

spark.read.json("/Volumes/workspace/default/chapter_09/ip_access_data/").createOrReplaceTempView("ips")

spark.sql("""
  SELECT
    ip_address,
    COUNT(*) AS qty_duplicated_ips
  FROM ips
  GROUP BY ip_address
  HAVING COUNT(*) > 1
  ORDER BY qty_duplicated_ips DESC
""").show(5)

# COMMAND ----------

# dbutils.fs.rm("/Volumes/workspace/default/chapter_09/ip_access_data/_checkpoint/example_stream_deduplication_1",True)

from pyspark.sql.functions import window, col, date_format

checkpointLocation = "/Volumes/workspace/default/chapter_09/ip_access_data/_checkpoint/example_stream_deduplication_1"

deduplication_at_read = (
  df_stream_ips
    .withColumn("access_date", col("access_date").cast("timestamp"))
    .withWatermark("access_date", "10 seconds")
    .dropDuplicatesWithinWatermark(["ip_address"])
    .writeStream
    .format("memory")
    .option("checkpointLocation",checkpointLocation)
    .trigger(availableNow=True)
    .outputMode("append")
    .queryName("drop_duplicates_on_read")
    .start()
)

# COMMAND ----------

spark.sql("""
  SELECT
    ip_address,
    COUNT(*) AS qty_duplicated_ips
  FROM drop_duplicates_on_read
  GROUP BY ip_address
  HAVING COUNT(*) > 1
  ORDER BY qty_duplicated_ips
""").show(5)

# COMMAND ----------

spark.sql("""
	CREATE OR REPLACE TABLE tb_ip_address (
	  ip_address STRING,
	  access_date TIMESTAMP,
	  access_point STRING,
	  load_date TIMESTAMP
	)
""")

# COMMAND ----------

from pyspark.sql.functions import col

def insert_new_ips(df_batch, batch_id):
  df_batch.createOrReplaceTempView("ip_updates")

	spark.sql("""
		MERGE INTO tb_ip_address AS target
		USING ip_updates AS source
		ON target.ip_address = source.ip_address
		WHEN NOT MATCHED THEN
		INSERT (ip_address, access_date, access_point, load_date)
		VALUES (source.ip_address, source.access_date, source.access_point, current_timestamp())
	""")

# COMMAND ----------

# dbutils.fs.rm("/Volumes/workspace/default/chapter_09/ip_access_data/_checkpoint/example_stream_deduplication_2",True)

from pyspark.sql.functions import window, col, date_format

checkpointLocation = "/Volumes/workspace/default/chapter_09/ip_access_data/_checkpoint/example_stream_deduplication_2"

deduplication_at_read = (
  df_stream_ips
    .withColumn("access_date", col("access_date").cast("timestamp"))
    .withWatermark("access_date", "10 seconds")
    .dropDuplicatesWithinWatermark(["ip_address"])
    .writeStream
    .foreachBatch(insert_new_ips)
    .option("checkpointLocation",checkpointLocation)
    .trigger(availableNow=True)
    .outputMode("append")
    .queryName("drop_duplicates_on_read")
    .start()
)

# COMMAND ----------

spark.sql("""
  SELECT
    ip_address,
    COUNT(*) AS qty_duplicated_ips
  FROM tb_ip_address
  GROUP BY ip_address
  HAVING COUNT(*) > 1
  ORDER BY qty_duplicated_ips
""").show(5)

# COMMAND ----------

# MAGIC %md ### Using Structured Streaming with AvailableNow

# COMMAND ----------

# dbutils.fs.rm("/Volumes/workspace/default/chapter_09/_checkpoint/tb_api_stream_data",True)

checkpointLocation = "/Volumes/workspace/default/chapter_09/_checkpoint/tb_api_stream_data"

stream_path = "/Volumes/workspace/default/chapter_09/api_stream_data"

schema = spark.read.json(stream_path).schema

(
    spark.readStream
        .format("json")
        .schema(schema)
        .load(stream_path)
        .writeStream
        .option("checkpointLocation", checkpointLocation)
        .trigger(availableNow=True)
        .toTable("tb_api_stream_data")
).awaitTermination()

# COMMAND ----------

spark.sql("""
  SELECT * FROM tb_api_stream_data
""").show(5)

# COMMAND ----------

# MAGIC %md ### Using COPY INTO Statement

# COMMAND ----------

spark.sql("""
	CREATE TABLE IF NOT EXISTS tb_api_data_copy
""")

# COMMAND ----------

spark.sql("""
	COPY INTO tb_api_data_copy
	FROM '/Volumes/workspace/default/chapter_09/api_stream_data/'
	FILEFORMAT = JSON
	FORMAT_OPTIONS ('inferSchema' = 'true')
	COPY_OPTIONS ('mergeSchema' = 'true')
""")

spark.sql("""
  SELECT * FROM tb_api_data_copy
""").show(5)

# COMMAND ----------

# MAGIC %md ### Auto Loader for Streaming File Ingestion

# COMMAND ----------

checkpointLocation = "/Volumes/workspace/default/chapter_09/_checkpoint/tb_api_data_autoloader"
schemaLocation = "/Volumes/workspace/default/chapter_09/_schema/tb_api_data_autoloader"

(
  spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", schemaLocation)
      .option("cloudFiles.inferColumnTypes", True)
      .load(stream_path)
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .trigger(availableNow=True)
      .toTable("tb_api_data_autoloader")
).awaitTermination()

# COMMAND ----------

cleanup_all_resources()
