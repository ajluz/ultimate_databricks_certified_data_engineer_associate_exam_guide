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

    view_name = f"ip_updates_{batch_id}"

    df_batch.createOrReplaceTempView(view_name)

    spark.sql(f"""
        MERGE INTO tb_ip_address AS target
        USING {view_name} AS source
        ON target.ip_address = source.ip_address
        WHEN NOT MATCHED THEN
        INSERT (ip_address, access_date, access_point, load_date)
        VALUES (
            source.ip_address, 
            source.access_date, 
            source.access_point, 
            current_timestamp()
        )
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

# dbutils.fs.rm("/Volumes/workspace/default/chapter_09/_schema/tb_api_data_autoloader", True)
# dbutils.fs.rm("/Volumes/workspace/default/chapter_09/_checkpoint/tb_api_data_autoloader", True)

# spark.sql("""
#   DROP TABLE IF EXISTS tb_api_data_autoloader
# """)

checkpointLocation = "/Volumes/workspace/default/chapter_09/_checkpoint/tb_api_data_autoloader"
schemaLocation = "/Volumes/workspace/default/chapter_09/_schema/tb_api_data_autoloader"

stream_path = "/Volumes/workspace/default/chapter_09/ip_access_data/"

autoloader_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", schemaLocation)
       .option("cloudFiles.schemaEvolutionMode", "none")
       .option("cloudFiles.inferColumnTypes", True)
       .load(stream_path)
       .writeStream
       .option("checkpointLocation", checkpointLocation)
       .trigger(availableNow=True)
       .toTable("tb_api_data_autoloader")
).awaitTermination()

spark.sql("""
  SELECT * FROM tb_api_data_autoloader
""").show(5)

# COMMAND ----------

df = spark.createDataFrame(dbutils.fs.ls(f"{schemaLocation}/_schemas/"))
df.select("path").show(truncate=False)

# COMMAND ----------

import json
from pyspark.sql.functions import col, replace
from pyspark.sql.types import StructType

schemas_text = (
    spark.read.text(f"{schemaLocation}/_schemas/0")
              .filter(col("value")!= "v1")
              .collect()[0]["value"]
)

schemas_json = json.loads(schemas_text)

schema = StructType.fromJson(
    json.loads(schemas_json["dataSchemaJson"])
)

print(schema)

# COMMAND ----------

generate_ip_access_files("09", type_of_column='string')

# COMMAND ----------

(spark.read.json(stream_path)).show(5)

# COMMAND ----------

autoloader_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", schemaLocation)
       .option("cloudFiles.schemaEvolutionMode", "none")
       .option("cloudFiles.inferColumnTypes", True)
       .load(stream_path)
       .writeStream
       .option("checkpointLocation", checkpointLocation)
       .trigger(availableNow=True)
       .toTable("tb_api_data_autoloader")
).awaitTermination()

spark.sql("""
  SELECT * FROM tb_api_data_autoloader
""").show(5)

# COMMAND ----------

generate_ip_access_files("09", type_of_column='string')

# COMMAND ----------

autoloader_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", schemaLocation)
       .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
       .option("cloudFiles.inferColumnTypes", True)
       .load(stream_path)
       .writeStream
       .option("checkpointLocation", checkpointLocation)
       .option("mergeSchema", "true")
       .trigger(availableNow=True)
       .toTable("tb_api_data_autoloader")
).awaitTermination()

# COMMAND ----------

autoloader_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", schemaLocation)
       .option("cloudFiles.schemaEvolutionMode", "rescue")
       .option("cloudFiles.inferColumnTypes", True)
       .load(stream_path)
       .writeStream
       .option("checkpointLocation", checkpointLocation)
       .trigger(availableNow=True)
       .option("mergeSchema", "true")
       .toTable("tb_api_data_autoloader")
).awaitTermination()

spark.sql("""
  SELECT t1.ip_address, t1._rescued_data
  FROM tb_api_data_autoloader AS t1
  WHERE t1._rescued_data IS NOT NULL
""").show(5, False)

# COMMAND ----------

generate_ip_access_files("09", type_of_column='string')

# COMMAND ----------

autoloader_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", schemaLocation)
       .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
       .option("cloudFiles.inferColumnTypes", True)
       .load(stream_path)
       .writeStream
       .option("checkpointLocation", checkpointLocation)
       .trigger(availableNow=True)
       .option("mergeSchema", "true")
       .toTable("tb_api_data_autoloader")
).awaitTermination()

# COMMAND ----------

df = spark.createDataFrame(dbutils.fs.ls(f"{schemaLocation}/_schemas/"))
df.select("path").show(truncate=False)

# COMMAND ----------

schemas_text = (
    spark.read.text(f"{schemaLocation}/_schemas/1")
              .filter(col("value")!= "v1")
              .collect()[0]["value"]
)

schemas_json = json.loads(schemas_text)

schema = StructType.fromJson(
    json.loads(schemas_json["dataSchemaJson"])
)

print(schema)

# COMMAND ----------

autoloader_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", schemaLocation)
       .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
       .option("cloudFiles.inferColumnTypes", True)
       .load(stream_path)
       .writeStream
       .option("checkpointLocation", checkpointLocation)
       .trigger(availableNow=True)
       .option("mergeSchema", "true")
       .toTable("tb_api_data_autoloader")
).awaitTermination()

spark.sql("""
  SELECT * FROM tb_api_data_autoloader AS t1
  WHERE t1.col_string IS NOT NULL
""").show(5, False)

# COMMAND ----------

generate_ip_access_files("09", type_of_column='dict')

# COMMAND ----------

autoloader_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", schemaLocation)
       .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
       .option("cloudFiles.inferColumnTypes", True)
       .load(stream_path)
       .writeStream
       .option("checkpointLocation", checkpointLocation)
       .trigger(availableNow=True)
       .option("mergeSchema", "true")
       .toTable("tb_api_data_autoloader")
).awaitTermination()

# COMMAND ----------

df = spark.createDataFrame(dbutils.fs.ls(f"{schemaLocation}/_schemas/"))
df.select("path").show(truncate=False)

schemas_text = (
    spark.read.text(f"{schemaLocation}/_schemas/2")
              .filter(col("value")!= "v1")
              .collect()[0]["value"]
)

schemas_json = json.loads(schemas_text)

schema = StructType.fromJson(
    json.loads(schemas_json["dataSchemaJson"])
)

print(schema)

# COMMAND ----------

autoloader_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", schemaLocation)
       .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
       .option("cloudFiles.inferColumnTypes", True)
       .load(stream_path)
       .writeStream
       .option("checkpointLocation", checkpointLocation)
       .trigger(availableNow=True)
       .option("mergeSchema", "true")
       .toTable("tb_api_data_autoloader")
).awaitTermination()

spark.sql("""
  SELECT t1.ip_address, t1.col_dict
  FROM tb_api_data_autoloader AS t1
  WHERE t1.col_dict IS NOT NULL
""").show(5, False)

# COMMAND ----------

# MAGIC %md ### Change Data Feed and CDC Patterns

# COMMAND ----------

spark.sql("""
	DROP TABLE IF EXISTS tb_customers
""")

spark.sql("""
	CREATE TABLE tb_customers (
    customer_id INT,
    customer_name STRING,
    customer_city STRING
	)
	TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

spark.sql("""
	INSERT INTO tb_customers VALUES
    (1, 'Ana', 'Lisbon'),
    (2, 'Bruno', 'Porto'),
    (3, 'Carla', 'Braga')
	""")

# spark.sql("""
# 	UPDATE tb_customers
# 	SET customer_city = 'Faro'
# 	WHERE customer_id = 2
# """)

# spark.sql("""
#   DELETE FROM tb_customers
# 	WHERE customer_id = 3
# """)

# COMMAND ----------

spark.sql("""
  SELECT * FROM table_changes('tb_customers', 1)
  ORDER BY _commit_version
""").show()

# COMMAND ----------

df_changes = (
  spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("tb_customers")
)

df_changes.orderBy("_commit_version").show()

# COMMAND ----------

df_stream_changes = (
  spark.readStream
       .option("readChangeFeed", "true")
       .table("tb_customers")
)

# COMMAND ----------

cleanup_all_resources()
