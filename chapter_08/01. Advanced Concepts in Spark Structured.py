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
generate_and_write_to_tables("08")

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

# MAGIC %md ### Watermarks to Handle Late Data

# COMMAND ----------

df_tumbling_win = (
  df_stream
  	.withWatermark("access_date", "10 minutes")
		.groupBy(window("access_date", "10 minutes"))
		.count()
)

# COMMAND ----------

# MAGIC %md ### Stream-Static Joins

# COMMAND ----------

from pyspark.sql.functions import col

checkpointLocation = "/Volumes/workspace/default/chapter_08/api_stream_data/_checkpoint/stream_static_join"

# dbutils.fs.rm(checkpointLocation, True)

df_api_stream_data = (
  spark.readStream
		.table("tb_api_stream_data")
	)

df_products = spark.read.table("products")

df_enriched = (
  df_api_stream_data
		.withColumn("product_name", col("payload.product_info.product_name"))
	  .join(df_products, "product_name")
		.select(
     	df_api_stream_data.access_date, 
      df_api_stream_data.ip_address, 
      df_api_stream_data.access_point,
      df_products.product_id,
      df_products.product_name,
      df_products.level
    )
    .writeStream
    .format("memory")
    .option("checkpointLocation",checkpointLocation)
    .trigger(availableNow=True)
    .outputMode("append")
    .queryName("stream_static_join")
    .start()
)

# COMMAND ----------

spark.sql("""
  SELECT 
    ip_address, 
    access_point, 
    product_id, 
    product_name, 
    level
  FROM stream_static_join 
  LIMIT 6
""").show()

# COMMAND ----------

# MAGIC %md ### Stream-Stream Joins

# COMMAND ----------

spark.sql("""
	  CREATE OR REPLACE TABLE tb_api_stream_sample AS
	  SELECT *
	  FROM tb_api_stream_data
	  WHERE access_point IN ('chrome', 'firefox')
	  LIMIT 10000
""")

# COMMAND ----------

from pyspark.sql.functions import col, expr

checkpointLocation = "/Volumes/workspace/default/chapter_08/api_stream_data/_checkpoint/stream_stream_join"

# dbutils.fs.rm(checkpointLocation, True)

df_web = (
  spark.readStream
    .table("tb_api_stream_sample")
    .select(
      col("ip_address").alias("web_ip"),
      col("access_date").cast("timestamp").alias("web_time")
    )
    .withWatermark("web_time", "30 minutes")
)

df_mobile = (
  spark.readStream
    .table("tb_api_stream_sample")
    .select(
      col("ip_address").alias("mobile_ip"),
      (col("access_date").cast("timestamp") + expr("INTERVAL 15 MINUTES")).alias("mobile_time")
    )
    .withWatermark("mobile_time", "30 minutes")
)

df_joined = (
  df_web.join(
    df_mobile,
    expr("""
      web_ip = mobile_ip AND
      mobile_time BETWEEN web_time AND web_time + INTERVAL 1 HOUR
    """)
  )
  .writeStream
  .format("memory")
  .option("checkpointLocation",checkpointLocation)
  .trigger(availableNow=True)
  .outputMode("append")
  .queryName("stream_stream_join")
  .start()
)

# COMMAND ----------

spark.sql("SELECT * FROM stream_stream_join LIMIT 5").show()

# COMMAND ----------

# MAGIC %md ### Dropping Duplicates On Streaming

# COMMAND ----------

df_orders = (
	spark.readStream
	  .table("orders")
)

df_deduplicated = (
  df_orders
    .withWatermark("order_date", "10 minutes")
    .dropDuplicates(["order_id", "order_date"])
)

# COMMAND ----------

df_deduplicated = (
  df_orders
    .withWatermark("order_date", "10 minutes")
    .dropDuplicatesWithinWatermark(["order_id"])
)

# COMMAND ----------

# MAGIC %md ### Using UDFs in Streaming Pipelines

# COMMAND ----------

checkpointLocation = "/Volumes/workspace/default/chapter_08/api_stream_data/_checkpoint/udf_example"

dbutils.fs.rm(checkpointLocation, True)

df_api_stream_data = (
  spark.readStream
    .table("tb_api_stream_data")
)

df_purchases = (
  df_api_stream_data
    .where("payload.payment_info.discount IS NOT NULL")
    .selectExpr(
      "payload.product_info.product_name AS product_name",
      "CAST(replace(payload.product_info.price, '$ ', '') AS DECIMAL(9,2)) AS price",
      "payload.payment_info.discount AS discount"
    )
	)

df_discounted = (
  df_purchases
    .selectExpr("*","calculate_discount(price, discount) AS final_price")
    .writeStream
    .format("memory")
    .option("checkpointLocation",checkpointLocation)
    .trigger(availableNow=True)
    .outputMode("append")
    .queryName("udf_example")
    .start()
)

# COMMAND ----------

spark.sql("SELECT * FROM udf_example LIMIT 5").show()

# COMMAND ----------

cleanup_all_resources()
