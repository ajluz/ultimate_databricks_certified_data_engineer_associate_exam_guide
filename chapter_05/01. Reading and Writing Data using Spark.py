# Databricks notebook source
# MAGIC %run "./setup/setup_chapter_05"

# COMMAND ----------

# spark.read.parquet("/Volumes/workspace/default/chapter_05/products/parquet/").createOrReplaceTempView('products')
# spark.read.parquet("/Volumes/workspace/default/chapter_05/users/parquet/").createOrReplaceTempView('users')
# spark.read.parquet("/Volumes/workspace/default/chapter_05/orders/parquet/").createOrReplaceTempView('orders')
# spark.read.parquet("/Volumes/workspace/default/chapter_05/order_details/parquet/").createOrReplaceTempView('order_details')


# COMMAND ----------

# MAGIC %md
# MAGIC # Reading and Writing Data Using Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data Files

# COMMAND ----------

# JSON 
spark.sql("SELECT * FROM json.`/Volumes/workspace/default/chapter_05/users/json/`").show(5)

# COMMAND ----------

# CSV without options 
spark.sql("SELECT * FROM csv.`/Volumes/workspace/default/chapter_05/products/csv/`").show(5, truncate=False)

# COMMAND ----------

# CSV with options 
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW products_csv
    USING CSV
    OPTIONS (
        path '/Volumes/workspace/default/chapter_05/products/csv/',
        header 'true',
        sep ','
)
""")

spark.sql("SELECT * FROM products_csv").show(5, truncate=False)

# COMMAND ----------

# parquet
spark.sql("SELECT * FROM parquet.`/Volumes/workspace/default/chapter_05/orders/parquet/`").show(5)

# orc
spark.sql("SELECT * FROM orc.`/Volumes/workspace/default/chapter_05/orders/orc/`").show(5)

# avro
spark.sql("SELECT * FROM avro.`/Volumes/workspace/default/chapter_05/orders/avro/`").show(5)

# COMMAND ----------

# text
spark.sql("SELECT * FROM text.`/Volumes/workspace/default/chapter_05/log/text/`").show(5, truncate=False)

# COMMAND ----------

# text
spark.sql("SELECT * FROM text.`/Volumes/workspace/default/chapter_05/products/csv/`").show(5, truncate=False)

# COMMAND ----------

source_path = "file:././00_data_files/00_data_files/db_diagram.png"
display(spark.read.format("binaryFile").load(source_path))
