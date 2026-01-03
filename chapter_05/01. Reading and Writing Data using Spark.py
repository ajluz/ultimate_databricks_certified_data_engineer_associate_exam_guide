# Databricks notebook source
# MAGIC %run "./setup/setup_chapter_05"

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading and Writing Data Using Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data Files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Json Files

# COMMAND ----------

# JSON 
spark.sql("SELECT * FROM json.`/Volumes/workspace/default/chapter_05/users/json/`").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Csv Files

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

# MAGIC %md
# MAGIC #### Reading Other File Types

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

spark.sql("SELECT * FROM binaryFile.`/Volumes/workspace/default/chapter_05/binary/db_diagram.png`").display()

# COMMAND ----------

# JDBC
# spark.sql("""
#     CREATE OR REPLACE TEMPORARY VIEW products_jdbc
#     USING JDBC
#         OPTIONS (
#             url "jdbc:postgresql://hostname:5432/database",
#             dbtable "products",
#             user "username",
#             password "password"
#     )
# """)

# spark.sql("SELECT * FROM products_jdbc")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to a Table

# COMMAND ----------

print('starting')

# COMMAND ----------

def cleanup():
    tables = [
        "workspace.default.users",
        "workspace.default.orders",
        "workspace.default.order_details",
        "workspace.default.products"
    ]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing to a Delta Table

# COMMAND ----------

# Create table using CTAS
spark.sql("""
    CREATE TABLE IF NOT EXISTS order_details
    USING delta
    AS 
        SELECT * 
        FROM parquet.`/Volumes/workspace/default/chapter_05/order_details/parquet/`
""")

# COMMAND ----------

spark.sql("SELECT * FROM order_details").show(5, truncate=False)

# COMMAND ----------

# Delta is the default option for creating on Databricks
spark.sql("""
    CREATE TABLE IF NOT EXISTS users
    -- USING delta
    AS 
        SELECT * 
        FROM json.`/Volumes/workspace/default/chapter_05/users/json/`
""")

spark.sql("SELECT * FROM users").show(5, truncate=False)

spark.sql("DESCRIBE DETAIL users").select("format").show()

# COMMAND ----------

# Create a table from a CSV file
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW vw_orders
    USING CSV
    OPTIONS (
        path '/Volumes/workspace/default/chapter_05/orders/csv/',
        header 'true',
        sep ','
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS orders
    AS 
        SELECT * FROM vw_orders
""")

spark.sql("SELECT * FROM orders").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing to a Iceberg Table

# COMMAND ----------

# Creating Iceberg Table
spark.sql("""
    CREATE TABLE IF NOT EXISTS products
    USING iceberg
    AS 
        SELECT * 
        FROM avro.`/Volumes/workspace/default/chapter_05/products/avro/`
""")

spark.sql("SELECT * FROM products").show(5, truncate=False)

spark.sql("DESCRIBE DETAIL products").select("format").show()

# COMMAND ----------

spark.sql("""
    INSERT INTO products (product_id, product_name, base_price) 
    VALUES (
        6, 'Book Club - Ultimate Databricks Certified Data Engineer Associate Exam Guide', 297.0
    )
""")

spark.sql("SELECT COUNT(*) rows_qty FROM products").show(truncate=False)

spark.sql("SELECT * FROM products WHERE product_id = 6").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Overwriting Data within a Table

# COMMAND ----------

# overwriting data using INSERT OVERWRITE
spark.sql("""
    INSERT OVERWRITE products
    SELECT * FROM avro.`/Volumes/workspace/default/chapter_05/products/avro/`
""")

spark.sql("SELECT COUNT(*) rows_qty FROM products").show(truncate=False)

spark.sql("DESCRIBE HISTORY products").select("version", "operation", "operationParameters.mode").show(truncate=False)

# COMMAND ----------

# overwriting data using CREATE OR REPLACE
spark.sql("""
    CREATE OR REPLACE TABLE products
    USING iceberg
        SELECT * FROM avro.`/Volumes/workspace/default/chapter_05/products/avro/`
""")

spark.sql("SELECT COUNT(*) rows_qty FROM products").show(truncate=False)

spark.sql("DESCRIBE HISTORY products").select("version", "operation", "operationParameters.mode").show(truncate=False)

# COMMAND ----------


