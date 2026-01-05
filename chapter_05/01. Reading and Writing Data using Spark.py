# Databricks notebook source
# MAGIC %run "./setup/setup_chapter_05"

# COMMAND ----------

generate_and_write_to_volume(chapter_number="05")

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

spark.sql("SELECT * FROM binaryFile.`/Volumes/workspace/default/chapter_05/binary/db_diagram.png`").show()

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

# def cleanup():
#     tables = [
#         "workspace.default.users",
#         "workspace.default.orders",
#         "workspace.default.order_details",
#         "workspace.default.products",
#         # "workspace.default.products_external",
#         # "workspace.default.users_external"
#     ]
#     for table in tables:
#         spark.sql(f"DROP TABLE IF EXISTS {table}")

#     # dbutils.fs.rm("abfss://dev@dataslightadlsgen2.dfs.core.windows.net/book/", recurse=True)

# cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing to a Delta Table

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

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
    INSERT INTO products (product_id, product_name, base_price, level) 
    VALUES (
        6, 'Book Club - Ultimate Databricks Certified Data Engineer Associate Exam Guide', 297.90, 'intermediate'
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

# MAGIC %md
# MAGIC #### Understanding External Tables

# COMMAND ----------

# # Creating External Table
# spark.sql("""
#     CREATE TABLE IF NOT EXISTS users_external
#     LOCATION 'abfss://dev@dataslightadlsgen2.dfs.core.windows.net/book/certified_data_engieer/users' -- Adding location
#     AS 
#         SELECT * 
#         FROM parquet.`/Volumes/workspace/default/chapter_05/users/parquet/`
# """)

# spark.sql("SELECT * FROM users_external").show(5, truncate=False)

# COMMAND ----------

# spark.sql("""
#     DESCRIBE EXTENDED users_external
# """).select("col_name", "data_type") \
#     .filter("col_name IN ('Location', 'Type')") \
#     .show(truncate=False)

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS users_external")

# display(dbutils.fs.ls("abfss://dev@dataslightadlsgen2.dfs.core.windows.net/book/certified_data_engieer/users"))

# COMMAND ----------

# dbutils.fs.rm("abfss://dev@dataslightadlsgen2.dfs.core.windows.net/book/certified_data_engieer/users", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting External to Managed Tables

# COMMAND ----------

# spark.sql("""
#     CREATE TABLE IF NOT EXISTS products_external
#     LOCATION 'abfss://dev@dataslightadlsgen2.dfs.core.windows.net/book/certified_data_engieer/products' -- Adding location
#     AS 
#         SELECT * 
#         FROM parquet.`/Volumes/workspace/default/chapter_05/products/parquet/`
# """)

# spark.sql("SELECT * FROM products_external").show(5, truncate=False)

# COMMAND ----------

# # Just on serverless or all purpose 17.0+
# spark.sql("ALTER TABLE products_external SET MANAGED")

# COMMAND ----------

# spark.sql("""
#     DESCRIBE EXTENDED products_external
# """).select("col_name", "data_type") \
#     .filter("col_name IN ('Location', 'Type')") \
#     .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cloning Tables Using CLONE Statement

# COMMAND ----------

# Deep Clone
spark.sql("""
    CREATE TABLE IF NOT EXISTS deep_clone_users
    CLONE users
""")

spark.sql("SELECT * FROM deep_clone_users").show(5, truncate=False)

# COMMAND ----------

# Shallow Clone
spark.sql("""
    CREATE TABLE IF NOT EXISTS shallow_clone_users
    SHALLOW CLONE users
""")

spark.sql("SELECT * FROM shallow_clone_users").show(5, truncate=False)

# COMMAND ----------

spark.sql("""
    UPDATE shallow_clone_users 
    SET profession = 'Data Engineer'
    WHERE user_id = 4
""")

print("Result for users table:")
spark.sql("SELECT * FROM users WHERE user_id = 4").show(truncate=False)

print("\nResult for shallow_clone_users table:")
spark.sql("SELECT * FROM shallow_clone_users WHERE user_id = 4").show(truncate=False)

# COMMAND ----------

spark.sql("""
    UPDATE users 
    SET profession = 'Developer'
    WHERE user_id = 7
""")

print("Result for users table:")
spark.sql("SELECT * FROM users WHERE user_id = 7").show(truncate=False)

print("\nResult for shallow_clone_users table:")
spark.sql("SELECT * FROM shallow_clone_users WHERE user_id = 7").show(truncate=False)

# COMMAND ----------

drop_cloned_tables()
