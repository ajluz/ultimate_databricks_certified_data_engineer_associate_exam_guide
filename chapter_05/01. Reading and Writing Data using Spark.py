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
# MAGIC #### Creating Foreign Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CAST(product_id AS INT),
# MAGIC     product_name,
# MAGIC     CAST(base_price AS DOUBLE)
# MAGIC FROM csv.`/Volumes/workspace/default/chapter_05/products/csv/`

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS foreign_products (
        product_id INT,
        product_name STRING,
        base_price DOUBLE
    )
    USING CSV
    OPTIONS (
        path = '/Volumes/workspace/default/chapter_05/products/csv/',
        header = 'true',
        sep = ','
    )
""")

# spark.sql("SELECT * FROM workspace.default.foreign_products").show(5, truncate=False)

# COMMAND ----------

def add_new_csv_file():
    csv_dir = "/Volumes/workspace/default/chapter_05/products/csv"
    csv_path = f"{csv_dir}/product_02.csv"
    os.makedirs(csv_dir, exist_ok=True)
    with open(csv_path, "w") as f:
        f.write("product_id,product_name,base_price\n")
        f.write("5,Book Club - Ultimate Databricks Certified Data Engineer Associate Exam Guide,297.0")

# COMMAND ----------


