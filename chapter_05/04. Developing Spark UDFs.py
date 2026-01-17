# Databricks notebook source
# MAGIC %md
# MAGIC # Developing Spark UDFs

# COMMAND ----------

# Creating UDF using Spark SQL
spark.sql("""
    CREATE OR REPLACE FUNCTION calculate_discount(value DECIMAL(9,2),discount DECIMAL(9,2))
    RETURNS DECIMAL(9,2)
        RETURN CAST(value * (1 - discount) AS decimal(9,2))
""")

spark.sql("DESCRIBE FUNCTION calculate_discount").show(truncate=False)

# COMMAND ----------

# Using UDF on Spark SQL
spark.sql("""
    SELECT 
        *,
        calculate_discount(unit_price, discount) AS discounted_price
    FROM order_details
""").show(5, truncate=False)
