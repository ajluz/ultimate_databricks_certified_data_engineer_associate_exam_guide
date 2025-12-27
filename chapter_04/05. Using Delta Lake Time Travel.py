# Databricks notebook source
# MAGIC %md
# MAGIC # Using Delta Lake Time Travel

# COMMAND ----------

spark.sql("SELECT * FROM tb_people").show(truncate=False)

# COMMAND ----------

(
    spark.sql("DESCRIBE HISTORY tb_people")
         .select("version", "timestamp", "userId", "operation")
).show(13)

# COMMAND ----------

spark.sql("SELECT * FROM tb_people VERSION AS OF 10").show()

# COMMAND ----------

spark.sql("""
        RESTORE TABLE tb_people 
            TO VERSION AS OF 10
""")

spark.sql("SELECT * FROM tb_people").show()

# COMMAND ----------

(
    spark.sql("DESCRIBE HISTORY tb_people")
         .select("version", "timestamp", "userId", "operation")
).show(1)