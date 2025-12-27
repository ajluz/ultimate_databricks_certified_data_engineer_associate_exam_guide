# Databricks notebook source
# MAGIC %md
# MAGIC ### Using VACUUM

# COMMAND ----------

spark.sql("""
    ALTER TABLE tb_people
        SET TBLPROPERTIES (
            delta.deletedFileRetentionDuration = 'interval 0 days'
        )
""")

# COMMAND ----------

print(
    "Number of files that will be removed from Storage:",
    len(spark.sql("VACUUM tb_people RETAIN 0 HOURS DRY RUN").collect())
    )

# COMMAND ----------

spark.sql("VACUUM tb_people RETAIN 0 HOURS")

# COMMAND ----------

(
    spark.sql("DESCRIBE HISTORY tb_people")
         .select("version", "operation", "operationMetrics")
         .where("operation LIKE '%VACUUM%'")
).show(truncate=False)

# COMMAND ----------

spark.sql("SELECT * FROM tb_people VERSION AS OF 1").show()