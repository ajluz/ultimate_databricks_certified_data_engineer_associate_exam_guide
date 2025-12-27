# Databricks notebook source
# MAGIC %md
# MAGIC ### Handling Low Cardinality Filters

# COMMAND ----------

df = spark.read.load('/databricks-datasets/learning-spark-v2/people/people-10m.delta')
df.write.option('maxRecordsPerFile', 1000).saveAsTable('default.tb_people_2')

# COMMAND ----------

spark.sql("""
    SELECT COUNT(*) AS amount
    FROM tb_people_2
    WHERE gender = 'F'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Partitioning

# COMMAND ----------

(
    df.write
      .option('maxRecordsPerFile', 1000)
      .partitionBy("gender")
      .saveAsTable('tb_people_2_partitioned')
)

# COMMAND ----------

spark.sql("""
    DESCRIBE DETAIL tb_people_2_partitioned
""").select(
    'partitionColumns'
    ).show(truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT COUNT(*) AS amount
    FROM tb_people_2_partitioned
    WHERE gender = 'F'
""").show()

# COMMAND ----------

spark.sql("""
    SELECT count(*) AS amount
    FROM tb_people_2_partitioned
    WHERE salary BETWEEN 110000 AND 145000
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Small Files Problem

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY tb_people_2
""").select(
    'operationMetrics'
    ).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using OPTIMIZE

# COMMAND ----------

# Note: Changing target file size to aproximately 2MB only for demonstrations; in production, larger targets (e.g., 128–512 MB) usually deliver better scan performance and fewer files.
spark.sql("""
    ALTER TABLE tb_people_2
    SET TBLPROPERTIES ('delta.targetFileSize' = 2097152)
""")

spark.sql("OPTIMIZE tb_people_2")

spark.sql("""
    DESCRIBE HISTORY tb_people_2
""").select(
    'operationMetrics.numRemovedFiles',
    'operationMetrics.numAddedFiles'
    ).where(
        "version = 2"
    ).show(truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT COUNT(*) AS amount
    FROM tb_people_2
    WHERE gender = 'F'
""").show()

# COMMAND ----------

spark.sql("""
    SELECT COUNT(*) AS amount
    FROM tb_people_2
    WHERE salary BETWEEN 110000 AND 145000
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling High Cardinality Filters by using Z-Ordering

# COMMAND ----------

spark.sql("""
    OPTIMIZE tb_people_2
    ZORDER BY (id)
""")

spark.sql("""
    DESCRIBE HISTORY tb_people_2
""").select(
    'operationParameters.zOrderBy',
    'operationMetrics.numRemovedFiles',
    'operationMetrics.numAddedFiles'
    ).where(
        "version = 3"
    ).show(truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT id, firstName, gender, ssn, salary
    FROM tb_people_2
    WHERE id = 99999
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Liquid Clustering

# COMMAND ----------

spark.sql("""
    ALTER TABLE tb_people_2
    CLUSTER BY (gender, salary, id)
""")

spark.sql("""
    OPTIMIZE tb_people_2 FULL
""")

spark.sql("""
    DESCRIBE DETAIL tb_people_2
""").select(
    "clusteringColumns"
).show()
