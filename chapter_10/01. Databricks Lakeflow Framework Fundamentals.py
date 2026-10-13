# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 10 — Databricks Lakeflow Framework Fundamentals
# MAGIC
# MAGIC Companion notebook. The pipeline files live in `chapter_10/pipeline/transformations/` and are run from the Lakeflow Pipelines Editor (pipeline `chapter_10_pipeline`, catalog `workspace`, schema `default`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Knowing the Pipeline Interface

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_10"

# COMMAND ----------

generate_ip_access_files(chapter_number="10")

# COMMAND ----------

# MAGIC %md
# MAGIC Create `chapter_10_pipeline`, add `transformations/01_st_ip_access.sql`, click **Run pipeline**, then:

# COMMAND ----------

spark.sql("""
  SELECT access_point, COUNT(*) AS qty_access
  FROM st_ip_access
  GROUP BY access_point
  ORDER BY qty_access DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Add `transformations/02_mv_access_by_point.sql`, run the pipeline again, then:

# COMMAND ----------

spark.sql("""
  SELECT * FROM mv_access_by_point
  ORDER BY qty_access DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flows

# COMMAND ----------

generate_ip_access_files(chapter_number="10", folder="ip_access_app", deliveries=5)

# COMMAND ----------

# MAGIC %md
# MAGIC Add `transformations/03_st_ip_access_all.sql`, run the pipeline, then:

# COMMAND ----------

spark.sql("""
  SELECT origin, COUNT(*) AS qty_access
  FROM st_ip_access_all
  GROUP BY origin
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Data Capture with AUTO CDC

# COMMAND ----------

generate_customer_changes(chapter_number="10", delivery=0)

# COMMAND ----------

# MAGIC %md
# MAGIC Add `transformations/04_customers.sql`, run the pipeline, then:

# COMMAND ----------

spark.sql("""
  SELECT * FROM customers
  ORDER BY customer_id
""").show()

# COMMAND ----------

spark.sql("""
  SELECT * FROM customers_history
  ORDER BY customer_id, __START_AT
""").show()

# COMMAND ----------

generate_customer_changes(chapter_number="10", delivery=1)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the pipeline again, then repeat the two queries above and:

# COMMAND ----------

spark.sql("""
  SELECT customer_id, customer_name, customer_city, __START_AT
  FROM customers_history
  WHERE __END_AT IS NULL
  ORDER BY customer_id
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality with Expectations and Quarantine

# COMMAND ----------

# MAGIC %md
# MAGIC Add `transformations/05_ip_access_quality.sql`, run the pipeline, then:

# COMMAND ----------

spark.sql("""
  SELECT 'quarantine' AS tb, COUNT(*) AS qty FROM st_ip_access_quarantine
  UNION ALL
  SELECT 'clean' AS tb, COUNT(*) AS qty FROM st_ip_access_clean
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Jobs — Triggers and Schedules
# MAGIC
# MAGIC With the file arrival trigger of `chapter_10_job` pointed at `ip_access_app/`, deliver a new file:

# COMMAND ----------

deliver_ip_access_files(chapter_number="10", folder="ip_access_app")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Delete `chapter_10_job` and `chapter_10_pipeline` from **Jobs & Pipelines** first, then:

# COMMAND ----------

# cleanup_chapter_10()
