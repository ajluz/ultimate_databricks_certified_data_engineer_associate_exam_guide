# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 10 — New Delivery
# MAGIC
# MAGIC Run by the `new_delivery` task of `chapter_10_job`: appends one more delivery file of accesses to `ip_access_data/` (never to `ip_access_app/`, which the file arrival trigger watches).

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_10"

# COMMAND ----------

deliver_ip_access_files(chapter_number="10", folder="ip_access_data", deliveries=1)
