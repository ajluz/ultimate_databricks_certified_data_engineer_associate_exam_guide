# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Hello World' AS col

# COMMAND ----------



# COMMAND ----------

# MAGIC %run ./setup_notebook

# COMMAND ----------

print(test_variable)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

print('fs help:')
dbutils.fs.help()

print('notebook help:')
dbutils.notebook.help()

print('widgets help:')
dbutils.widgets.help()

# COMMAND ----------

available_datasets = dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

display(available_datasets)