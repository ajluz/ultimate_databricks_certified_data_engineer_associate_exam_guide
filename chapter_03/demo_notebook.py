# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Hello World' AS col

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC
# MAGIC This is regular text. **This is bold text.** *This is italic text.*
# MAGIC
# MAGIC Below is a list:
# MAGIC - Item 1
# MAGIC - Item 2
# MAGIC - Item 3
# MAGIC
# MAGIC This is an embedded link: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">What is Markdown?</a>
# MAGIC
# MAGIC Below is a table:
# MAGIC | Name   | Age |
# MAGIC |--------|-----|
# MAGIC | Arthur | 34  |
# MAGIC

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
