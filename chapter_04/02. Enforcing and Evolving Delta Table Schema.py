# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Enforcement

# COMMAND ----------

spark.sql("DESCRIBE tb_people").show()
spark.sql("SELECT * FROM tb_people ORDER BY id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trying to Add rows with missing columns

# COMMAND ----------

df_without_one_col = spark.sql("SELECT 3 AS id, 'Ulisses Luz' AS name")
df_without_one_col.printSchema()
df_without_one_col.show()

# COMMAND ----------

(
    df_without_one_col.write
                         .mode('append')
                         .saveAsTable('tb_people')
)

# COMMAND ----------

spark.sql("SELECT * FROM tb_people ORDER BY id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trying to add rows with more columns

# COMMAND ----------

df_with_new_col = spark.sql("""
    SELECT 
        4 AS id, 
        'Nicole Gode' AS name, 
        DATE('1997-11-13') AS birth, 
        'female' AS gender
""")
df_with_new_col.printSchema()
df_with_new_col.show()

# COMMAND ----------

df_with_new_col.write \
    .mode('append') \
    .saveAsTable('tb_people')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trying to add rows with different Data Type

# COMMAND ----------

df_with_diff_schema_col = spark.sql("""
    SELECT 
        4 AS id, 
        'Humberto Luz' AS name, 
        '1959-11-03' AS birth
""")
df_with_diff_schema_col.printSchema()
df_with_diff_schema_col.show()

# COMMAND ----------

(
    df_with_diff_schema_col.write
        .mode('append')
        .saveAsTable('tb_people')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trying to add rows with uppercase Column Name

# COMMAND ----------

df_with_uppercase_col = spark.sql("""
    SELECT 
        4 AS ID, 
        'John Smith' AS NAME, 
        DATE('1985-08-23') AS BIRTH
""")
df_with_uppercase_col.printSchema()
df_with_uppercase_col.show()

# COMMAND ----------

(
    df_with_uppercase_col.write
        .mode('append')
        .saveAsTable('tb_people')
)

spark.sql("SELECT * FROM tb_people ORDER BY id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Evolution

# COMMAND ----------

# MAGIC %md ### Using Alter Table to Evolve Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tb_people 
# MAGIC   ADD COLUMNS (gender string);

# COMMAND ----------

df_with_new_col = spark.sql("""
    SELECT 
        5 AS id, 
        'Nicole Gode' AS name, 
        DATE('1997-11-13') AS birth, 
        'female' AS gender
""")

df_with_new_col.write \
    .mode('append') \
    .saveAsTable('tb_people')

spark.sql("SELECT * FROM tb_people ORDER BY id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using MergeSchema Option to Evolve Schema

# COMMAND ----------

df_with_new_col_2 = spark.sql("""
    SELECT 
        6 AS id, 
        'Valentina Gode' AS name, 
        DATE('2016-03-07') AS birth, 
        'female' AS gender,
        '123-45-67' AS ssn
""")
df_with_new_col_2.printSchema()
df_with_new_col_2.show()

# COMMAND ----------

df_with_new_col_2.write \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("tb_people")

spark.sql("SELECT * FROM tb_people ORDER BY id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### mergeSchema Option and Data Types Changes

# COMMAND ----------

df_with_diff_schema_col = spark.sql("""
    SELECT 
        7 AS id, 
        'Joselia Dantas' AS name, 
        '1962-05-18' AS birth
""")
df_with_diff_schema_col.printSchema()
df_with_diff_schema_col.show()

df_with_diff_schema_col.write \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("tb_people")