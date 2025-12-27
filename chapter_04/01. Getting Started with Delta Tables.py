# Databricks notebook source
# MAGIC %md
# MAGIC # Creating a Delta Table

# COMMAND ----------

spark.sql("""
    CREATE TABLE tb_people (
        id INT, 
        name STRING, 
        birth DATE
    ) USING DELTA
""")

# COMMAND ----------

spark.sql('SHOW TABLES').show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Describing a Delta Table

# COMMAND ----------

spark.sql('DESCRIBE tb_people').show()

# COMMAND ----------

spark.sql('DESCRIBE DETAIL tb_people').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DML Commands on Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using INSERT Statement

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tb_people (id, name, birth) 
# MAGIC VALUES (1, 'Arthur', '1990-01-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tb_people (id, name, birth) 
# MAGIC VALUES 
# MAGIC   (2, 'Maria', DATE('1995-05-15')),
# MAGIC   (3, 'John',  DATE('1988-09-30'));

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tb_people (id, name, birth) 
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   firstName,
# MAGIC   birthDate
# MAGIC FROM delta.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta`
# MAGIC WHERE id > 3;

# COMMAND ----------

spark.sql('SELECT * FROM tb_people ORDER BY id').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using UPDATE Statement

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE tb_people 
# MAGIC SET birth = '1991-01-07' 
# MAGIC WHERE id = 1;

# COMMAND ----------

spark.sql('SELECT * FROM tb_people WHERE id = 1').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using DELETE Statement

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM tb_people WHERE id = 2;

# COMMAND ----------

spark.sql('SELECT * FROM tb_people WHERE id = 2').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using MERGE Statement

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tb_people_2 
# MAGIC   AS 
# MAGIC SELECT * FROM tb_people LIMIT 0;
# MAGIC
# MAGIC INSERT INTO tb_people_2 (id, name, birth) 
# MAGIC VALUES
# MAGIC (1, "Arthur Luz", "1991-07-01"),
# MAGIC (2, "Heitor Luz", "1985-12-16");

# COMMAND ----------

spark.sql("""
    MERGE INTO tb_people AS target
    USING tb_people_2 AS source
    ON target.id = source.id
    WHEN MATCHED THEN
    UPDATE SET *
    WHEN NOT MATCHED BY TARGET
    THEN INSERT *
    WHEN NOT MATCHED BY SOURCE 
    THEN DELETE
""").show()

# COMMAND ----------

spark.sql('SELECT * FROM tb_people ORDER BY id').show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dropping a Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE tb_people_2;

# COMMAND ----------

spark.sql('SHOW TABLES').show()