# Databricks notebook source
# MAGIC %md
# MAGIC # Tracking Changes in a Delta Table

# COMMAND ----------

spark.sql('DESCRIBE HISTORY tb_people').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Concurrent Operations on Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple Concurrent INSERTs

# COMMAND ----------

import threading 
def execute_parallel_commands(commands):
    def execute_command(command): 
        print(f'executing: [{command}] ...')
        spark.sql(command)
        print(f'command: [{command}] have finished!')

    threads = []
    for command in commands: 
        thread = threading.Thread(target=execute_command, args=(command,))
        thread.start()
        threads.append(thread)
 
    for thread in threads: 
        thread.join()

# COMMAND ----------

multiple_inserts = [
  "INSERT INTO tb_people (id, name, birth, gender, ssn) VALUES (7, 'Mary', '1999-01-01', 'female', '987-11-34')",
  "INSERT INTO tb_people (id, name, birth, gender, ssn) VALUES (8, 'John', '1999-12-01', 'male', '987-65-43')",
]

execute_parallel_commands(multiple_inserts)

# COMMAND ----------

spark.sql('SELECT * FROM tb_people WHERE id IN (7,8)').show()

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY tb_people
""").limit(2).select('version','operation','operationMetrics').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concurrent INSERTs and UPDATEs

# COMMAND ----------

insert_and_update = [
  "INSERT INTO tb_people (id, name, birth, gender, ssn) VALUES (9, 'Dexter Morgan', '1971-02-01', 'male', '222-33-34')",
  "UPDATE tb_people SET name = 'Mary Jones' WHERE id = 7"
]

execute_parallel_commands(insert_and_update)

# COMMAND ----------

spark.sql('SELECT * FROM tb_people WHERE id IN (7,9)').show()

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY tb_people
""").limit(2).select('version','operation','operationMetrics').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple Concurrent Changes

# COMMAND ----------

multiple_updates = [
  "UPDATE tb_people SET birth = '1982-01-05', gender = 'male' WHERE id = 3",
  "UPDATE tb_people SET gender = 'male' WHERE id = 2"
]

execute_parallel_commands(multiple_updates)

# COMMAND ----------

spark.sql('SELECT * FROM tb_people WHERE id IN (2,3)').show()

# COMMAND ----------

multiple_updates_same_row = [
  "UPDATE tb_people SET ssn = '010-24-999' WHERE id = 1",
  "UPDATE tb_people SET gender = 'male' WHERE id = 1",
]

execute_parallel_commands(multiple_updates_same_row)