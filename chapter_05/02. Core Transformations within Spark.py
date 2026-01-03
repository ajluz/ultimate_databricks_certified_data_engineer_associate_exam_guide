# Databricks notebook source
# MAGIC %md
# MAGIC # Core Transformations Within Spark

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_05"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Relationships Between Tables

# COMMAND ----------

create_join_example_tables()

# COMMAND ----------

spark.sql("SELECT * FROM orders_example").show(truncate=False)

# COMMAND ----------

spark.sql("SELECT * FROM users_example).show(truncate=False)

# COMMAND ----------

# Inner Join
spark.sql("""
    SELECT 
        u.email,
        o.*
    FROM users_example AS u
    INNER JOIN orders_example AS o ON o.user_id = u.user_id -- could be JOIN 
""").show(5, truncate=False)

# COMMAND ----------

# Left Join
spark.sql("""
    SELECT 
        u.email,
        o.*
    FROM users_example AS u
    LEFT OUTER JOIN orders_example AS o ON o.user_id = u.user_id -- could be LEFT JOIN
""").show(truncate=False)

# COMMAND ----------

# Right Join
spark.sql("""
    SELECT 
        u.email,
        o.*
    FROM users_example AS u
    RIGHT OUTER JOIN orders_example AS o ON o.user_id = u.user_id -- could be RIGHT JOIN
""").show(truncate=False)

# COMMAND ----------

# Full Join
spark.sql("""
    SELECT 
        u.email,
        o.*
    FROM users_example AS u
    FULL OUTER JOIN orders_example AS o ON o.user_id = u.user_id -- could be FULL JOIN
""").show(truncate=False)

# COMMAND ----------

# Semi Join
spark.sql("""
    SELECT *
    FROM orders_example AS o 
    SEMI JOIN users_example AS u ON o.user_id = u.user_id
""").show(truncate=False)

# COMMAND ----------

# Anti Join
spark.sql("""
    SELECT *
    FROM orders_example AS o 
    ANTI JOIN users_example AS u ON o.user_id = u.user_id
""").show(truncate=False)

# COMMAND ----------

drop_join_example_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Conditions to Filter Data

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM users 
    WHERE profession = 'Data Engineer'
""").show(5, truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM users 
    WHERE profession IN ('Data Engineer','Developer')
""").show(5, truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM orders
    WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31'
""").show(5, truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM orders
    WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31'
        AND payment_method != 'debit_card'
""").show(5, truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM users 
    WHERE profession = NULL
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM users 
    WHERE profession IS NULL
""").show(5, truncate=False)

# COMMAND ----------

# Using Like
spark.sql("""
    SELECT * 
    FROM users 
    WHERE profession LIKE 'Data%'
""").show(5, truncate=False)

# COMMAND ----------

# Using Like Any
spark.sql("""
    SELECT * 
    FROM users 
    WHERE profession LIKE ANY ('Data%', 'Dev%')
""").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Data Types with Built-In Functions

# COMMAND ----------

# numeric functions
num_var = 1234.5678

spark.sql(f"""
    SELECT 'ABS' AS function_name, CAST(ABS(-{num_var}) AS STRING) AS result
    UNION ALL
    SELECT 'ROUND(2)', CAST(ROUND({num_var}, 2) AS STRING)
    UNION ALL
    SELECT 'ROUND(0)', CAST(ROUND({num_var}) AS STRING)
    UNION ALL
    SELECT 'POWER', CAST(POWER(2, 3) AS STRING)
    UNION ALL
    SELECT 'SQRT', CAST(SQRT(16) AS STRING)
    UNION ALL
    SELECT 'EXP', CAST(EXP(1) AS STRING)
    UNION ALL
    SELECT 'RAND', CAST(RAND() AS STRING)
    UNION ALL
    SELECT 'RAND(seed)', CAST(RAND(42) AS STRING)
    UNION ALL
    SELECT 'GREATEST', CAST(GREATEST(10, 20, 5) AS STRING)
    UNION ALL
    SELECT 'LEAST', CAST(LEAST(10, 20, 5) AS STRING)
""").show(truncate=False)


# COMMAND ----------

# string functions
string_var="This is Spark, Spark SQL"
s=string_var.replace("'","''")

spark.sql(f"""
    SELECT 'CONCAT' AS function_name, CAST(CONCAT('{s}',' is awesome')AS STRING) AS result
    UNION ALL 
    SELECT 'SUBSTRING' , CAST(SUBSTRING('{s}',8,6)AS STRING) 
    UNION ALL
    SELECT 'LEFT', CAST(LEFT('{s}',4)AS STRING)
    UNION ALL
    SELECT 'RIGHT', CAST(RIGHT('{s}',3)AS STRING)
    UNION ALL
    SELECT 'LEN', CAST(LEN('{s}'||'     ')AS STRING)
    UNION ALL
    SELECT 'LENGTH', CAST(LENGTH('{s}')AS STRING)
    UNION ALL
    SELECT 'CHARINDEX', CAST(CHARINDEX('is','{s}')AS STRING)
    UNION ALL
    SELECT 'REPLACE', CAST(REPLACE('{s}','Spark','Databricks')AS STRING)
    UNION ALL
    SELECT 'UPPER', CAST(UPPER('{s}')AS STRING)
    UNION ALL
    SELECT 'LOWER', CAST(LOWER('{s}')AS STRING)
    UNION ALL
    SELECT 'TRIM', CAST(TRIM(' '||'{s}'||' ')AS STRING)
    UNION ALL
    SELECT 'LTRIM', CAST(LTRIM(' '||'{s}'||' ')AS STRING)
    UNION ALL
    SELECT 'RTRIM', CAST(RTRIM(' '||'{s}'||' ')AS STRING)
    UNION ALL
    SELECT 'SPLIT', TO_JSON(SPLIT('{s}',','))
    UNION ALL
    SELECT 'TRANSLATE', CAST(TRANSLATE('{s}','Sk','Xy')AS STRING)
    UNION ALL
    SELECT 'LPAD', CAST(LPAD('{s}',25,'0')AS STRING)
    UNION ALL
    SELECT 'RPAD', CAST(RPAD('{s}',25,'0')AS STRING)
""").show(truncate=False)

# COMMAND ----------

# date & timestamp functions
date_var = "2025-02-10 14:51:27"

spark.sql(f"""
    SELECT 'CURRENT_TIMESTAMP' AS function_name, CAST(CURRENT_TIMESTAMP() AS STRING) AS result
    UNION ALL
    SELECT 'NOW', CAST(now() AS STRING)
    UNION ALL 
    SELECT 'FROM_UTC_TIMESTAMP', CAST(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS STRING)
    UNION ALL
    SELECT 'YEAR', CAST(YEAR('{date_var}') AS STRING)
    UNION ALL
    SELECT 'MONTH', CAST(MONTH('{date_var}') AS STRING)
    UNION ALL
    SELECT 'DAY', CAST(DAY('{date_var}') AS STRING)
    UNION ALL
    SELECT 'DAYOFYEAR', CAST(DAYOFYEAR('{date_var}') AS STRING)
    UNION ALL
    SELECT 'DAYOFWEEK', CAST(DAYOFWEEK('{date_var}') AS STRING)
    UNION ALL
    SELECT 'DAYOFMONTH', CAST(DAYOFMONTH('{date_var}') AS STRING)
    UNION ALL
    SELECT 'DATE', CAST(DATE('{date_var}') AS STRING)
    UNION ALL
    SELECT 'HOUR', CAST(HOUR('{date_var}') AS STRING)
    UNION ALL
    SELECT 'MINUTE', CAST(MINUTE('{date_var}') AS STRING)
    UNION ALL
    SELECT 'SECOND', CAST(SECOND('{date_var}') AS STRING)
    UNION ALL
    SELECT 'UNIX_TIMESTAMP', CAST(UNIX_TIMESTAMP('{date_var}', 'yyyy-MM-dd HH:mm:ss') AS STRING)
    UNION ALL
    SELECT 'DATE_FORMAT (BR)',DATE_FORMAT('{date_var}', 'dd/MM/yyyy')
""").show(truncate=False)

