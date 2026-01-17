# Databricks notebook source
# MAGIC %md
# MAGIC # Performing Advanced Transformations

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_05"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working with Window Functions

# COMMAND ----------

create_window_functions_example_tables()

# COMMAND ----------

# Over Explanation
spark.sql("""
    SELECT 
        level, 
        product_name, 
        base_price,
        ROW_NUMBER() OVER(ORDER BY base_price DESC) AS price_rank
    FROM products
    ORDER BY base_price ASC
""").show(truncate=False)

# COMMAND ----------

# Over Partition By Explanation
spark.sql("""
    SELECT 
        level, 
        product_name, 
        base_price,
        ROW_NUMBER() OVER(PARTITION BY level ORDER BY base_price DESC) AS price_rank
    FROM products
    ORDER BY base_price ASC
""").show(truncate=False)

# COMMAND ----------

# Over Rows Between Explanation
spark.sql("""
  SELECT level, year, quarter, revenue,
    SUM(revenue) OVER (
      PARTITION BY level
      ORDER BY year, quarter
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
  FROM revenue_by_course_level_and_quarter
  ORDER BY level, year, quarter
""").show(truncate=False)

# COMMAND ----------

# Aggregation Window Functions
spark.sql("""
  SELECT DISTINCT
    level,
    SUM(revenue) OVER (PARTITION BY level) AS sum_revenue_by_course_level,
    AVG(revenue) OVER (PARTITION BY level) AS avg_revenue_by_course_level
  FROM revenue_by_course_level_and_quarter
  ORDER BY level
""").show(truncate=False)

# COMMAND ----------

# Ranking Functions
spark.sql("""
  SELECT 
    level, 
    product_name, 
    base_price,
    ROW_NUMBER() OVER(PARTITION BY level ORDER BY base_price DESC) AS row_number_by_course_level,
	  DENSE_RANK() OVER(PARTITION BY level ORDER BY base_price DESC) AS dense_rank_by_course_level,
   RANK() OVER(PARTITION BY level ORDER BY base_price DESC) AS rank_by_course_level
  FROM products
  WHERE level = 'intermediate'
  ORDER BY level
""").show()

# COMMAND ----------

# Offset Functions
spark.sql("""
    SELECT 
        level, year, quarter, revenue,
        LAG(revenue, 1, 0) OVER (PARTITION BY level ORDER BY year, quarter) AS previous_quarter_sales,
        LEAD(revenue, 1,0) OVER (PARTITION BY level ORDER BY year, quarter) AS next_quarter_sales,
        FIRST_VALUE(revenue) OVER (PARTITION BY level ORDER BY year, quarter) AS first_value,
        LAST_VALUE(revenue) OVER (
            PARTITION BY level 
            ORDER BY year, quarter
            -- need to used Rows between to catch last_value with partition by 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_value
    FROM revenue_by_course_level_and_quarter
    WHERE level = 'advanced'
    ORDER BY level, year, quarter
""").show(truncate=False)

# COMMAND ----------

drop_window_functions_example_tables()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working with Common Table Expressions

# COMMAND ----------

# get last 2 orders from each user -> focus on user_id = 5
spark.sql("""
    WITH cte AS (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY order_date DESC) AS rn
        FROM orders
    )

    SELECT 
        u.user_id,
        u.email,
        cte.order_id,
        cte.order_date
    FROM cte 
    JOIN users AS u ON cte.user_id = u.user_id
    WHERE rn <= 2
    ORDER BY u.user_id, order_date DESC
""").show(10, truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM orders 
    WHERE user_id = 5 
    ORDER BY order_date DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working with the QUALIFY Statement

# COMMAND ----------

# Using QUALIFY
spark.sql("""
    SELECT 
        u.user_id,
        u.email,
        o.order_id,
        o.order_date
    FROM orders AS o
    JOIN users AS u ON o.user_id = u.user_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY u.user_id ORDER BY o.order_date DESC) <= 2
    ORDER BY u.user_id
""").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working With ARRAY Data

# COMMAND ----------

# Common Built-in functions to handle array
spark.sql("""
    SELECT 
        ARRAY(100, 200, 300, 400) AS generated_array,
        ARRAY(100, 200, 300, 400)[0] AS first_element,
        ARRAY(100, 200, 300, 400)[2] AS third_element,
        ARRAY(100, 200, 300) || ARRAY(400, 500) AS merge_arrays,
        ARRAY_DISTINCT(ARRAY(200, 100, 100, 300, 300)) AS distinct_array,
        ARRAY_SORT(ARRAY(200, 100, 300)) AS sort_array,
        SEQUENCE(1, 20, 4) AS step_array
""").show(truncate=False)

# COMMAND ----------

# generating arrays based on table column data
spark.sql("""
    SELECT ARRAY_AGG(DISTINCT level) AS distinct_categories 
    FROM products
""").show(truncate=False)

# COMMAND ----------

# before exploding
print("Result from array_products_by_order files before using EXPLODE function:")
spark.sql("""
    SELECT *
    FROM json.`/Volumes/workspace/default/chapter_05/array_products_by_order/json`
""").show(5, truncate=False)

# after exploding
print("\nResult from array_products_by_order files after using EXPLODE function:")
spark.sql("""
    SELECT 
        order_id, 
        array_products,
        EXPLODE(array_products) AS product_id
    FROM json.`/Volumes/workspace/default/chapter_05/array_products_by_order/json`
""").show(10, truncate=False)

# COMMAND ----------

# filtering an array column
spark.sql("""
    SELECT 
        order_id, 
        array_products
    FROM json.`/Volumes/workspace/default/chapter_05/array_products_by_order/json`
    WHERE ARRAY_CONTAINS(array_products, 2)
""").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working With JSON Nested Data

# COMMAND ----------

create_json_example_tables()

# COMMAND ----------

# order_details_by_id_string is a string column with JSON content
# order_details_by_id_struct is a column with struct datatype
spark.sql("SELECT * FROM order_details_json LIMIT 5").display()

# COMMAND ----------

# It's possible to use : syntax in queries to access subfields in JSON strings
spark.sql("""
    SELECT 
        order_id,
        order_details_by_id_string:product_id AS product_id,
        order_details_by_id_string:unit_price AS unit_price,
        order_details_by_id_string:discount AS discount 
    FROM order_details_json
""").show(5, truncate=False)

# COMMAND ----------

# It's possible to use . syntax in queries to access subfields within a struct column
spark.sql("""
    SELECT 
        order_id,
        order_details_by_id_struct.product_id AS product_id,
        order_details_by_id_struct.unit_price AS unit_price,
        order_details_by_id_struct.discount AS discount 
    FROM order_details_json
""").show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cell 26
# converting json string column to struct data type using FROM_JSON
schema_from_column = 'STRUCT<product_id: BIGINT, unit_price: DOUBLE, discount: DOUBLE>'

spark.sql(f"""
    SELECT 
        order_id,
        FROM_JSON(
            order_details_by_id_string, 
            '{schema_from_column}'
        ) AS order_details_by_id_struct
    FROM order_details_json
    LIMIT 5
""").display()

# COMMAND ----------

# It's possible to use SCHEMA_OF_JSON_AGG function to define a common schema from all records of the dataset dinamically
schema_from_column = spark.sql("""
    SELECT DISTINCT 
        SCHEMA_OF_JSON_AGG(order_details_by_id_string) AS schema_from_col
    FROM order_details_json
    GROUP BY order_id
""").collect()[0]['schema_from_col']

print(f'schema from column [order_details_by_id_string]:"{schema_from_column}"\n')

spark.sql(f"""
    SELECT 
        order_id,
        FROM_JSON(
            order_details_by_id_string, 
            '{schema_from_column}'
        ) AS order_details_by_id_struct
    FROM order_details_json
    LIMIT 5
""").display()

# COMMAND ----------

# converting a column with struct data type to JSON string using TO_JSON
spark.sql(f"""
    SELECT 
        order_id,
        TO_JSON(order_details_by_id_struct) AS order_details_by_id_string
    FROM order_details_json
    LIMIT 5
""").display()

# COMMAND ----------

# using NAMED_STRUCT function to create a single struct column from multiple columns
spark.sql(f"""
    SELECT 
        order_id,
        NAMED_STRUCT(
            'product_id', product_id, 
            'unit_price', unit_price,
            'discount', discount
        ) AS order_details_by_id_struct
    FROM order_details
    LIMIT 5
""").display()

# COMMAND ----------

drop_json_example_tables()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working With VARIANT Data Type

# COMMAND ----------

drop_variant_example_tables()
create_variant_example_tables()

# COMMAND ----------

spark.sql("SELECT * FROM order_details_variant").show(truncate=False)

# COMMAND ----------

# Using TRY_PARSE_JSON function to convert a json string column to variant data type
df = spark.sql("""
    SELECT 
        order_id,
        (order_details_by_id_string) AS order_details_by_id_variant
    FROM order_details_variant
""")

df.printSchema()

df.show(5, truncate=False)

# COMMAND ----------

# Using PARSE_JSON + TO_JSON function to convert a struct field column to variant data type
df = spark.sql("""
    SELECT 
        order_id,
        PARSE_JSON(TO_JSON(order_details_by_id_struct)) AS order_details_by_id_variant
    FROM order_details_json
""")

df.printSchema()

df.show(5, truncate=False)

# COMMAND ----------

drop_json_example_tables()
