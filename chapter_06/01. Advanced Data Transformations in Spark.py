# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Data Transformations in Spark

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_06"

# COMMAND ----------

generate_and_write_to_volume("06")

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working with Window Functions

# COMMAND ----------

create_window_functions_example_tables()

# COMMAND ----------

# Book - example for figure 6.1
spark.sql("""
    SELECT p.level, p.product_id, p.base_price AS unit_price,
        SUM(p.base_price) OVER() AS `OVER`,
        SUM(p.base_price) OVER (PARTITION BY p.level) AS `PARTITION BY`,
        SUM(p.base_price) OVER (
            PARTITION BY p.level 
            ORDER BY p.base_price, p.product_id
            ROWS BETWEEN UNBOUNDED PRECEDING 
            AND CURRENT ROW
        ) AS `ROWS BETWEEN`
    FROM products AS p
    ORDER BY 
        CASE p.level
            WHEN 'beginner' THEN 1
            WHEN 'intermediate' THEN 2
            WHEN 'advanced' THEN 3
        END
""").show(truncate=False)

# COMMAND ----------

# Over Order By Explanation
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
      ROWS BETWEEN UNBOUNDED PRECEDING 
        AND CURRENT ROW
    ) AS running_revenue
  FROM revenue_by_course_level_and_quarter
  WHERE level = 'intermediate'
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
    ROW_NUMBER() OVER(PARTITION BY level ORDER BY base_price DESC) AS row_number,
	  DENSE_RANK() OVER(PARTITION BY level ORDER BY base_price DESC) AS dense_rank,
   RANK() OVER(PARTITION BY level ORDER BY base_price DESC) AS rank
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
        SELECT *,
               ROW_NUMBER() OVER(
                   PARTITION BY user_id 
                   ORDER BY order_date DESC
               ) AS rn
        FROM orders
    )
    SELECT u.user_id,
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
    SELECT u.user_id, u.email, o.order_id, o.order_date
    FROM orders AS o
    JOIN users AS u ON o.user_id = u.user_id
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY u.user_id 
        ORDER BY o.order_date DESC
    ) <= 2
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
    FROM json.`/Volumes/workspace/default/chapter_06/array_products_by_order/json`
""").show(5, truncate=False)

# after exploding
print("\nResult from array_products_by_order files after using EXPLODE function:")
spark.sql("""
    SELECT 
        order_id, 
        array_products,
        EXPLODE(array_products) AS product_id
    FROM json.`/Volumes/workspace/default/chapter_06/array_products_by_order/json`
""").show(10, truncate=False)

# COMMAND ----------

# filtering an array column
spark.sql("""
    SELECT 
        order_id, 
        array_products
    FROM json.`/Volumes/workspace/default/chapter_06/array_products_by_order/json`
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

# It's possible to use both syntax patterns to JOIN or filter data on WHERE clause
spark.sql("""
    SELECT 
        order_id,
        order_details_by_id_string:product_id AS product_id,
        p.product_name,
        order_details_by_id_string:unit_price AS unit_price,
        order_details_by_id_string:discount AS discount
    FROM order_details_json AS od
    JOIN products AS p ON od.order_details_by_id_string:product_id = p.product_id
""").show(5, truncate=False)

spark.sql("""
    SELECT 
        order_id,
        order_details_by_id_struct.product_id AS product_id,
        p.product_name,
        order_details_by_id_struct.unit_price AS unit_price,
        order_details_by_id_struct.discount AS discount
    FROM order_details_json AS od
    JOIN products AS p ON od.order_details_by_id_struct.product_id = p.product_id
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
    SELECT
        SCHEMA_OF_JSON_AGG(order_details_by_id_string) AS schema_from_col
    FROM order_details_json
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

create_variant_example_tables()

# COMMAND ----------

spark.sql("""
    SELECT * 
    FROM order_details_variant 
    --WHERE order_id IN (2000, 2001, 2002, 2005)
    ORDER BY order_id
""").show(truncate=False)

# COMMAND ----------

# Using SCHEMA_OF_JSON_AGG function to define a common schema from all records of the dataset dinamically
schema_from_column = spark.sql("""
    SELECT 
        SCHEMA_OF_JSON_AGG(order_details_by_id_string) AS schema_from_col
    FROM order_details_variant
""").collect()[0]['schema_from_col']

print(f'schema from column [order_details_by_id_string]:"{schema_from_column}"\n')

# Since we have multiples schemas in the same column define a struct field would mess up the schema
spark.sql(f"""
    SELECT 
        order_id,
        FROM_JSON(
            order_details_by_id_string, 
            '{schema_from_column}'
        ) AS order_details_by_id_struct
    FROM order_details_variant
    ORDER BY order_id
""").display()

# COMMAND ----------

# It's possible to use PARSE_JSON function to convert a json string column to variant data type
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW vw_order_details_variant AS
    SELECT 
        order_id,
        PARSE_JSON(order_details_by_id_string) AS order_details_by_id_variant
    FROM order_details_variant
    WHERE order_id != 2005
    ORDER BY order_id
""")

spark.sql("""
    SELECT * 
    FROM vw_order_details_variant
""").printSchema()

spark.sql("""
    SELECT * 
    FROM vw_order_details_variant 
""").show(3, truncate=False)

# COMMAND ----------

# It's possible to use : syntax in queries to access subfields in variant field
# It will get just the rows that contains the path being accessed
# It's possible to cast the datatype when accessed using :: syntax
spark.sql("""
    SELECT 
        order_id, 
        order_details_by_id_variant:id::INT AS product_id,
        order_details_by_id_variant:unit_price::DOUBLE AS unit_price,
        order_details_by_id_variant:auto_renew::BOOLEAN AS auto_renew,
        order_details_by_id_variant:currency::STRING AS currency
    FROM vw_order_details_variant
    ORDER BY order_id
""").show()

# COMMAND ----------

# Use schema_of_variant function to return the schema for each row within the column
spark.sql("""
    SELECT 
        schema_of_variant(order_details_by_id_variant) variant_row_schema
    FROM vw_order_details_variant
""").show(truncate=False)

# COMMAND ----------

# Use schema_of_variant_agg function to return common schema for all rows within the dataset
spark.sql("""
    SELECT 
        schema_of_variant_agg(order_details_by_id_variant) variant_common_schema
    FROM vw_order_details_variant
""").collect()[0]['variant_common_schema']

# COMMAND ----------

# It's possible to use TRY_PARSE_JSON function to handle columns with broken json strings
# the order_id 2005 has a broken json string. It will return NULL for that row
spark.sql("""
    WITH variant_cte AS (
        SELECT 
            order_id,
            order_details_by_id_string,
            TRY_PARSE_JSON(
                order_details_by_id_string
            ) AS order_details_by_id_variant
        FROM order_details_variant
        WHERE order_id = 2005
        ORDER BY order_id
    )
    SELECT 
        order_id,
        order_details_by_id_string,
        order_details_by_id_variant
    FROM variant_cte
""").show(truncate=False)

# COMMAND ----------

drop_variant_example_tables()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working With PIVOT, UNPIVOT and Grouping Sets

# COMMAND ----------

create_pivot_example_tables()

# COMMAND ----------

spark.sql("SELECT * FROM pivot_example").show(5, truncate=False)

# COMMAND ----------

# Using PIVOT on Spark SQL 
spark.sql("""
    SELECT *
    FROM ( 
        SELECT year_month, level, qty 
        FROM pivot_example
    ) AS q 
    PIVOT(
        SUM(qty) 
        FOR level
            IN ('beginner','intermediate','advanced')
    )
    ORDER BY year_month
""").show(5, truncate=False)

# COMMAND ----------

# Creating a dynamic pivot query 
df = spark.sql("SELECT DISTINCT level FROM pivot_example")
levels = [row["level"] for row in df.select("level").collect()]
pivot_columns = ", ".join([f"'{level}'" for level in levels])

pivot_query = f"""
SELECT *
FROM ( 
    SELECT year_month, level, qty 
    FROM pivot_example
) AS q 
PIVOT(
    SUM(qty) 
    FOR level
        IN ({pivot_columns})
)
ORDER BY year_month
"""

print(f"pivot_columns are: ({pivot_columns})\n")

print(f"dinamic pivot query is: \n{pivot_query}\n")

spark.sql(pivot_query).show(truncate=False)

# COMMAND ----------

create_unpivot_example_tables()

# COMMAND ----------

spark.sql("SELECT * FROM unpivot_example").show(5, truncate=False)

# COMMAND ----------

# Using UNPIVOT on Spark SQL
spark.sql("""
    SELECT year_month, level, qty 
    FROM unpivot_example
    UNPIVOT(
    qty FOR level 
        IN(advanced, beginner, intermediate)
    ) AS unpvt
    ORDER BY year_month, level
""").show(5, truncate=False)

# COMMAND ----------

drop_pivot_example_tables()
drop_unpivot_example_tables()

# COMMAND ----------

create_grouping_sets_example_tables()

# COMMAND ----------

# Using UNION ALL to handle multiple group by clauses
spark.sql("""
    SELECT 
        -- group by all
        NULL AS level,
        NULL AS year_month,
        SUM(qty) AS qty 
    FROM grouping_sets_example
    UNION ALL 
    SELECT 
        -- group by level
        level,
        NULL AS year_month,
        SUM(qty) AS qty 
    FROM grouping_sets_example
    GROUP BY level
    UNION ALL 
    SELECT 
        -- group by level and year_month
        level,
        year_month,
        SUM(qty) AS qty 
    FROM grouping_sets_example
    GROUP BY level, year_month
    ORDER BY year_month, level
""").show(10)

# COMMAND ----------

# Using GROUPING SETS to handle multiple group by clauses
spark.sql("""
    SELECT level, year_month, SUM(qty) AS qty 
    FROM grouping_sets_example
    GROUP BY 
        GROUPING SETS (
            (), -- group by all 
            (level), -- group by level
            (level, year_month) -- group by level and year_month
        )
    ORDER BY year_month, level
""").show(10)

# COMMAND ----------

# Using ROLLUP 
spark.sql("""
    SELECT level, year_month, SUM(qty) AS qty 
    FROM grouping_sets_example
    GROUP BY ROLLUP (level, year_month)
    ORDER BY year_month, level
""").show(10)

# COMMAND ----------

# Using CUBE
spark.sql("""
    SELECT 
        level,
        year_month,
        SUM(qty) AS qty 
    FROM grouping_sets_example
    GROUP BY CUBE (level, year_month)
    ORDER BY year_month, level
""").show(10)

# COMMAND ----------

drop_grouping_sets_example_tables()
