# Databricks notebook source
# MAGIC %md
# MAGIC # Performing Advanced Transformations

# COMMAND ----------

# MAGIC %run "./setup/setup_chapter_05"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working with Window Functions

# COMMAND ----------

drop_window_functions_example_tables()
create_window_functions_example_tables()

# COMMAND ----------

# Over Explanation
spark.sql("""
    SELECT 
        category, 
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
        category, 
        product_name, 
        base_price,
        ROW_NUMBER() OVER(PARTITION BY category ORDER BY base_price DESC) AS price_rank
    FROM products
    ORDER BY base_price ASC
""").show(truncate=False)

# COMMAND ----------

# Over Rows Between Explanation
spark.sql("""
  SELECT category, year, quarter, revenue,
    SUM(revenue) OVER (
      PARTITION BY category
      ORDER BY year, quarter
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
  FROM revenue_by_category_and_quarter
  ORDER BY category, year, quarter
""").show(truncate=False)

# COMMAND ----------

# Aggregation Window Functions
spark.sql("""
  SELECT DISTINCT
    category,
    SUM(revenue) OVER (PARTITION BY category) AS sum_revenue_by_category,
    AVG(revenue) OVER (PARTITION BY category) AS avg_revenue_by_category
  FROM revenue_by_category_and_quarter
  ORDER BY category
""").show(truncate=False)

# COMMAND ----------

# Ranking Functions
spark.sql("""
  SELECT 
    category, 
    product_name, 
    base_price,
    ROW_NUMBER() OVER(PARTITION BY category ORDER BY base_price DESC) AS row_number_by_category,
	  DENSE_RANK() OVER(PARTITION BY category ORDER BY base_price DESC) AS dense_rank_by_category,
   RANK() OVER(PARTITION BY category ORDER BY base_price DESC) AS rank_by_category
  FROM products
  WHERE category = 'intermediate'
  ORDER BY category
""").show()

# COMMAND ----------

# Offset Functions
spark.sql("""
    SELECT 
        category, year, quarter, revenue,
        LAG(revenue, 1, 0) OVER (PARTITION BY category ORDER BY year, quarter) AS previous_quarter_sales,
        LEAD(revenue, 1,0) OVER (PARTITION BY category ORDER BY year, quarter) AS next_quarter_sales,
        FIRST_VALUE(revenue) OVER (PARTITION BY category ORDER BY year, quarter) AS first_value,
        LAST_VALUE(revenue) OVER (
            PARTITION BY category 
            ORDER BY year, quarter
            -- need to used Rows between to catch last_value with partition by 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_value
    FROM revenue_by_category_and_quarter
    WHERE category = 'advanced'
    ORDER BY category, year, quarter
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working with Common Table Expressions

# COMMAND ----------

# spark.sql("""
#     WITH cte_month AS (
#         SELECT 
#             MONTH(order_date) AS order_month, 
#             user_id
#         FROM orders
#     )

#     SELECT 
#         order_month, 
#         COUNT(DISTINCT user_id) AS cust_count
#     FROM cte_month
#     GROUP BY order_month
#     ORDER BY order_month
# """).show(truncate=False)
