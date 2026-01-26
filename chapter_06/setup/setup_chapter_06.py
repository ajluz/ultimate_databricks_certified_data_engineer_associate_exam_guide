# Databricks notebook source
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
import os
import decimal

def drop_tables():
    tables = [
        "workspace.default.pivot_example",
        "workspace.default.unpivot_example",
        "workspace.default.grouping_sets_example",
        "workspace.default.order_details_variant",
        "workspace.default.revenue_by_course_level_and_quarter"
    ]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

def drop_volume(chapter_number: str):
    spark.sql(f"drop volume if exists workspace.default.chapter_{chapter_number}")

def create_volume(chapter_number: str):
    spark.sql(f"create volume if not exists workspace.default.chapter_{chapter_number}")

def create_enumerated_files(temp_path: str, table_name: str, file_format: str):
    data_files = [f for f in dbutils.fs.ls(temp_path) if f.name.startswith("part-")]
    for index, file_name in enumerate(data_files, start=1):
        path = temp_path.replace('temp/', '')
        dbutils.fs.mv(file_name.path, f"{path}{table_name}_0{index}.{file_format}")

def create_complex_data_files(chapter_number: str, temp_root: str):
    df = spark.table('workspace.default.order_details')

    array_products = (
        df.groupBy("order_id")
          .agg(F.collect_list("product_id").alias("array_products"))
    )

    order_details_dict = (
        df.groupBy("order_id")
          .agg(F.collect_list(F.struct(
                F.col("product_id").alias("product_id"),
                F.col("unit_price").alias("unit_price"),
                F.col("discount").alias("discount")
          )).alias("order_details"))
    )

    (array_products.repartition(4)
        .write.mode("overwrite").format("json")
        .save(f"{temp_root}/array_products_by_order/json/"))

    (order_details_dict.repartition(4)
        .write.mode("overwrite").format("json")
        .save(f"{temp_root}/order_details_dict/json/"))

def create_pivot_example_tables():
    spark.sql("""
        CREATE TABLE IF NOT EXISTS pivot_example 
        AS
            SELECT
                date_format(o.order_date, 'yyyy/MM') AS year_month,
                p.level,
                COUNT(*) AS qty
            FROM products AS p
            INNER JOIN order_details AS od ON p.product_id=od.product_id
            INNER JOIN orders AS o ON od.order_id=o.order_id
            GROUP BY date_format(o.order_date, 'yyyy/MM'),
                    p.level
            ORDER BY year_month,
                    p.level
    """)

def drop_pivot_example_tables():
    spark.sql("DROP TABLE IF EXISTS pivot_example")

def create_unpivot_example_tables():
    spark.sql("""
        CREATE TABLE IF NOT EXISTS unpivot_example 
        AS
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
    """)

def drop_unpivot_example_tables():
    spark.sql("DROP TABLE IF EXISTS unpivot_example")

def create_grouping_sets_example_tables():
    spark.sql("""
        CREATE TABLE IF NOT EXISTS grouping_sets_example 
        AS
            SELECT
                date_format(o.order_date, 'yyyy/MM') AS year_month,
                p.level,
                COUNT(*) AS qty
            FROM products AS p
            INNER JOIN order_details AS od ON p.product_id=od.product_id
            INNER JOIN orders AS o ON od.order_id=o.order_id
            GROUP BY date_format(o.order_date, 'yyyy/MM'),
                    p.level
            ORDER BY year_month,
                    p.level
    """)

def drop_grouping_sets_example_tables():
    spark.sql("DROP TABLE IF EXISTS grouping_sets_example")

def create_variant_example_tables():
    spark.sql("""
        CREATE TABLE IF NOT EXISTS order_details_variant
        AS
            SELECT 
                2000 AS order_id,
                '{"discount":0.05,"product_id":5,"unit_price":283.0}' AS order_details_by_id_string
            UNION ALL
            SELECT 
                2001,
                '{"id": 50, "unit_price": 99.99, "discount": 0.15, "qty": 2}'
            UNION ALL
            SELECT 
                2002,
                '{"name": "New Course 1", "price": 149.99, "currency": "USD"}'
            UNION ALL
            SELECT 
                2003,
                '{"item": "New Course 2", "cost": 59.99, "tax": 4.80, "shipping": 5.00, "total": 69.79}'
            UNION ALL
            SELECT 
                2004,
                '{"id": 123, "description": "Subscription", "monthly_fee": 29.99, "billing_cycle": "monthly", "auto_renew": true}'
            UNION ALL 
            SELECT 
                2005,
                '{"product_id": 1, ]'
    """)

def drop_variant_example_tables():
    spark.sql("DROP TABLE IF EXISTS order_details_variant")

def create_json_example_tables():
    spark.sql("""
        CREATE TABLE IF NOT EXISTS order_details_json 
        AS
            WITH cte AS (
                SELECT 
                    order_id,
                    explode(order_details) AS order_details_by_id
                FROM json.`/Volumes/workspace/default/chapter_06/order_details_dict/json`
            )
            SELECT 
                order_id,
                TO_JSON(order_details_by_id) AS order_details_by_id_string,
                order_details_by_id AS order_details_by_id_struct
            FROM cte
    """)

def create_window_functions_example_tables():
    spark.sql("""
        CREATE TABLE IF NOT EXISTS revenue_by_course_level_and_quarter 
        AS 
            SELECT
                p.level,
                YEAR(o.order_date) AS year,
                QUARTER(o.order_date) AS quarter,
                CAST(SUM(od.unit_price) AS DECIMAL(10,2)) AS revenue
            FROM products p
            JOIN order_details od ON p.product_id = od.product_id
            JOIN orders o ON od.order_id = o.order_id
            GROUP BY p.level, 
                     YEAR(o.order_date), 
                     QUARTER(o.order_date)
    """)

def drop_window_functions_example_tables():
    spark.sql("DROP TABLE IF EXISTS revenue_by_course_level_and_quarter")

def drop_json_example_tables():
    spark.sql("DROP TABLE IF EXISTS order_details_json")

def generate_and_write_to_volume(chapter_number: str):
    drop_volume(chapter_number)
    drop_tables()
    create_volume(chapter_number)
    main_temp_path = f"/Volumes/workspace/default/chapter_{chapter_number}/temp"

    create_complex_data_files(chapter_number, main_temp_path)
    create_enumerated_files(f"{main_temp_path}/array_products_by_order/json/", "array_products_by_order", "json")
    create_enumerated_files(f"{main_temp_path}/order_details_dict/json/", "order_details_dict", "json")

    dbutils.fs.rm(main_temp_path, True)
