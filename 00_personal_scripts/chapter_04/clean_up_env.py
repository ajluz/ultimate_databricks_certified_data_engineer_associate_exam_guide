# Databricks notebook source
### DROP ALL TABLES
for row in tables_df.collect():
    spark.sql(f"DROP TABLE IF EXISTS `{row['database']}`.`{row['tableName']}`")
display(spark.sql("SHOW TABLES"))
