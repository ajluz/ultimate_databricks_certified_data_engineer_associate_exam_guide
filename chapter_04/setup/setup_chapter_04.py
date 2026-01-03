# Databricks notebook source
def cleanup():
    tables = [
        "workspace.default.tb_people",
        "workspace.default.tb_people_2",
        "workspace.default.tb_people_2_partitioned"
    ]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
