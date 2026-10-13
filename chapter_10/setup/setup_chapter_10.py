# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Chapter 10: Databricks Lakeflow Framework Fundamentals
# MAGIC
# MAGIC Helpers used by the companion notebooks of Chapter 10. Run this notebook with `%run "./setup/setup_chapter_10"`.
# MAGIC
# MAGIC Generated data (deterministic, seeded):
# MAGIC - `ip_access_data/` and `ip_access_app/` — JSON deliveries of accesses (same shape as Chapter 9), with a share of invalid IP addresses (`999.999.999.999`) and `unknown` access points
# MAGIC - `customer_changes/` — JSON change feed of customers (INSERT / UPDATE / DELETE with a change timestamp), including one late, out-of-order change

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone
import builtins

CHAPTER_NUMBER = "10"


def volume_path(chapter_number: str = CHAPTER_NUMBER) -> str:
    return f"/Volumes/workspace/default/chapter_{chapter_number}"


def drop_volume(chapter_number: str = CHAPTER_NUMBER):
    spark.sql(f"DROP VOLUME IF EXISTS workspace.default.chapter_{chapter_number}")


def create_volume(chapter_number: str = CHAPTER_NUMBER):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS workspace.default.chapter_{chapter_number}")

# COMMAND ----------

def _ip_access_delivery(delivery: int, rows_per_delivery: int, distinct_ips: int, invalid_ip_rate: float, seed: int, base_epoch: int):
    """One delivery of accesses: access_date, ip_address, access_point. A share of rows carries the invalid address 999.999.999.999."""
    access_points = ['iphone', 'android', 'chrome', 'safari', 'firefox', 'unknown']
    access_point_array = F.array(*[F.lit(a) for a in access_points])

    def _octet(n: int):
        return F.pmod(F.xxhash64(F.col("ip_id"), F.lit(seed + n)), F.lit(254)) + 1

    invalid_bucket = int(invalid_ip_rate * 1000)

    return (
        spark.range(rows_per_delivery)
            .withColumn("ip_id", F.pmod(F.xxhash64("id", F.lit(delivery), F.lit(seed)), F.lit(distinct_ips)))
            .withColumn(
                "ip_address",
                F.when(
                    F.pmod(F.xxhash64("id", F.lit(delivery), F.lit(seed + 30)), F.lit(1000)) < F.lit(invalid_bucket),
                    F.lit("999.999.999.999")
                ).otherwise(F.format_string("%d.%d.%d.%d", _octet(1), _octet(2), _octet(3), _octet(4)))
            )
            .withColumn(
                "access_date",
                F.timestamp_seconds(
                    F.lit(base_epoch + delivery * 3600)
                    + F.pmod(F.xxhash64("id", F.lit(delivery), F.lit(seed + 10)), F.lit(3600))
                )
            )
            .withColumn(
                "access_point",
                F.element_at(
                    access_point_array,
                    (F.pmod(F.xxhash64("id", F.lit(delivery), F.lit(seed + 20)), F.lit(len(access_points))) + 1).cast("int")
                )
            )
            .select("access_date", "ip_address", "access_point")
    )


def _write_ip_access_deliveries(output_path: str, first_delivery: int, deliveries: int, rows_per_delivery: int, distinct_ips: int, resend_rate: float, invalid_ip_rate: float, seed: int):
    base_epoch = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp())
    for delivery in range(first_delivery, first_delivery + deliveries):
        df_delivery = _ip_access_delivery(delivery, rows_per_delivery, distinct_ips, invalid_ip_rate, seed, base_epoch)
        df_resent = df_delivery.sample(fraction=resend_rate, seed=seed + delivery)
        (df_delivery.unionByName(df_resent)
            .repartition(1)
            .sortWithinPartitions("access_date")
            .write
            .mode("append")
            .json(output_path))


def generate_ip_access_files(
    chapter_number: str = CHAPTER_NUMBER,
    folder: str = "ip_access_data",
    deliveries: int = 10,
    rows_per_delivery: int = 1000,
    distinct_ips: int = 2000,
    resend_rate: float = 0.1,
    invalid_ip_rate: float = 0.05,
    seed: int = 42,
    ):
    """Resets the folder and writes `deliveries` JSON files of accesses (one file per delivery, one hour apart)."""
    output_path = f"{volume_path(chapter_number)}/{folder}"
    dbutils.fs.rm(output_path, True)
    _write_ip_access_deliveries(output_path, 0, deliveries, rows_per_delivery, distinct_ips, resend_rate, invalid_ip_rate, seed)
    print(f"{deliveries} delivery file(s) written to {output_path}")


def deliver_ip_access_files(
    chapter_number: str = CHAPTER_NUMBER,
    folder: str = "ip_access_data",
    deliveries: int = 1,
    rows_per_delivery: int = 1000,
    distinct_ips: int = 2000,
    resend_rate: float = 0.1,
    invalid_ip_rate: float = 0.05,
    seed: int = 42,
    ):
    """Appends `deliveries` new JSON files to the folder, continuing from the number of files already there (later timestamps)."""
    output_path = f"{volume_path(chapter_number)}/{folder}"
    try:
        existing = [f for f in dbutils.fs.ls(output_path) if f.name.endswith(".json")]
    except Exception:
        existing = []
    first_delivery = len(existing)
    _write_ip_access_deliveries(output_path, first_delivery, deliveries, rows_per_delivery, distinct_ips, resend_rate, invalid_ip_rate, seed)
    print(f"{deliveries} new delivery file(s) written to {output_path} (deliveries {first_delivery} to {first_delivery + deliveries - 1})")

# COMMAND ----------

CUSTOMER_CHANGE_DELIVERIES = {
    0: [
        (1, "Ana",   "Lisbon", "INSERT", "2026-07-01 09:00:00"),
        (2, "Bruno", "Porto",  "INSERT", "2026-07-01 09:05:00"),
        (3, "Carla", "Braga",  "INSERT", "2026-07-01 09:10:00"),
    ],
    1: [
        (2, "Bruno", "Faro",   "UPDATE", "2026-07-01 10:00:00"),
        (3, "Carla", "Braga",  "DELETE", "2026-07-01 10:30:00"),
        (4, "Diego", "Porto",  "INSERT", "2026-07-01 10:45:00"),
        # late change: older than the UPDATE above, must be discarded by SEQUENCE BY
        (2, "Bruno", "Coimbra", "UPDATE", "2026-07-01 09:30:00"),
    ],
    2: [
        (1, "Ana",   "Porto",  "UPDATE", "2026-07-01 11:00:00"),
        (2, "Bruno", "Aveiro", "UPDATE", "2026-07-01 11:15:00"),
    ],
}


def generate_customer_changes(chapter_number: str = CHAPTER_NUMBER, delivery: int = 0):
    """Writes one JSON file with the change records of the given delivery into customer_changes/ (delivery 0 resets the folder)."""
    if delivery not in CUSTOMER_CHANGE_DELIVERIES:
        raise ValueError(f"delivery must be one of {sorted(CUSTOMER_CHANGE_DELIVERIES)}, got {delivery!r}")
    output_path = f"{volume_path(chapter_number)}/customer_changes"
    if delivery == 0:
        dbutils.fs.rm(output_path, True)
    df = spark.createDataFrame(
        CUSTOMER_CHANGE_DELIVERIES[delivery],
        "customer_id INT, customer_name STRING, customer_city STRING, cdc_change_type STRING, cdc_change_timestamp STRING"
    )
    df.repartition(1).write.mode("append").json(output_path)
    print(f"delivery {delivery}: {df.count()} change record(s) written to {output_path}")

# COMMAND ----------

def cleanup_chapter_10(chapter_number: str = CHAPTER_NUMBER):
    """Removes the chapter volume. Delete chapter_10_job and chapter_10_pipeline from the Jobs & Pipelines page first (deleting the pipeline drops its tables)."""
    drop_volume(chapter_number)
    print(f"volume workspace.default.chapter_{chapter_number} dropped")

# COMMAND ----------

create_volume(CHAPTER_NUMBER)
