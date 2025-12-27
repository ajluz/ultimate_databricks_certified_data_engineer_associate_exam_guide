# Databricks notebook source
# MAGIC %pip install dbldatagen
# MAGIC
# MAGIC import dbldatagen as dg
# MAGIC from pyspark.sql import functions as F, types as T
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC def drop_volume(chapter_number: str):
# MAGIC     spark.sql(f"drop volume if exists workspace.default.chapter_{chapter_number}")
# MAGIC
# MAGIC def create_volume(chapter_number: str):
# MAGIC     spark.sql(f"""
# MAGIC         create volume if not exists workspace.default.chapter_{chapter_number}
# MAGIC         comment 'Datasets for chapter {chapter_number}'
# MAGIC     """)
# MAGIC
# MAGIC def create_enumerated_files(temp_path: str, table_name: str, file_format: str):
# MAGIC     data_files = [f for f in dbutils.fs.ls(temp_path) if f.name.startswith("part-")]
# MAGIC     for index, file_name in enumerate(data_files, start=1):
# MAGIC         path = temp_path.replace('temp/', '')
# MAGIC         dbutils.fs.mv(file_name.path, f"{path}{table_name}_0{index}.{file_format}")
# MAGIC
# MAGIC def create_complex_data_files(chapter_number: str, temp_root: str):
# MAGIC     od_path = f"/Volumes/workspace/default/chapter_{chapter_number}/order_details/parquet/"
# MAGIC     df = spark.read.parquet(od_path)
# MAGIC
# MAGIC     array_products = (
# MAGIC         df.groupBy("order_id")
# MAGIC           .agg(F.collect_list("product_id").alias("array_products"))
# MAGIC     )
# MAGIC
# MAGIC     order_details_dict = (
# MAGIC         df.groupBy("order_id")
# MAGIC           .agg(F.collect_list(F.struct(
# MAGIC                 F.col("product_id").alias("productid"),
# MAGIC                 F.col("unit_price").alias("unitprice"),
# MAGIC                 F.col("discount").alias("discount")
# MAGIC           )).alias("order_details"))
# MAGIC     )
# MAGIC
# MAGIC     (array_products.repartition(4)
# MAGIC         .write.mode("overwrite").format("json")
# MAGIC         .save(f"{temp_root}/array_products_by_order/json/"))
# MAGIC
# MAGIC     (order_details_dict.repartition(4)
# MAGIC         .write.mode("overwrite").format("json")
# MAGIC         .save(f"{temp_root}/order_details_dict/json/"))
# MAGIC
# MAGIC def write_all_formats(df, main_temp_path: str, table_name: str, one_file: bool = False):
# MAGIC     dict_types = { 
# MAGIC         "csv":     {"options": {"delimiter": ",", "header": "true"}},
# MAGIC         "parquet": {"options": {}},
# MAGIC         "json":    {"options": {}},
# MAGIC         "avro":    {"options": {}},
# MAGIC         "orc":     {"options": {}}
# MAGIC     }
# MAGIC     for fmt, spec in dict_types.items():
# MAGIC         tmp = f"{main_temp_path}/{table_name}/{fmt}/"
# MAGIC         writer_df = df.coalesce(1) if one_file else df.repartition(4)
# MAGIC         (writer_df.write
# MAGIC             .mode("overwrite")
# MAGIC             .options(**spec["options"])
# MAGIC             .format(fmt)
# MAGIC             .save(tmp))
# MAGIC         create_enumerated_files(tmp, table_name, fmt)
# MAGIC
# MAGIC def generate_and_write_to_volume(
# MAGIC     chapter_number: str,
# MAGIC     users_n: int = 1000,
# MAGIC     orders_n: int = 500,
# MAGIC     orphan_rate: float = 0.01,
# MAGIC     seed_users: int = 101,
# MAGIC     seed_orders: int = 202,
# MAGIC     seed_details: int = 303,
# MAGIC     seed_lines: int = 404,
# MAGIC     seed_pickers: int = 505,
# MAGIC     ):
# MAGIC
# MAGIC     countries = [
# MAGIC         ("US","United States"), ("BR","Brazil"), ("IN","India"), ("GB","United Kingdom"),
# MAGIC         ("DE","Germany"), ("FR","France"), ("ES","Spain"), ("CA","Canada"),
# MAGIC         ("AU","Australia"), ("MX","Mexico"), ("AR","Argentina"), ("CO","Colombia"),
# MAGIC         ("PT","Portugal"), ("IT","Italy"), ("NL","Netherlands"), ("SE","Sweden"),
# MAGIC         ("JP","Japan"), ("KR","South Korea"), ("ZA","South Africa"), ("NG","Nigeria")
# MAGIC     ]
# MAGIC     country_codes  = [c for c,_ in countries]
# MAGIC     country_weights = [10, 18, 18, 5, 4, 4, 4, 3, 2, 3, 2, 2, 2, 3, 2, 2, 3, 2, 2, 2]
# MAGIC
# MAGIC     user_professions = ['Data Engineer','Data Architect','Data Analyst','Data Scientist','Developer']
# MAGIC     courses = [
# MAGIC         'Building a Data Lakehouse with SQL and DDP',
# MAGIC         'Mastering SQL on Databricks',
# MAGIC         'Data Heroes Mentorship Program',
# MAGIC         'Book Club - Spark the Definitive Guide',
# MAGIC         'Book Club - Delta Lake the Definitive Guide'
# MAGIC     ]
# MAGIC     course_prices = [99.00, 297.00, 997.00, 297.00, 297.00]
# MAGIC
# MAGIC     email_providers = ['@gmail.com','@hotmail.com','@outlook.com']
# MAGIC     email_distribution = [60, 25, 15]
# MAGIC     gender = ['M','F']
# MAGIC     gender_distribution = [49, 51]
# MAGIC     payment_methods = ['credit_card','debit_card','paypal','apple_pay','google_pay']
# MAGIC     payment_method_distribution = [55, 15, 12, 8, 10]
# MAGIC     discounts = [0.05, 0.10, 0.15]
# MAGIC     discount_distribution = [50, 35, 15]
# MAGIC     installments = [1, 2, 3, 10, 12]
# MAGIC     installment_distribution = [20, 15, 5, 30, 30]
# MAGIC
# MAGIC     invalid_count = int(orders_n * orphan_rate)
# MAGIC     invalid_user_id = users_n + 9999
# MAGIC
# MAGIC     drop_volume(chapter_number)
# MAGIC     create_volume(chapter_number)
# MAGIC     main_temp_path = f"/Volumes/workspace/default/chapter_{chapter_number}/temp"
# MAGIC
# MAGIC     gen_users = (
# MAGIC         dg.DataGenerator(spark, name="users_gen", rows=users_n, partitions=4, randomSeedMethod="hash_fieldname")
# MAGIC         .withIdOutput()
# MAGIC         .withColumn("user_id", T.LongType(), expr="id + 1")
# MAGIC         .withColumn("email_provider", T.StringType(), values=email_providers, weights=email_distribution, baseColumn=["user_id"])
# MAGIC         .withColumn(
# MAGIC             "email", T.StringType(),
# MAGIC             expr="concat('user_', lpad(cast(user_id as string), 6, '0'), email_provider)",
# MAGIC             baseColumn=["user_id", "email_provider"]
# MAGIC         )
# MAGIC         .withColumn("gender", T.StringType(), values=gender, weights=gender_distribution, baseColumn=["user_id"],
# MAGIC                     nullable=True, percentNulls=0.08)
# MAGIC         .withColumn("profession", T.StringType(), values=user_professions, baseColumn=["user_id"],
# MAGIC                     nullable=True, percentNulls=0.05)
# MAGIC         .withColumn("country_code", T.StringType(), values=country_codes, weights=country_weights, baseColumn=["user_id"])
# MAGIC     )
# MAGIC     users_df = gen_users.build().select("user_id","email","gender","profession","country_code")
# MAGIC     country_map = F.create_map([F.lit(x) for kv in countries for x in kv])
# MAGIC     users_df = users_df.withColumn("country", country_map[F.col("country_code")]) \
# MAGIC                        .select("user_id","email","gender","profession","country_code","country")
# MAGIC
# MAGIC     products_df = spark.createDataFrame(
# MAGIC         list(zip(range(1, len(courses)+1), courses, course_prices)),
# MAGIC         schema="product_id long, product_name string, base_price double"
# MAGIC     )
# MAGIC
# MAGIC     day_offset_expr = f"cast(pmod(abs(xxhash64(id + 1, {seed_orders})), 365) as int)"
# MAGIC     sec_of_day_expr = f"(cast(pmod(abs(xxhash64(id + 1, {seed_orders}+1)), 86399) as int) + 1)"
# MAGIC     order_ts_expr = (
# MAGIC         f"to_timestamp(from_unixtime(unix_timestamp('2025-01-01 00:00:00')"
# MAGIC         f" + {day_offset_expr} * 86400 + {sec_of_day_expr}))"
# MAGIC     )
# MAGIC
# MAGIC     gen_orders = (
# MAGIC         dg.DataGenerator(spark, name="orders_gen", rows=orders_n, partitions=4, randomSeedMethod="hash_fieldname")
# MAGIC         .withIdOutput()
# MAGIC         .withColumn("order_id", T.LongType(), expr="id + 1")
# MAGIC         .withColumn("user_id", T.LongType(), expr=f"pmod(abs(xxhash64(id + 1, {seed_users})), {users_n}) + 1")
# MAGIC         .withColumn("order_date", T.TimestampType(), expr=order_ts_expr)
# MAGIC         .withColumn("payment_method", T.StringType(), values=payment_methods, weights=payment_method_distribution)
# MAGIC         .withColumn("installments", T.IntegerType(), values=installments, weights=installment_distribution)
# MAGIC     )
# MAGIC
# MAGIC     orders_valid = gen_orders.build().select("order_id","user_id","order_date","payment_method","installments")
# MAGIC
# MAGIC     orders_with_country = (
# MAGIC         orders_valid.join(users_df.select("user_id","country_code"), "user_id", "left")
# MAGIC     )
# MAGIC
# MAGIC     decide_parcelar = F.pmod(F.abs(F.xxhash64(F.col("order_id"), F.lit(seed_orders + 7))), F.lit(100)).cast("int")
# MAGIC     parcelas_hash   = F.pmod(F.abs(F.xxhash64(F.col("order_id"), F.lit(seed_orders + 8))), F.lit(11)).cast("int") 
# MAGIC
# MAGIC     latam_countries = F.array(F.lit("BR"), F.lit("MX"), F.lit("AR"))
# MAGIC
# MAGIC     installments_realistic = (
# MAGIC         F.when(
# MAGIC             (F.col("payment_method") == "credit_card") &
# MAGIC             (F.array_contains(latam_countries, F.col("country_code"))),
# MAGIC             F.when(decide_parcelar < 30, F.lit(1))
# MAGIC              .otherwise(parcelas_hash + 2)
# MAGIC         )
# MAGIC         .otherwise(F.lit(1))
# MAGIC     )
# MAGIC
# MAGIC     orders_df = (
# MAGIC         orders_with_country
# MAGIC         .withColumn("installments", installments_realistic.cast("int"))
# MAGIC     )
# MAGIC
# MAGIC     orphans_ids = (
# MAGIC         orders_df.select("order_id", F.xxhash64("order_id", F.lit(seed_orders)).alias("h"))
# MAGIC                  .orderBy("h").limit(invalid_count)
# MAGIC                  .select("order_id").withColumn("is_orphan", F.lit(True))
# MAGIC     )
# MAGIC
# MAGIC     orders_df = (
# MAGIC         orders_df.join(orphans_ids, "order_id", "left")
# MAGIC                  .withColumn("user_id",
# MAGIC                      F.when(F.col("is_orphan"), F.lit(invalid_user_id)).otherwise(F.col("user_id")).cast("long"))
# MAGIC                  .drop("is_orphan", "country_code")
# MAGIC     )
# MAGIC
# MAGIC     weights_lines = [20, 25, 30, 15, 10]
# MAGIC     cum_lines = [sum(weights_lines[:i+1]) for i in range(len(weights_lines))]
# MAGIC     bucket_lines = F.pmod(F.xxhash64(F.col("order_id"), F.lit(seed_lines)), F.lit(100)).cast("int")
# MAGIC
# MAGIC     order_with_count = (
# MAGIC         orders_df.select("order_id","user_id")
# MAGIC         .withColumn("line_count",
# MAGIC             F.when(bucket_lines < cum_lines[0], F.lit(1))
# MAGIC              .when(bucket_lines < cum_lines[1], F.lit(2))
# MAGIC              .when(bucket_lines < cum_lines[2], F.lit(3))
# MAGIC              .when(bucket_lines < cum_lines[3], F.lit(4))
# MAGIC              .otherwise(F.lit(5)))
# MAGIC     )
# MAGIC     order_lines = order_with_count.withColumn("line_no", F.explode(F.sequence(F.lit(1), F.col("line_count")))).drop("line_count")
# MAGIC
# MAGIC     w_user = Window.partitionBy("user_id").orderBy(F.col("order_id").asc(), F.col("line_no").asc())
# MAGIC     order_lines_ranked = order_lines.withColumn("user_line_rank", F.row_number().over(w_user)).filter(F.col("user_line_rank") <= 5)
# MAGIC
# MAGIC     h = [F.xxhash64(F.col("user_id"), F.lit(i), F.lit(seed_pickers)) for i in range(1, 6)]
# MAGIC     perm_array = F.array_sort(F.array(
# MAGIC         F.struct(h[0].alias("h"), F.lit(1).alias("pid")),
# MAGIC         F.struct(h[1].alias("h"), F.lit(2).alias("pid")),
# MAGIC         F.struct(h[2].alias("h"), F.lit(3).alias("pid")),
# MAGIC         F.struct(h[3].alias("h"), F.lit(4).alias("pid")),
# MAGIC         F.struct(h[4].alias("h"), F.lit(5).alias("pid"))
# MAGIC     ))
# MAGIC
# MAGIC     order_lines_with_product = (
# MAGIC         order_lines_ranked
# MAGIC         .withColumn("product_struct", F.element_at(perm_array, F.col("user_line_rank")))
# MAGIC         .withColumn("product_id", F.col("product_struct.pid").cast("long"))
# MAGIC         .drop("product_struct")
# MAGIC     )
# MAGIC
# MAGIC     cum_disc = [sum(discount_distribution[:i+1]) for i in range(len(discount_distribution))]
# MAGIC     bucket_disc = F.pmod(F.xxhash64(F.col("order_id"), F.col("line_no"), F.lit(seed_details+1)), F.lit(100)).cast("int")
# MAGIC     discount_col = (
# MAGIC         F.when(bucket_disc < cum_disc[0], F.lit(discounts[0]))
# MAGIC          .when(bucket_disc < cum_disc[1], F.lit(discounts[1]))
# MAGIC          .otherwise(F.lit(discounts[2]))
# MAGIC     )
# MAGIC
# MAGIC     details_df = (
# MAGIC         order_lines_with_product
# MAGIC         .withColumn("discount", discount_col.cast("double"))
# MAGIC         .join(products_df.select("product_id","base_price"), "product_id", "inner")
# MAGIC         .join(orders_df.select("order_id","installments"), "order_id", "inner")
# MAGIC         .withColumn("unit_price", F.round((F.col("base_price") * (1 - F.col("discount"))) / F.col("installments"), 2))
# MAGIC         .select("order_id","product_id","unit_price","discount")
# MAGIC     )
# MAGIC
# MAGIC     write_all_formats(users_df, main_temp_path, "users")
# MAGIC     write_all_formats(products_df, main_temp_path, "products", one_file=True)
# MAGIC     write_all_formats(orders_df.select("order_id","user_id","order_date","payment_method","installments"),
# MAGIC                       main_temp_path, "orders")
# MAGIC     write_all_formats(details_df,  main_temp_path, "order_details")
# MAGIC
# MAGIC     create_complex_data_files(chapter_number, main_temp_path)
# MAGIC     create_enumerated_files(f"{main_temp_path}/array_products_by_order/json/", "array_products_by_order", "json")
# MAGIC     create_enumerated_files(f"{main_temp_path}/order_details_dict/json/", "order_details_dict", "json")
# MAGIC
# MAGIC     dbutils.fs.rm(main_temp_path, True)

# COMMAND ----------

generate_and_write_to_volume(chapter_number="05")
