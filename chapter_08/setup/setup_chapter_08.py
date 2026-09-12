# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %pip install dbldatagen
# MAGIC
# MAGIC import dbldatagen as dg
# MAGIC from pyspark.sql import functions as F, types as T
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.types import *
# MAGIC import os
# MAGIC import decimal
# MAGIC
# MAGIC def drop_volume(chapter_number: str):
# MAGIC     spark.sql(f"drop volume if exists workspace.default.chapter_{chapter_number}")
# MAGIC
# MAGIC def create_volume(chapter_number: str):
# MAGIC     spark.sql(f"create volume if not exists workspace.default.chapter_{chapter_number}")
# MAGIC
# MAGIC def drop_delta_example_table():
# MAGIC     spark.sql(f"DROP TABLE IF EXISTS workspace.default.tb_api_stream_data")
# MAGIC
# MAGIC def cleanup_resources():
# MAGIC     for stream in spark.streams.active:
# MAGIC         stream.stop()
# MAGIC     drop_volume("07")
# MAGIC     spark.sql("DROP TABLE IF EXISTS workspace.default.tb_api_stream_data")
# MAGIC
# MAGIC def generate_and_write_to_volume(
# MAGIC     chapter_number: str,
# MAGIC     target_files: int = 100,
# MAGIC     target_file_size_bytes: int = 10485760,
# MAGIC     sample_rows: int = 10000,
# MAGIC     mobile_followup_fraction: float = 0.25,
# MAGIC     ):
# MAGIC
# MAGIC     drop_volume(chapter_number)
# MAGIC     create_volume(chapter_number)
# MAGIC
# MAGIC     target_files = max(50, min(120, target_files))
# MAGIC
# MAGIC     countries = [
# MAGIC         ("US","United States"), ("BR","Brazil"), ("IN","India"), ("GB","United Kingdom"),
# MAGIC         ("DE","Germany"), ("FR","France"), ("ES","Spain"), ("CA","Canada"),
# MAGIC         ("AU","Australia"), ("MX","Mexico"), ("AR","Argentina"), ("CO","Colombia"),
# MAGIC         ("PT","Portugal"), ("IT","Italy"), ("NL","Netherlands"), ("SE","Sweden"),
# MAGIC         ("JP","Japan"), ("KR","South Korea"), ("ZA","South Africa"), ("NG","Nigeria")
# MAGIC     ]
# MAGIC     country_names = [name for _, name in countries]
# MAGIC
# MAGIC     user_professions = ['Data Engineer','Data Architect','Data Analyst','Data Scientist','Developer']
# MAGIC     courses = [
# MAGIC         'Building a Data Lakehouse with SQL and DDP',
# MAGIC         'Mastering SQL on Databricks',
# MAGIC         'Data Heroes Mentorship Program',
# MAGIC         'Book Club - Spark the Definitive Guide',
# MAGIC         'Book Club - Delta Lake the Definitive Guide' 
# MAGIC     ]
# MAGIC
# MAGIC     course_prices = [
# MAGIC         decimal.Decimal('99.90'), 
# MAGIC         decimal.Decimal('247.90'), 
# MAGIC         decimal.Decimal('997.90'), 
# MAGIC         decimal.Decimal('297.90'), 
# MAGIC         decimal.Decimal('297.90')
# MAGIC     ]
# MAGIC     
# MAGIC     course_distribution = [25,20,15,25,15]
# MAGIC
# MAGIC     mail_providers = ['@gmail.com','@hotmail.com','@outlook.com']
# MAGIC     mail_distribution = [60, 25, 15]
# MAGIC     
# MAGIC     genders = ['M','F']
# MAGIC     gender_distribution = [49, 51]
# MAGIC     
# MAGIC     payment_methods = ['credit_card','debit_card','paypal','apple_pay','google_pay']
# MAGIC     payment_method_distribution = [55, 15, 12, 8, 10]
# MAGIC     
# MAGIC     discounts = [
# MAGIC         decimal.Decimal('0.05'),
# MAGIC         decimal.Decimal('0.10'),
# MAGIC         decimal.Decimal('0.15')
# MAGIC     ]
# MAGIC     discount_distribution = [50, 35, 15]
# MAGIC     
# MAGIC     installments = [1, 2, 3, 10, 12]
# MAGIC     installment_distribution = [20, 15, 5, 30, 30]
# MAGIC
# MAGIC     access_points = ['iphone','android','chrome','safari','firefox','unknown']
# MAGIC     access_point_distribution = [30,20,20,10,15,5]
# MAGIC
# MAGIC     ages = [18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50]
# MAGIC
# MAGIC     def _build_df(rows):
# MAGIC         df_test = (
# MAGIC             dg.DataGenerator(
# MAGIC                 spark,
# MAGIC                 name="data_set",
# MAGIC                 rows=rows,
# MAGIC                 uniqueValues=rows,
# MAGIC                 partitions=8,
# MAGIC                 randomSeedMethod='hash_fieldname'
# MAGIC             )
# MAGIC             .withIdOutput()
# MAGIC             .withColumn(
# MAGIC                 "access_id",
# MAGIC                 LongType(),
# MAGIC                 minValue=0,
# MAGIC                 uniqueValues=rows,
# MAGIC                 omit=True
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "access_date",
# MAGIC                 "timestamp",
# MAGIC                 begin="2024-06-01 00:00:00",
# MAGIC                 end="2026-07-01 23:59:59",
# MAGIC                 interval="247 seconds",
# MAGIC                 baseColumn=["access_id"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "ip_address",
# MAGIC                 StringType(),
# MAGIC                 expr="""case 
# MAGIC                     when rand() <= 0.01 then '999.999.999.999' 
# MAGIC                     else format_string('%d.%d.%d.%d', int(rand() * 255), int(rand() * 255), int(rand() * 255), int(rand() * 255)) 
# MAGIC                 end""",
# MAGIC                 baseColumn=["access_id"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "access_point",
# MAGIC                 StringType(),
# MAGIC                 values=access_points,
# MAGIC                 weights=access_point_distribution,
# MAGIC                 baseColumn=["access_id"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "hash",
# MAGIC                 StringType(),
# MAGIC                 omit=True,
# MAGIC                 expr="md5(concat(cast(rand() as string),access_id,access_date,ip_address,access_point))",
# MAGIC                 baseColumn=["access_id","access_date","ip_address","access_point"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "user_name",
# MAGIC                 StringType(),
# MAGIC                 omit=True,
# MAGIC                 expr="concat('User ', concat(left(hash,4),substr(hash,14,2),right(hash,4)))",
# MAGIC                 baseColumn=["hash"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "mail_provider",
# MAGIC                 "string",
# MAGIC                 random=True,
# MAGIC                 omit=True,
# MAGIC                 weights=mail_distribution,
# MAGIC                 values=mail_providers,
# MAGIC                 baseColumn=["hash"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "user_email",
# MAGIC                 StringType(),
# MAGIC                 omit=True,
# MAGIC                 expr="concat('user_', concat(left(hash,4),substr(hash,14,2),right(hash,4)), mail_provider)",
# MAGIC                 baseColumn=["hash", "mail_provider"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "gender",
# MAGIC                 "string",
# MAGIC                 random=True,
# MAGIC                 omit=True,
# MAGIC                 weights=gender_distribution,
# MAGIC                 values=genders,
# MAGIC                 baseColumn=["hash"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "profession",
# MAGIC                 "string",
# MAGIC                 random=True,
# MAGIC                 omit=True,
# MAGIC                 values=user_professions
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "age",
# MAGIC                 "string",
# MAGIC                 random=True,
# MAGIC                 omit=True,
# MAGIC                 values=ages
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "country",
# MAGIC                 StringType(),
# MAGIC                 omit=True,
# MAGIC                 values=country_names,
# MAGIC                 baseColumn="access_id"
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "product_name",
# MAGIC                 StringType(),
# MAGIC                 omit=True,
# MAGIC                 values=courses,
# MAGIC                 weights=course_distribution,
# MAGIC                 baseColumn="access_id"
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "price",
# MAGIC                 DecimalType(9,2),
# MAGIC                 omit=True,
# MAGIC                 values=course_prices,
# MAGIC                 weights=course_distribution,
# MAGIC                 baseColumn=["access_id"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "price_string",
# MAGIC                 StringType(),
# MAGIC                 omit=True,
# MAGIC                 expr="""case 
# MAGIC                     when product_name = 'Building a Data Lakehouse with SQL and DDP' 
# MAGIC                         then '$ 99.90' 
# MAGIC                     when product_name = 'Mastering SQL on Databricks' 
# MAGIC                         then '$ 247.90' 
# MAGIC                     when product_name = 'Data Heroes Mentorship Program' 
# MAGIC                         then '$ 997.90'
# MAGIC                     when product_name = 'Book Club - Spark the Definitive Guide'
# MAGIC                         then '$ 297.90'
# MAGIC                     when product_name = 'Book Club - Delta Lake the Definitive Guide'
# MAGIC                         then '$ 297.90'
# MAGIC                     else '$ 0.00' end""",
# MAGIC                 baseColumn=["product_name"]
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "discount",
# MAGIC                 DecimalType(9,2),
# MAGIC                 omit=True,
# MAGIC                 values=discounts,
# MAGIC                 nullable=True,
# MAGIC                 percentNulls=0.7,
# MAGIC                 weights=discount_distribution,
# MAGIC                 baseColumn="access_id"
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "discount_amount",
# MAGIC                 DecimalType(9,2),
# MAGIC                 omit=True,
# MAGIC                 expr="cast((price * discount) as numeric(9,2))",
# MAGIC                 baseColumn=["access_id",'price', 'discount']
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "final_price",
# MAGIC                 DecimalType(9,2),
# MAGIC                 omit=True,
# MAGIC                 expr="cast((price - coalesce(discount_amount,0)) as numeric(9,2))",
# MAGIC                 baseColumn=["access_id",'discount_amount', 'price']
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "payment_method",
# MAGIC                 StringType(),
# MAGIC                 omit=True,
# MAGIC                 values=payment_methods,
# MAGIC                 weights=payment_method_distribution,
# MAGIC                 baseColumn="access_id"
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "base_installments",
# MAGIC                 LongType(),
# MAGIC                 omit=True,
# MAGIC                 values=installments,
# MAGIC                 weights=installment_distribution,
# MAGIC                 baseColumn="access_id"
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "installments",
# MAGIC                 StringType(),
# MAGIC                 omit=True,
# MAGIC                 expr="""case when payment_method != 'credit_card'
# MAGIC                             then 1 
# MAGIC                         else base_installments end""",
# MAGIC                 baseColumn=["access_id",'base_installments','payment_method']
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "installment_value",
# MAGIC                 DecimalType(9,2),
# MAGIC                 omit=True,
# MAGIC                 expr="cast((final_price/installments) as numeric(9,2))",
# MAGIC                 baseColumn=["access_id",'installments','final_price']
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "user_info",
# MAGIC                 StructType([
# MAGIC                     StructField('name',StringType()),
# MAGIC                     StructField('age',StringType()),
# MAGIC                     StructField('gender',StringType()),
# MAGIC                     StructField('email',StringType()),
# MAGIC                     StructField('profession',StringType()),
# MAGIC                     StructField('country',StringType())
# MAGIC                 ]),
# MAGIC                 omit=True,
# MAGIC                 expr="""
# MAGIC                     named_struct(
# MAGIC                         'name', user_name, 
# MAGIC                         'age', age,
# MAGIC                         'gender', gender,
# MAGIC                         'email', user_email,
# MAGIC                         'profession', profession,
# MAGIC                         'country', country
# MAGIC                     )""",
# MAGIC                 nullable=True,
# MAGIC                 percentNulls=0.7,
# MAGIC                 baseColumn=['user_name','age','gender','user_email','profession', 'country']
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "product_info",
# MAGIC                 StructType([
# MAGIC                     StructField('product_name',StringType()),
# MAGIC                     StructField('price',StringType())
# MAGIC                 ]),
# MAGIC                 omit=True,
# MAGIC                 expr="""
# MAGIC                     case when user_info is not null then 
# MAGIC                     named_struct(
# MAGIC                         'product_name', product_name, 
# MAGIC                         'price', price_string
# MAGIC                     )
# MAGIC                     else null end""",
# MAGIC                 nullable=True,
# MAGIC                 percentNulls=0.7,
# MAGIC                 baseColumn=['user_info','product_name','price_string']
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "payment_info",
# MAGIC                 StructType([
# MAGIC                     StructField('discount',DecimalType(9,2)),
# MAGIC                     StructField('final_price',DecimalType(9,2)),
# MAGIC                     StructField('payment_method',StringType()),
# MAGIC                     StructField('installments',LongType()),
# MAGIC                     StructField('installment_value',DecimalType(9,2))
# MAGIC                 ]),
# MAGIC                 omit=True,
# MAGIC                 expr="""
# MAGIC                     case when product_info is not null then 
# MAGIC                     named_struct(
# MAGIC                         'discount', discount, 
# MAGIC                         'final_price', final_price,
# MAGIC                         'payment_method', payment_method,
# MAGIC                         'installments', installments,
# MAGIC                         'installment_value', installment_value
# MAGIC                     ) else null end""",
# MAGIC                 baseColumn=['product_info','discount','final_price','payment_method','installments','installment_value']
# MAGIC             )
# MAGIC             .withColumn(
# MAGIC                 "payload",
# MAGIC                 StructType([
# MAGIC                     StructField('user_info',StructType([
# MAGIC                                                     StructField('name',StringType()),
# MAGIC                                                     StructField('age',StringType()),
# MAGIC                                                     StructField('gender',StringType()),
# MAGIC                                                     StructField('email',StringType()),
# MAGIC                                                     StructField('profession',StringType()),
# MAGIC                                                     StructField('country',StringType())
# MAGIC                                                 ])),
# MAGIC                     StructField('product_info',StructType([
# MAGIC                                                     StructField('product_name',StringType()),
# MAGIC                                                     StructField('price',StringType())
# MAGIC                                                 ])),
# MAGIC                     StructField('payment_info',StructType([
# MAGIC                                                     StructField('discount',DecimalType(9,2)),
# MAGIC                                                     StructField('final_price',DecimalType(9,2)),
# MAGIC                                                     StructField('payment_method',StringType()),
# MAGIC                                                     StructField('installments',LongType()),
# MAGIC                                                     StructField('installment_value',DecimalType(9,2))
# MAGIC                                                 ]))
# MAGIC                 ]),
# MAGIC                 expr="""
# MAGIC                     named_struct(
# MAGIC                         'user_info', user_info,
# MAGIC                         'product_info', product_info,
# MAGIC                         'payment_info', payment_info
# MAGIC                     )""",
# MAGIC                 baseColumn=['user_info','product_info','payment_info']
# MAGIC             )
# MAGIC         )
# MAGIC
# MAGIC         return df_test.build().drop('id')
# MAGIC
# MAGIC     sample_rows = max(1000, sample_rows)
# MAGIC     sample_df = _build_df(sample_rows)
# MAGIC     avg_row_size = sample_df.select(F.avg(F.length(F.to_json(F.struct(*sample_df.columns))))).collect()[0][0]
# MAGIC     avg_row_size = avg_row_size if avg_row_size and avg_row_size > 0 else 1
# MAGIC
# MAGIC     total_rows = int((target_files * target_file_size_bytes) / avg_row_size)
# MAGIC     total_rows = max(total_rows, target_files * 1000)
# MAGIC
# MAGIC     df = _build_df(total_rows)
# MAGIC
# MAGIC     # Chapter 08 cross-device follow-ups: a fraction of the web accesses
# MAGIC     # (chrome/firefox) gets a correlated mobile access from the same
# MAGIC     # ip_address 5-40 minutes later, so stream-stream interval joins
# MAGIC     # find real matches. Set mobile_followup_fraction=0 to disable.
# MAGIC     if mobile_followup_fraction and mobile_followup_fraction > 0:
# MAGIC         df_followup = (
# MAGIC             df.filter(F.col("access_point").isin("chrome", "firefox"))
# MAGIC               .sample(fraction=mobile_followup_fraction, seed=42)
# MAGIC               .withColumn(
# MAGIC                   "access_date",
# MAGIC                   F.expr("timestampadd(MINUTE, cast(rand() * 35 + 5 as int), access_date)")
# MAGIC               )
# MAGIC               .withColumn(
# MAGIC                   "access_point",
# MAGIC                   F.when(F.rand(seed=7) < 0.7, F.lit("iphone")).otherwise(F.lit("android"))
# MAGIC               )
# MAGIC         )
# MAGIC         df = df.unionByName(df_followup)
# MAGIC
# MAGIC     output_path = f"/Volumes/workspace/default/chapter_{chapter_number}/api_stream_data"
# MAGIC
# MAGIC     (df.repartition(target_files)
# MAGIC        .write
# MAGIC        .format('json')
# MAGIC        .mode('overwrite')
# MAGIC        .save(output_path))
# MAGIC
# MAGIC     # create a Delta table having the generated json files as source
# MAGIC     (spark.read.json(output_path)
# MAGIC        .write
# MAGIC        .format('delta')
# MAGIC        .mode('overwrite')
# MAGIC        .saveAsTable('workspace.default.tb_api_stream_data'))
# MAGIC
# MAGIC     return output_path
# MAGIC
# MAGIC def drop_tables():
# MAGIC     tables = [
# MAGIC         "workspace.default.users",
# MAGIC         "workspace.default.orders",
# MAGIC         "workspace.default.order_details",
# MAGIC         "workspace.default.products"
# MAGIC     ]
# MAGIC     for table in tables:
# MAGIC         spark.sql(f"DROP TABLE IF EXISTS {table}")
# MAGIC
# MAGIC def copy_file_workspace_to_volume(source_path: str, target_path: str):
# MAGIC     import os
# MAGIC     try:
# MAGIC         target_dir = os.path.dirname(target_path)
# MAGIC         os.makedirs(target_dir, exist_ok=True)
# MAGIC         with open(source_path, "rb") as src:
# MAGIC             data = src.read()
# MAGIC         with open(target_path, "wb") as dst:
# MAGIC             dst.write(data)
# MAGIC     except Exception as e:
# MAGIC         print(f"Error copying file: {e}")
# MAGIC
# MAGIC def create_enumerated_files(
# MAGIC     temp_path: str, table_name: str, file_format: str
# MAGIC     ):
# MAGIC     data_files = [f for f in dbutils.fs.ls(temp_path) if f.name.startswith("part-")]
# MAGIC     for index, file_name in enumerate(data_files, start=1):
# MAGIC         path = temp_path.replace('temp/', '')
# MAGIC         dbutils.fs.mv(file_name.path, f"{path}{table_name}_0{index}.{file_format}")
# MAGIC
# MAGIC def write_all_formats(df, main_temp_path: str, table_name: str, one_file: bool = False):
# MAGIC     dict_types = {"parquet": {"options": {}}}
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
# MAGIC def generate_and_write_to_tables(
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
# MAGIC     drop_volume(chapter_number)
# MAGIC     drop_tables()
# MAGIC     create_volume(chapter_number)
# MAGIC     main_temp_path = f"/Volumes/workspace/default/chapter_{chapter_number}/temp"
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
# MAGIC         
# MAGIC     ]
# MAGIC     course_prices = [
# MAGIC         decimal.Decimal('99.90'), 
# MAGIC         decimal.Decimal('247.90'), 
# MAGIC         decimal.Decimal('997.90'), 
# MAGIC         decimal.Decimal('297.90'), 
# MAGIC         decimal.Decimal('297.90')
# MAGIC     ]
# MAGIC     
# MAGIC     course_categories = [
# MAGIC         "beginner",
# MAGIC         "intermediate",
# MAGIC         "advanced",
# MAGIC         "intermediate",
# MAGIC         "intermediate"
# MAGIC     ]
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
# MAGIC     gen_users = (
# MAGIC         dg.DataGenerator(
# MAGIC             spark, name="users_gen", rows=users_n, partitions=4, randomSeedMethod="hash_fieldname"
# MAGIC         )
# MAGIC         .withIdOutput()
# MAGIC         .withColumn("user_id", T.LongType(), expr="id + 1")
# MAGIC         .withColumn(
# MAGIC             "email_provider", 
# MAGIC             T.StringType(), values=email_providers, weights=email_distribution, baseColumn=["user_id"]
# MAGIC         )
# MAGIC         .withColumn(
# MAGIC             "email", T.StringType(),
# MAGIC             expr="concat('user_', lpad(cast(user_id as string), 6, '0'), email_provider)",
# MAGIC             baseColumn=["user_id", "email_provider"]
# MAGIC         )
# MAGIC         .withColumn(
# MAGIC             "gender", 
# MAGIC             T.StringType(), values=gender, weights=gender_distribution, baseColumn=["user_id"],
# MAGIC             nullable=True, percentNulls=0.08
# MAGIC         )
# MAGIC         .withColumn(
# MAGIC             "profession", 
# MAGIC             T.StringType(), values=user_professions, baseColumn=["user_id"],
# MAGIC             nullable=True, percentNulls=0.05)
# MAGIC         .withColumn(
# MAGIC             "country_code", 
# MAGIC             T.StringType(), values=country_codes, weights=country_weights, 
# MAGIC             baseColumn=["user_id"]
# MAGIC         )
# MAGIC     )
# MAGIC     users_df = gen_users.build().select("user_id","email","gender","profession","country_code")
# MAGIC     country_map = F.create_map([F.lit(x) for kv in countries for x in kv])
# MAGIC     users_df = users_df.withColumn("country", country_map[F.col("country_code")]) \
# MAGIC                        .select("user_id","email","gender","profession","country_code","country")
# MAGIC
# MAGIC     products_df = spark.createDataFrame(
# MAGIC         list(zip(range(1, len(courses)+1), courses, course_prices, course_categories)),
# MAGIC         schema="product_id long, product_name string, base_price decimal(10,2), level string"
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
# MAGIC     dbutils.fs.rm(main_temp_path, True)
# MAGIC
# MAGIC     users_df.write.format('delta').mode('overwrite').saveAsTable('workspace.default.users')
# MAGIC     products_df.write.format('delta').mode('overwrite').saveAsTable('workspace.default.products')
# MAGIC     orders_df.select("order_id","user_id","order_date","payment_method","installments").write.format('delta').mode('overwrite').saveAsTable('workspace.default.orders')
# MAGIC     details_df.write.format('delta').mode('overwrite').saveAsTable('workspace.default.order_details')

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 5)

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE FUNCTION calculate_discount(
        value DECIMAL(9,2),
        discount DECIMAL(9,2)
    )
    RETURNS DECIMAL(9,2)
        RETURN CAST(value * (1 - discount) AS decimal(9,2))
""")
