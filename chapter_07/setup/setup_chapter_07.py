# Databricks notebook source
# MAGIC %pip install dbldatagen
# MAGIC
# MAGIC import dbldatagen as dg
# MAGIC from pyspark.sql import functions as F, types as T
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.types import *
# MAGIC import os
# MAGIC import decimal
# MAGIC
# MAGIC # def drop_tables():
# MAGIC #     tables = [
# MAGIC #         ""
# MAGIC #     ]
# MAGIC #     for table in tables:
# MAGIC #         spark.sql(f"DROP TABLE IF EXISTS {table}")
# MAGIC
# MAGIC def drop_volume(chapter_number: str):
# MAGIC     spark.sql(f"drop volume if exists workspace.default.chapter_{chapter_number}")
# MAGIC
# MAGIC def create_volume(chapter_number: str):
# MAGIC     spark.sql(f"create volume if not exists workspace.default.chapter_{chapter_number}")
# MAGIC
# MAGIC def generate_and_write_to_volume(
# MAGIC     chapter_number: str,
# MAGIC     target_files: int = 60,
# MAGIC     target_file_size_bytes: int = 14831751,
# MAGIC     sample_rows: int = 10000,
# MAGIC     ):
# MAGIC
# MAGIC     drop_volume(chapter_number)
# MAGIC     create_volume(chapter_number)
# MAGIC
# MAGIC     target_files = max(50, min(80, target_files))
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
# MAGIC                 baseColumn=["hash"]
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
# MAGIC     avg_row_size = sample_df.toJSON().map(lambda r: len(r)).mean()
# MAGIC     avg_row_size = avg_row_size if avg_row_size and avg_row_size > 0 else 1
# MAGIC
# MAGIC     total_rows = int((target_files * target_file_size_bytes) / avg_row_size)
# MAGIC     total_rows = max(total_rows, target_files * 1000)
# MAGIC
# MAGIC     df = _build_df(total_rows)
# MAGIC     output_path = f"/Volumes/workspace/default/{chapter_number}/api_stream_data"
# MAGIC
# MAGIC     (df.repartition(target_files)
# MAGIC        .write
# MAGIC        .format('json')
# MAGIC        .mode('overwrite')
# MAGIC        .save(output_path))
# MAGIC
# MAGIC     return output_path
