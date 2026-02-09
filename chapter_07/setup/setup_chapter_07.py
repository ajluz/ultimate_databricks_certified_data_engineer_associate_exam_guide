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
# MAGIC     run_as_batch: bool,
# MAGIC     chapter_number: str
# MAGIC     ):
# MAGIC
# MAGIC     drop_volume(chapter_number)
# MAGIC     create_volume(chapter_number)
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
# MAGIC     course_categories = [
# MAGIC         "beginner",
# MAGIC         "intermediate",
# MAGIC         "advanced",
# MAGIC         "intermediate",
# MAGIC         "intermediate"
# MAGIC     ]
# MAGIC
# MAGIC     mail_providers = ['@gmail.com','@hotmail.com','@outlook.com']
# MAGIC     mail_distribution = [60, 25, 15]
# MAGIC     
# MAGIC     gender = ['M','F']
# MAGIC     gender_distribution = [49, 51]
# MAGIC     
# MAGIC     payment_methods = ['credit_card','debit_card','paypal','apple_pay','google_pay']
# MAGIC     payment_method_distribution = [55, 15, 12, 8, 10]
# MAGIC     
# MAGIC     discounts = [0.05, 0.10, 0.15]
# MAGIC     discount_distribution = [50, 35, 15]
# MAGIC     
# MAGIC     installments = [1, 2, 3, 10, 12]
# MAGIC     installment_distribution = [20, 15, 5, 30, 30]
# MAGIC
# MAGIC     access_point = ['iphone','android','chrome','safari','firefox','unknown']
# MAGIC     access_point_distribution = [30,20,20,10,15,5]
# MAGIC
# MAGIC     age = [18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50]
# MAGIC
# MAGIC     df_test = (
# MAGIC         dg.DataGenerator(spark, 
# MAGIC                         name="data_set", 
# MAGIC                         rows=number_unique_rows,
# MAGIC                         uniqueValues=number_unique_rows,
# MAGIC                         partitions=8, 
# MAGIC                         randomSeedMethod='hash_fieldname')
# MAGIC         .withIdOutput()
# MAGIC
# MAGIC         .withColumn("order_id", 
# MAGIC                     LongType(), 
# MAGIC                     minValue=0,
# MAGIC                     uniqueValues=number_unique_rows, 
# MAGIC                     omit=True)
# MAGIC         
# MAGIC         .withColumn("access_date", 
# MAGIC                     "timestamp",
# MAGIC                     begin="2024-06-01 00:00:00",
# MAGIC                     end="2026-07-01 23:59:59",
# MAGIC                     interval="247 seconds", 
# MAGIC                     baseColumn=["order_id"])
# MAGIC         
# MAGIC         .withColumn("ip_address", 
# MAGIC                     StringType(),
# MAGIC                     expr="""case 
# MAGIC                         when rand() <= 0.01 then '999.999.999.999' 
# MAGIC                         else format_string('%d.%d.%d.%d', int(rand() * 255), int(rand() * 255), int(rand() * 255), int(rand() * 255)) 
# MAGIC                     end""",
# MAGIC                     baseColumn=["order_id"])
# MAGIC         
# MAGIC         .withColumn("access_point", 
# MAGIC                     StringType(),
# MAGIC                     values=access_point,
# MAGIC                     weights=access_point_distribution,
# MAGIC                     baseColumn=["order_id"])
# MAGIC         
# MAGIC         .withColumn("hash", 
# MAGIC                     StringType(),
# MAGIC                     omit=True, 
# MAGIC                     expr="md5(concat(cast(rand() as string),order_id,access_date,ip_address,access_point))",
# MAGIC                     baseColumn=["order_id","access_date","ip_address","access_point"])
# MAGIC         
# MAGIC         .withColumn("user_name", 
# MAGIC                     StringType(),
# MAGIC                     omit=True, 
# MAGIC                     expr="concat('User ', concat(left(hash,4),substr(hash,14,2),right(hash,4)))",
# MAGIC                     baseColumn=["hash"])
# MAGIC         
# MAGIC         .withColumn("mail_provider", 
# MAGIC                     "string", 
# MAGIC                     random=True, 
# MAGIC                     omit=True, 
# MAGIC                     weights=mail_distribution,
# MAGIC                     values=mail_providers)
# MAGIC         
# MAGIC         .withColumn("user_email", 
# MAGIC                     StringType(),
# MAGIC                     omit=True, 
# MAGIC                     expr="concat('user_', concat(left(hash,4),substr(hash,14,2),right(hash,4)), mail_provider)",
# MAGIC                     baseColumn=["hash"])
# MAGIC         
# MAGIC         .withColumn("gender", 
# MAGIC                     "string", 
# MAGIC                     random=True, 
# MAGIC                     omit=True, 
# MAGIC                     weights=gender_distribution,
# MAGIC                     values=gender)
# MAGIC         
# MAGIC         .withColumn("profession", 
# MAGIC                     "string", 
# MAGIC                     random=True, 
# MAGIC                     omit=True, 
# MAGIC                     values=user_professions)
# MAGIC         
# MAGIC         .withColumn("age", 
# MAGIC                     "string", 
# MAGIC                     random=True, 
# MAGIC                     omit=True, 
# MAGIC                     values=age)
# MAGIC         
# MAGIC         .withColumn("country", 
# MAGIC                     StringType(),
# MAGIC                     omit=True, 
# MAGIC                     values=countries,
# MAGIC                     baseColumn="order_id")
# MAGIC         
# MAGIC         .withColumn("product_name", 
# MAGIC                     StringType(),
# MAGIC                     omit=True, 
# MAGIC                     values=courses,
# MAGIC                     weights=course_distribution,
# MAGIC                     baseColumn="order_id")
# MAGIC         
# MAGIC         .withColumn("price", 
# MAGIC                     DecimalType(9,2),
# MAGIC                     omit=True, 
# MAGIC                     values=course_prices,
# MAGIC                     weights=course_distribution,
# MAGIC                     baseColumn=["order_id"])
# MAGIC
# MAGIC         .withColumn("price_string", 
# MAGIC                     StringType(),
# MAGIC                     omit=True, 
# MAGIC                     expr="""case 
# MAGIC                         when product_name = 'Building a Data Lakehouse with SQL and DDP' 
# MAGIC                             then '$ 99.90' 
# MAGIC                         when product_name = 'Mastering SQL on Databricks' 
# MAGIC                             then '$ 247.90' 
# MAGIC                         when product_name = 'Data Heroes Mentorship Program' 
# MAGIC                             then '$ 997.90'
# MAGIC                         when product_name = 'Book Club - Spark the Definitive Guide'
# MAGIC                             then '$ 297.90'
# MAGIC                         when product_name = 'Book Club - Delta Lake the Definitive Guide'
# MAGIC                             then '$ 297.90'
# MAGIC                         else '$ 0,00' end""",
# MAGIC                     baseColumn=["product_name"])
# MAGIC         
# MAGIC         .withColumn("discount", 
# MAGIC                     DecimalType(9,2),
# MAGIC                     omit=True, 
# MAGIC                     values=discounts, 
# MAGIC                     nullable=True, 
# MAGIC                     percentNulls=0.7, 
# MAGIC                     weights=discount_distribution,
# MAGIC                     baseColumn="order_id")
# MAGIC         
# MAGIC         .withColumn("discount_amount", 
# MAGIC                     DecimalType(9,2),
# MAGIC                     omit=True, 
# MAGIC                     expr="cast((price * discount) as numeric(9,2))",
# MAGIC                     baseColumn=["order_id",'price', 'discount'])
# MAGIC         
# MAGIC         .withColumn("final_price", 
# MAGIC                     DecimalType(9,2),
# MAGIC                     omit=True, 
# MAGIC                     expr="cast((price - coalesce(discount_amount,0)) as numeric(9,2))",
# MAGIC                     baseColumn=["order_id",'discount_amount', 'price'])
# MAGIC         
# MAGIC         .withColumn("payment_method", 
# MAGIC                     StringType(),
# MAGIC                     omit=True, 
# MAGIC                     values=payment_methods, 
# MAGIC                     weights=payment_method_distribution,
# MAGIC                     baseColumn="order_id")
# MAGIC         
# MAGIC         .withColumn("base_installments", 
# MAGIC                     LongType(),
# MAGIC                     omit=True, 
# MAGIC                     values=installments, 
# MAGIC                     weights=installment_distribution,
# MAGIC                     baseColumn="order_id")
# MAGIC         
# MAGIC         .withColumn("installments", 
# MAGIC                     StringType(),
# MAGIC                     omit=True, 
# MAGIC                     expr="""case when payment_method != 'credit_card'
# MAGIC                                 then 1 
# MAGIC                             else base_installments end""",
# MAGIC                     baseColumn=["order_id",'base_installments','payment_method'])
# MAGIC         
# MAGIC         .withColumn("installment_value", 
# MAGIC                     DecimalType(9,2),
# MAGIC                     omit=True, 
# MAGIC                     expr="cast((final_price/installments) as numeric(9,2))",
# MAGIC                     baseColumn=["order_id",'installments','final_price'])
# MAGIC         
# MAGIC         .withColumn("user_info",
# MAGIC                     StructType([
# MAGIC                         StructField('name',StringType()),
# MAGIC                         StructField('age',StringType()),
# MAGIC                         StructField('gender',StringType()),
# MAGIC                         StructField('email',StringType()),
# MAGIC                         StructField('profession',StringType()),
# MAGIC                         StructField('country',StringType())
# MAGIC                     ]),
# MAGIC                     omit=True, 
# MAGIC                     expr="""
# MAGIC                         named_struct(
# MAGIC                             'name', name, 
# MAGIC                             'age', idade,
# MAGIC                             'gender', gender,
# MAGIC                             'email', email,
# MAGIC                             'profession', profession,
# MAGIC                             'country', country
# MAGIC                         )""",
# MAGIC                     nullable=True, 
# MAGIC                     percentNulls=0.7,
# MAGIC                     baseColumn=['name','age','gender','email','profession', 'country'])
# MAGIC         
# MAGIC         .withColumn("product_info",
# MAGIC                     StructType([
# MAGIC                         StructField('product_name',StringType()),
# MAGIC                         StructField('price',StringType())
# MAGIC                     ]),
# MAGIC                     omit=True, 
# MAGIC                     expr="""
# MAGIC                         case when info_usuario is not null then 
# MAGIC                         named_struct(
# MAGIC                             'product_name', product_name, 
# MAGIC                             'price', price_string
# MAGIC                         )
# MAGIC                         else null end""",
# MAGIC                     nullable=True, 
# MAGIC                     percentNulls=0.7,
# MAGIC                     baseColumn=['user_info','product_name','price_string'])
# MAGIC         
# MAGIC         .withColumn("payment_info",
# MAGIC                     StructType([
# MAGIC                         StructField('discount',DecimalType(9,2)),
# MAGIC                         StructField('final_price',DecimalType(9,2)),
# MAGIC                         StructField('payment_method',StringType()),
# MAGIC                         StructField('installments',LongType()),
# MAGIC                         StructField('installment_value',DecimalType(9,2))
# MAGIC                     ]),
# MAGIC                     omit=True, 
# MAGIC                     expr="""
# MAGIC                         case when product_info is not null then 
# MAGIC                         named_struct(
# MAGIC                             'discount', discount, 
# MAGIC                             'final_price', final_price,
# MAGIC                             'payment_method', payment_method,
# MAGIC                             'installments', installments,
# MAGIC                             'installment_value', installment_value
# MAGIC                         ) else null end""",
# MAGIC                     baseColumn=['product_info','discount','final_price','payment_method','installments','installment_value'])
# MAGIC         
# MAGIC         .withColumn("payload",
# MAGIC                     StructType([
# MAGIC                         StructField('user_info',StructType([
# MAGIC                                                         StructField('name',StringType()),
# MAGIC                                                         StructField('age',StringType()),
# MAGIC                                                         StructField('gender',StringType()),
# MAGIC                                                         StructField('email',StringType()),
# MAGIC                                                         StructField('profession',StringType()),
# MAGIC                                                         StructField('country',StringType())
# MAGIC                                                     ])),
# MAGIC                         StructField('product_info',StructType([
# MAGIC                                                         StructField('product_name',StringType()),
# MAGIC                                                         StructField('price',StringType())
# MAGIC                                                     ])),
# MAGIC                         StructField('payment_info',StructType([
# MAGIC                                                         StructField('discount',DecimalType(9,2)),
# MAGIC                                                         StructField('final_price',DecimalType(9,2)),
# MAGIC                                                         StructField('payment_method',StringType()),
# MAGIC                                                         StructField('installments',LongType()),
# MAGIC                                                         StructField('installment_value',DecimalType(9,2))
# MAGIC                                                     ]))
# MAGIC                     ]),
# MAGIC                     expr="""
# MAGIC                         named_struct(
# MAGIC                             'user_info', user_info,
# MAGIC                             'product_info', product_info,
# MAGIC                             'payment_info', payment_info
# MAGIC                         )""",
# MAGIC                     baseColumn=['user_info','product_info','payment_info'])
# MAGIC     )
# MAGIC
# MAGIC     df = df_test.build(withStreaming=True, options={'rowsPerSecond': number_rows}).drop('id')
# MAGIC     
# MAGIC     if run_as_batch == True:
# MAGIC         generate_api_stream_data = (
# MAGIC             df.writeStream
# MAGIC                 .format('json')
# MAGIC                 .outputMode('append')
# MAGIC                 .queryName('generate_api_stream_data')
# MAGIC                 ### Adicionado para funcionar no databricks Free
# MAGIC                 ### Se você estiver estudando em um ambiente fora do Free 
# MAGIC                 ### a linha de trigger pode ser removida e o processo vai rodar
# MAGIC                 ### continuamente.
# MAGIC                 .trigger(availableNow=True)
# MAGIC                 .option('checkpointLocation',f'/Volumes/workspace/default/{chapter_number}/_checkpoint/api_stream_data')
# MAGIC                 .start(f"/Volumes/workspace/default/{chapter_number}/api_stream_data")
# MAGIC         )
# MAGIC     else:
# MAGIC         generate_api_stream_data = (
# MAGIC             df.writeStream
# MAGIC                 .format('json')
# MAGIC                 .outputMode('append')
# MAGIC                 .queryName('generate_api_stream_data')
# MAGIC                 .option('checkpointLocation', f'/Volumes/workspace/default/{chapter_number}/_checkpoint/api_stream_data')
# MAGIC                 .start(f"/Volumes/workspace/default/{chapter_number}/api_stream_data")
# MAGIC         )   
# MAGIC     
# MAGIC     return generate_api_stream_data
