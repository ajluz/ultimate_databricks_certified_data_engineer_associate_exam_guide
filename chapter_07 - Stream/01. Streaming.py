# Databricks notebook source
# MAGIC %run "./setup/setup_chapter_07"

# COMMAND ----------

generate_and_write_to_volume("07")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 5)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/chapter_07/api_stream_data/"))

# COMMAND ----------

stream_path = "/Volumes/workspace/default/chapter_07/api_stream_data/"

static = spark.read.json(stream_path)
schema = static.schema

print(schema)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, year
from pyspark.sql.types import TimestampType

# a opção maxFilePerTrigger vai definir quantos arquivos podem ser lidos por vez dentro do processo de stream. 
# a leitura do Stream assim como no batch é Lazy Evaluated. Ele não vai iniciar até que uma action seja chamada. 
streaming = (
    spark.readStream
         .schema(schema)
         .option('maxFilesPerTrigger', 1)
         .json(stream_path)
)

# COMMAND ----------

# DBTITLE 1,Cell 7
display(
  spark.readStream
       .schema(schema)
       .format('json')
       .load(stream_path),
  checkpointLocation='/Volumes/workspace/default/chapter_07/checkpoint/display_7',
  streamingQuery={'outputMode': 'append'}
)

# COMMAND ----------

streaming = (
    spark.readStream
         .schema(schema)
         .option('maxFilesPerTrigger', 1)
         .json(stream_path)
)

# COMMAND ----------

access_counts = streaming.groupBy("access_point").count()

## é possivel quebrar linhas no código assim:
# activityQuery = activityCounts.writeStream.queryName("activity_counts")\
# .format("memory").outputMode("complete")\
# .start()

## Ou assim:
# O nome activity_counts foi dado pra essa stream query. 
# ele deve ser único entre as stream queries executadas na mesma sessão.
activityQuery = (
    access_counts.writeStream
                  .queryName("access_counts")
                  .format("memory")
                  .option("checkpointLocation","/Volumes/workspace/default/chapter_07/checkpoint/test_5")
                  .trigger(availableNow=True)
                  .outputMode("complete")
                  .start()
)

# COMMAND ----------

# Use esse comando para verificar as queries em stream na sessão do Spark.
for stream in spark.streams.active:
    print(stream.lastProgress)

# COMMAND ----------

spark.sql("""
  SELECT DISTINCT access_point
  FROM access_counts
  --WHERE access_point = 'iphone'
""").display()

# COMMAND ----------

from pyspark.sql.functions import expr

# Transformação simples igual em dataframes em batch
simpleTransform = (
    streaming.withColumn("is_mobile", expr("access_point IN ('iphone', 'android')"))
             .where("is_mobile")
             .where("payload.payment_info IS NOT NULL")
             .writeStream.queryName("simple_transform")
             .format("memory")
             .outputMode("append")
             .option("checkpointLocation","/Volumes/workspace/default/chapter_07/checkpoint/simpleTransform_4")
             .trigger(availableNow=True)
             .start()
)

# COMMAND ----------

spark.sql("""
  SELECT * FROM simple_transform
""").display()

# COMMAND ----------



# COMMAND ----------

# Aggregação simples
simple_aggregation = (
    streaming.rollup("","model").avg()
             .drop("avg(Arrival_time)")
             .drop("avg(Creation_Time)")
             .drop("avg(Index)")
             .writeStream.queryName("device_counts")
             .format("memory")
             .outputMode("complete")
             .start()
    )

# COMMAND ----------

# Criação de um batch dataframe 
historicalAgg = (
    static.groupBy("gt","model").avg()
    )

# Stream-Static Join        
deviceModelStats_stream_to_static_join = (
    streaming.drop("Arrival_Time","Creation_Time","Index")
             .cube("gt","model").avg()
             .join(
                 historicalAgg, 
                 ["gt","model"]
             )
             .writeStream
             .queryName("device_counts__stream_static_join")
             .format("memory")
             .outputMode("complete")
             .start()
)

# COMMAND ----------

# # Exemplo de como realizar a leitura de stream a partir do Kafka
# df1 = (
#     spark.readStream.format("kafka")
#          .option("kafka.bootstrap.servers","host1:port1,host2:port2")
#          .option("subscribe","topic1")
#          .load()
# )

# # Exemplo de escrita no Kafka
# (
#     df1.writeStream
#        .format("kafka")
#        .option("checkpointLocation","/to/HDFS-compatible/dir")
#        .option("kafka.bootstrap.servers","host1:port1,host2:port2")
#        .start()
# )

# COMMAND ----------

# Escrevendo dados em stream para um diretório de modo continuo
# .format() define o formato de saida do arquivo
# a option checkpointLocation define onde o stream vai guardar o checkpoint para controlar o processo de sink
# o start() vai receber como parametro o diretório onde os dados serão processados
(
    streaming.writeStream
             .queryName('write_stream_1')
             .outputMode('append')
             .format('delta')
             .trigger(processingTime='10 seconds')
             .option('checkpointLocation', 'FileStore/clube_do_livro/episodio17/_checkpoint/table_1')
             .start("FileStore/clube_do_livro/episodio17/table_1")
)

# COMMAND ----------

# Escrevendo dados em stream para em uma tabela de modo continuo
# o método toTable() vai receber como parametro o nome da tabela que será criada e processada
(
    streaming.writeStream
             .queryName('write_stream_2')
             .outputMode('append')
             .format('delta')
             .trigger(processingTime='10 seconds')
             .option('checkpointLocation', 'FileStore/clube_do_livro/episodio17/_checkpoint/table_2')
             .toTable('Tabela_2')
)

# COMMAND ----------

# Escrevendo dados em stream para em uma tabela usando o availableNow=True | Batch
# a trigger availableNow=True vai permitir executar o processo de stream em batch
(
    streaming.limit(100)
             .writeStream
             .queryName('write_stream_3')
             .outputMode('append')
             .format('delta')
             .trigger(availableNow=True)
             .option('checkpointLocation', 'FileStore/clube_do_livro/episodio17/_checkpoint/table_3')
             .toTable('Tabela_3')
).awaitTermination()

# COMMAND ----------

stream_path = "/databricks-datasets/definitive-guide/data/activity-data/"

# Lendo dados de um diretório usando o Auto Loader para inferencia e evolução de schema
# para usar o Auto Loader o metodo format() precisa receber o valor CloudFiles como parametro
# a option cloudFiles.format é obrigatória e define o formato do arquivo
# a option cloudFiles.schemaLocation define onde o spark vai armazenar o schema que foi inferido
# a option cloudFiles.schemaEvolutionMode define como o Auto Loader vai lidar com evoluções de schema
# a option cloudFiles.inferColumnTypes vai inferir os tipos de dados das colunas. Default False

autoLoaderDf = (
    spark.readStream
         .format('CloudFiles')
         .option('cloudFiles.maxFilesPerTrigger', 1)
         .option('cloudFiles.format', 'json')
         .option('cloudFiles.schemaLocation', '/FileStore/clube_do_livro/episodio17/_schemas/auto_loader')
         .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
         .option('cloudFiles.inferColumnTypes', True)
         .load(stream_path)
)

display(autoLoaderDf)

# COMMAND ----------

display(spark.read.text('/FileStore/clube_do_livro/episodio17/_schemas/auto_loader/_schemas/0', wholetext=True))
