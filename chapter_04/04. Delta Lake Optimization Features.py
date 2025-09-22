# Databricks notebook source
# MAGIC %md
# MAGIC # Low Cardinality Filters

# COMMAND ----------

# spark.sql("drop table if exists tb_people_2")
# spark.sql("drop table if exists tb_people_2_partitioned")

# COMMAND ----------

df = spark.read.load('/databricks-datasets/learning-spark-v2/people/people-10m.delta')
df.write.option('maxRecordsPerFile', 1000).saveAsTable('tb_people_2')

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY tb_people_2
""").select(
    'operationMetrics'
    ).show(truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT COUNT(*) amount FROM tb_people_2 WHERE gender = 'F'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Partitioning

# COMMAND ----------

(
    df.write
      .option('maxRecordsPerFile', 1000)
      .partitionBy("gender")
      .saveAsTable('tb_people_2_partitioned')
)

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY tb_people_2_partitioned
""").select(
    'operationMetrics'
    ).show(truncate=False)

# COMMAND ----------

spark.sql("""
    DESCRIBE DETAIL tb_people_2_partitioned
""").select(
    'partitionColumns'
    ).show(truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT COUNT(*) amount 
    FROM tb_people_2_partitioned 
    WHERE gender = 'F'
""").show()

# COMMAND ----------

# Note: Changing target file size to 2 MB only for demonstrations; in production, larger targets (e.g., 128–512 MB) usually deliver better scan performance and fewer files.
# spark.sql("ALTER TABLE tb_people_2 SET TBLPROPERTIES ('delta.targetFileSize' = 2097152)")

# spark.sql("OPTIMIZE tb_people_2")

# len(spark.table("tb_people_2").inputFiles())

spark.sql("""
    DESCRIBE HISTORY tb_people_2
""").select(
    'operationMetrics.numRemovedFiles',
    'operationMetrics.numAddedFiles'
    ).where(
        "version = 2"
    ).show(truncate=False)

# COMMAND ----------

spark.sql("""
    SELECT COUNT(*) amount FROM tb_people_2 WHERE gender = 'F'
""").show()

# COMMAND ----------

spark.sql("""
    SELECT id, firstName, lastName, gender, ssn
    FROM tb_people_2 
    WHERE id = 999999
""").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Performance Issue #1 - Filtros em colunas com Baixa Cardinalidade
# MAGIC Usando o comando abaixo será possivel realizar uma query filtrando os dados da tabela [lab___delta_with_small_files] pela coluna [gender]. A query demorará aproximadamente 6 segundos para ser executada.

# COMMAND ----------

spark.sql("""
    SELECT count(*) 
    FROM lab___delta_with_small_files 
    WHERE gender = 'F'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Usando o `dbutils.fs.ls` apontando para o diretório onde a tabela [lab___delta_with_small_files] armazena os seus dados é possivel verificar que os dados dessa tabela estão distribuídos em 100 arquivos diferentes.  

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/lab___delta_with_small_files'))

# COMMAND ----------

# MAGIC %md 
# MAGIC Usando o comando abaixo será possivel realizar uma query filtrando os dados da tabela [lab___delta_with_small_files_partitioned] pela coluna [gender]. 
# MAGIC
# MAGIC A query demorará aproximadamente 1 segundo para ser executada. 
# MAGIC
# MAGIC Isso acontece porque como a tabela foi particionada pela coluna [gender] o Spark irá realizar o que chamamos de `Data Skipping` para ler apenas os arquivos necessários para realização da query. 

# COMMAND ----------

spark.sql("""
    SELECT count(*) 
    FROM lab___delta_with_small_files_partitioned
    WHERE gender = 'F'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Usando o `dbutils.fs.ls` apontando para o diretório onde a tabela [lab___delta_with_small_files_partitioned] armazena os seus dados é possivel verificar que os dados dessa tabela estão distribuídos em dois sub-diretórios diferentes.  

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/lab___delta_with_small_files_partitioned'))
display(dbutils.fs.ls('dbfs:/user/hive/warehouse/lab___delta_with_small_files_partitioned/gender=M/'))

# COMMAND ----------

# MAGIC %md 
# MAGIC Usando o comando abaixo será possivel realizar uma nova query na tabela [lab___delta_with_small_files] filtrando os dados através da coluna [salary]. A query demorará aproximadamente 4 segundos para ser executada. 

# COMMAND ----------

spark.sql("""
    SELECT count(*) 
    FROM lab___delta_with_small_files 
    WHERE salary BETWEEN 110000 AND 145000 
""").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Agora usando o comando abaixo será possivel realizar a mesma nova query na tabela [lab___delta_with_small_files_partitioned] filtrando os dados através da coluna [salary]. A query demorará aproximadamente 5 segundos para ser executada. 
# MAGIC
# MAGIC Uma vez que o filtro não foi realizado na coluna de partição, o Spark precisa varrer todos os arquivos para descobrir em qual deles está os dados pesquisados. 

# COMMAND ----------

spark.sql("""
    SELECT count(*) 
    FROM lab___delta_with_small_files_partitioned
    WHERE salary BETWEEN 110000 AND 145000 
""").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Quando devo Particionar uma tabela Delta?
# MAGIC 1. A Databricks recomenda que tabelas com menos de 1TB de dados não sejam particionadas. Veja a <a href="https://learn.microsoft.com/en-us/azure/databricks/tables/partitions" target="_blank">documentação oficial</a>. 
# MAGIC
# MAGIC 2. Um outro ponto importante é que dependendo do Databricks runtime e features utilizadas será necessário particionar tabelas que sofram atualizações concorrentes. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Performance Issue #2 - Muitos arquivos pequenos - Small files issue
# MAGIC Conforme vimos, os dados da tabela [lab___delta_with_small_files] estão distribuídos em 101 arquivos pequenos.

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/lab___delta_with_small_files'))

# COMMAND ----------

# MAGIC %md 
# MAGIC O segundo método de performance que pode ser utlizado em tabelas delta é o **`OPTIMIZE`**. Com ele é possivel concatenar os arquivos pequenos dentro de uma tabela delta em arquivos maiores, dando performance dessa forma a consultas realizadas nessa tabela usando o Spark.
# MAGIC
# MAGIC Após execução do comando é possivel perceber na coluna de [metrics] que 99 arquivos foram 'excluídos' da estrutura de dados da tabela e 33 novos arquivos foram adicionados.  

# COMMAND ----------

spark.sql("""
    OPTIMIZE lab___delta_with_small_files
""").display()

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/lab___delta_with_small_files/_delta_log'))

# COMMAND ----------

# MAGIC %sql describe history lab___delta_with_small_files

# COMMAND ----------

# MAGIC %md 
# MAGIC Agora realize a mesma consulta filtrando pela coluna [gender] na tabela [lab___delta_with_small_files] e observe que a query agora demora aproiximadamente 2 segundos para ser executada. 

# COMMAND ----------

spark.sql("""
    SELECT count(*) 
    FROM lab___delta_with_small_files 
    WHERE gender = 'F'
""").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Realize a nova consulta filtrando os dados pela coluna [salary] na tabela [lab___delta_with_small_files] e observe que a query também possui agora um ganho de performance executando em aproximadamente 1 segundo. 

# COMMAND ----------

spark.sql("""
    SELECT count(*) 
    FROM lab___delta_with_small_files 
    WHERE salary BETWEEN 110000 AND 145000 
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Issue #3 - Filtros em colunas com Alta Cardinalidade
# MAGIC Agora, após realizado o OPTIMIZE (método recomemdado para performance tunning de tabelas delta) utilize a query abaixo para perquisar a pessoa que possui o [id] = 999999. 
# MAGIC
# MAGIC Esse filtro está sendo realizado em uma coluna com `alta cardinalidade`. Como não se pode particionar uma tabela por colunas de alta cardinalidade, a engine do Spark não possui uma forma de identificar em qual arquivo esse valor de id está localizado resultando assim em uma varredura de todos os arquivos de dados da tabela.

# COMMAND ----------

spark.sql("""
    SELECT *
    FROM lab___delta_with_small_files 
    WHERE id = 999999
""").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC O terceiro método de performance que pode ser utlizado em tabelas delta é o **`ZORDER BY`**. Com ele é possivel realizar uma expecie de indexação de dados dentro dos arquivos parquet usando as colunas especificadas no comando de performance. 
# MAGIC
# MAGIC Execute o comando `OPTIMIZE` abaixo seguindo do `ZORDER BY (id)`. Os arquivos da tabela serão reescritos poderem dessa ver em order ascendente com relação aos valores da coluna [id].
# MAGIC
# MAGIC A coluna [metrics] mostra que 24 novos arquivos foram escritos dentro do diretório de dados da tabela [lab___delta_with_small_files]. 

# COMMAND ----------

spark.sql("""
    OPTIMIZE lab___delta_with_small_files ZORDER BY (id)
""").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Através do comando `DESCRIBE HISTORY` é possivel verificar que as informações sobre o último comando OPTIMIZE executado foram armazenados no arquivo de log de transação com a versão 2. 

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY lab___delta_with_small_files
""").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Ao analisar o arquivo de log de transação 2 será possivel perceber que os 24 arquivos foram adicionados pelo comando `OPTIMIZE`
# MAGIC
# MAGIC No meta-dado de cada arquivo o valor de max e min da coluna [id] foi armazenado de forma ordenada. Dessa forma, esse meta-dado agora pode ser usado pela engine do Spark para realizar `Data Skipping`. 

# COMMAND ----------

display(spark.read.format('json').load("dbfs:/user/hive/warehouse/lab___delta_with_small_files/_delta_log/00000000000000000002.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC Agora, após realizado o `ZORDER BY` (método recomemdado para performance tunning de tabelas delta para colunas com alta cardinalidade) utilize a query abaixo para perquisar a pessoa que possui o [id] = 999999. 
# MAGIC
# MAGIC A query engine vai retornar os dados em aproximadamente 1 segundo.
# MAGIC
# MAGIC Perceba no plano de execução que apenas um arquivo foi lido e que o parametro `number of files pruned` mostra o valor o que significa que o Spark ignorou a leitura de 23 arquivos. 

# COMMAND ----------

spark.sql("""
    SELECT *
    FROM lab___delta_with_small_files 
    WHERE id = 999999
""").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Otimização de Custo de Armazenamento de Storage
# MAGIC Na coluna de [metrics] do último comando optimize executado mostra que 24 arquivos foram escritos contendo os dados da tabela [lab___delta_with_small_files]. 
# MAGIC
# MAGIC Ao Executar o comando de list no diretório da tabela será possivel observar 158 arquivos. Isso significa que os arquivos contendo a versão anterior dos dados não foram deletados do diretório. Eles foram somente excluídos do log de transação da tabela delta. 
# MAGIC
# MAGIC Os Arquivos com as versões anteriores dos dados precisam ser excluídos do diretório por questão de custo de armazenamento. 

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/lab___delta_with_small_files"))

# COMMAND ----------

# MAGIC %md 
# MAGIC O Delta Lake possui um comando que pode ser utilizado para realizar essa deleção dos dados antigos: O **`VACUUM`**. 
# MAGIC
# MAGIC Para conseguir garantir o Time Travel e evitar deleções de dados que podem estar sendo usados por transações simultâneas, a configuração padrão do Spark delimita que somente arquivos mais velhos que 7 dias do momento da execução do comando de `VACUUM` podem ser deletados. 
# MAGIC
# MAGIC Para fins de demonstração iremos desabilitar essa regra - **Não Recomendado para ambientes de Produção**. Veja <a href="https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/delta-vacuum" target="_blank">documentação oficial</a>.

# COMMAND ----------

spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled','false')
spark.conf.get('spark.databricks.delta.retentionDurationCheck.enabled')

# COMMAND ----------

# MAGIC %md 
# MAGIC Execute o comando de **`VACUUM`** abaixo. o parametro `RETAIN n HOURS` determina quais arquivos serão mantidos com base na diferença entre o momento da escrita do arquivo e a execução do comando de VACUUM - Padrão de 168 horas ou 7 dias. 

# COMMAND ----------

# MAGIC %sql VACUUM lab___delta_with_small_files RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql VACUUM lab___delta_with_small_files RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md 
# MAGIC Ao Executar o comando de list no diretório da tabela será possivel observar dessa vez somente 24 arquivos de dados. Os arquivos com as versões anteriores dos dados foram deletados com sucesso do diretório da tabela.

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/lab___delta_with_small_files'))
