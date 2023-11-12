# Databricks notebook source
# MAGIC %md
# MAGIC Leitura dos objetos stream 

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
jsonSchema = StructType([ StructField("calories_burnt", DoubleType(), True), StructField("device_id", LongType(), True), StructField("id", StringType(), True), StructField("miles_walked", DoubleType(), True), StructField("num_steps", LongType(), True), StructField("time_stamp", TimestampType(), True),StructField("user_id", LongType(), True) ])
inputPath = '/user/hive/raw/stream/device'

streamingInputDF = (
  spark
    .readStream
    .option("failOnDataLoss","false")
    .schema(jsonSchema)               
    .option("maxFilesPerTrigger", 1)  # Emularemos o aspecto de Streaming realizando a leitura de um dataset por vez.
    .json(inputPath)
)

# COMMAND ----------

# MAGIC %md leitura da tabela de usuario
# MAGIC

# COMMAND ----------

path = '/databricks-datasets/iot-stream/data-user/'
file_csv = "userData.csv"
path_file = "{}{}".format(path,file_csv)
df_user = spark.read.csv(path_file,header="true")
df_user = df_user.filter("bp = 'High'")
# display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC Cruzamento entre tabelas uma stream e outra batch

# COMMAND ----------

streamingInputDF = streamingInputDF.join(df_user,streamingInputDF.user_id == df_user.userid)

# COMMAND ----------

# MAGIC %md
# MAGIC Aplicação de regra de negocio e janela
# MAGIC obs como é uma agregação complexa foi necessário usar o withWatermark

# COMMAND ----------

streamingAggrs = (
  streamingInputDF
    .withWatermark("time_stamp", "1 minutes")
    .groupBy("user_id", window("time_stamp","1 minutes"))\
        .agg(sum("num_steps").alias("sum_num_steps"),\
             sum("miles_walked").alias("sum_miles_walked"),\
             sum("calories_burnt").alias("sum_calories_burnt"),\
             avg("risk").alias("avg_risk"),\
             count("id").alias("count_events"))
    .filter("sum_num_steps < 19000")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Escrita em tabela delta, OBs em um processo de append precisamos ter o diretorio da tabela limpo antes de começar a carga, podemos usar o modo merge caso seja necessário manter historico, porem para um processo stream o ideal e apos um periodo mover os dados para uma tabela hist e limpar a tabela principal

# COMMAND ----------

dbutils.fs.rm("/tmp/delta/events/_checkpoints/",recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/",recurse=True)

streamingAggrs.writeStream\
  .format("delta")\
  .outputMode("append")\
  .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")\
  .trigger(processingTime="30 seconds") \
  .start("dbfs:/user/hive/warehouse/tb_data_device_user_10minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC Analise da tabela salva

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Tb_data_device_user_10Minutes

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/tb_data_device_user_10minutes"))
