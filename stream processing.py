# Databricks notebook source
path = '/databricks-datasets/iot-stream/data-user/'
file_csv = "userData.csv"
path_file = "{}{}".format(path,file_csv)
df_user = spark.read.csv(path_file,header="true")
df_user = df_user.filter("bp = 'High'")
# display(df_user)

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
    .option("maxFilesPerTrigger", 1000)  # Emularemos o aspecto de Streaming realizando a leitura de um dataset por vez.
    .json(inputPath)
)

# COMMAND ----------

streamingInputDF = streamingInputDF.join(df_user,streamingInputDF.user_id == df_user.userid)

# COMMAND ----------

display(streamingInputDF)

# COMMAND ----------

streamingAggrs = (
  streamingInputDF
    .withWatermark("timestamp", "10 minutes")
    .groupBy("user_id", window("timestamp","10 minutes"))\
        .agg(sum("num_steps").alias("sum_num_steps"),\
             sum("miles_walked").alias("sum_miles_walked"),\
             sum("calories_burnt").alias("sum_calories_burnt"),\
             avg("risk").alias("avg_risk"),\
             count("id").alias("count_events"))
    .filter("sum_num_steps < 19000")
)

# COMMAND ----------

display(streamingAggrs)

# COMMAND ----------

df2 = df_stream \
              .withColumn('timestamp', unix_timestamp(col('EventDate'), "MM/dd/yyyy hh:mm:ss aa").cast(TimestampType())) \
              .withWatermark("timestamp", "1 minutes") \
              .groupBy(col("SendID"), "timestamp") \
              .agg(max(col('timestamp')).alias("timestamp")) \
              .orderBy('timestamp', ascending=False)

# COMMAND ----------

withWatermark("timestamp", "10 minutes")

# COMMAND ----------

display(streamingAggrs)

# COMMAND ----------

streamingDF.writeStream.outputMode("append")
  .option("checkpointLocation", orders_checkpoint_path)
  .partitionBy("submitted_yyyy_mm")
  .start("/pathtotable/sachin")

# COMMAND ----------

query = (
  streamingAggrs
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")        
    .toTable("Tb_data_device_user_10Minutes")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Tb_data_device_user_10Minutes

# COMMAND ----------

dbutils.fs.rm("/tmp/delta/events/_checkpoints/",recurse=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/",recurse=True)

# COMMAND ----------


display(dbutils.fs.ls('dbfs:/user/hive/warehouse/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table Tb_data_device_user_10Minutes

# COMMAND ----------

sensorStreamDF = sensorStreamDF \
.withWatermark("eventTimestamp", "10 minutes") \
.groupBy(window(sensorStreamDF.eventTimestamp, "10 minutes")) \
.avg(sensorStreamDF.temperature,
     sensorStreamDF.pressure)

sensorStreamDF.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/delta/events/_checkpoints/temp_pressure_job/")
  .start("/delta/temperatureAndPressureAverages")

# COMMAND ----------

events.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")
   .toTable("Tb_data_device_user_10Minutes")

# COMMAND ----------

display(streamingInputDF)
