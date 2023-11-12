# Databricks notebook source
import json
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType,TimestampType
from pyspark.sql.functions import col, sha2

# COMMAND ----------

import random
import datetime;
def user_id():
    user = random.choice([1,4,6,7,15,20,26,27,28,29,30,33,34,35,36]) 
    return user
def calories_burnt():
    calories = random.uniform(50.0, 600.0)
    return calories
def num_steps():
    steps = random.randrange(35, 15000, 1)
    return steps
def miles_walked():
    walked = random.uniform(0.10, 10.0)
    return walked
def time_stamp():
    return datetime.datetime.now()
def device_id():
    device = random.randrange(0, 1001, 1)
    return device
def get_data():
    return [(user_id(),calories_burnt(),num_steps(),miles_walked(),time_stamp(),device_id())]
def get_schema():
    return StructType([ \
    StructField("user_id",IntegerType(),True), \
    StructField("calories_burnt",FloatType(),True), \
    StructField("num_steps",IntegerType(),True), \
    StructField("miles_walked", FloatType(), True), \
    StructField("time_stamp", TimestampType(), True), \
    StructField("device_id", IntegerType(), True) \
  ])
def get_df_device(data2,schema): 
    return spark.createDataFrame(data=data2,schema=schema)
def write_df(df,filePath):
    df.coalesce(1).write.option("compression", "gzip",).json(filePath,mode='append')

# COMMAND ----------

concatenated_col = concat_ws("_", col("user_id"), col("calories_burnt"), col("num_steps"), col("miles_walked"), col("time_stamp"), col("device_id"))
for x in range(10000):
    data2 = get_data()
    schema = get_schema()
    df_device = get_df_device(data2,schema)
    df_device = df_device.withColumn("id",sha2(concatenated_col, 256))
    output_path = '/user/hive/raw/stream/device'
    write_df(df_device,output_path)

# COMMAND ----------

# dbutils.fs.rm("dbfs:/user/hive/raw/stream/device",recurse=True)
