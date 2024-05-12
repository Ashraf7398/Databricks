-- Databricks notebook source
-- create database mart;
use mart

-- COMMAND ----------

select * from endtoendproject.cancellation limit 1 

-- COMMAND ----------

-- select count(tail_id),count(distinct(tail_id)) from endtoendproject.plane
desc endtoendproject.cancellation

-- COMMAND ----------

create or replace table dim_cancellation (
  code string,
  description string
  ) ;

-- COMMAND ----------

insert overwrite dim_cancellation
select 
code,
description 
from endtoendproject.cancellation

-- COMMAND ----------

select * from dim_cancellation limit 10

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("ReadTableExample") \
-- MAGIC     .getOrCreate()
-- MAGIC df = spark.sql("SELECT * FROM dim_cancellation")
-- MAGIC df.write.format("delta").mode("Overwrite").save("/mnt/raw_datalake_mart/dim_cancellation")

-- COMMAND ----------


