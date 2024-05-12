-- Databricks notebook source
-- create database mart;
use mart

-- COMMAND ----------

select * from endtoendproject.unique_carriers limit 1 

-- COMMAND ----------

-- select count(tail_id),count(distinct(tail_id)) from endtoendproject.plane
desc endtoendproject.unique_carriers

-- COMMAND ----------

create or replace table dim_unique_carriers (
  code string,
  description string
  ) ;

-- COMMAND ----------

insert overwrite dim_unique_carriers
select 
code,
description 
from endtoendproject.unique_carriers

-- COMMAND ----------

select * from dim_unique_carriers limit 10

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("ReadTableExample") \
-- MAGIC     .getOrCreate()
-- MAGIC df = spark.sql("SELECT * FROM dim_unique_carriers")
-- MAGIC df.write.format("delta").mode("Overwrite").save("/mnt/raw_datalake_mart/dim_unique_carriers")

-- COMMAND ----------


