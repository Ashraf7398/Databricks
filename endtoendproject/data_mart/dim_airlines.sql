-- Databricks notebook source
-- create database mart;
use mart

-- COMMAND ----------

select * from endtoendproject.airlines limit 1 

-- COMMAND ----------

-- select count(tail_id),count(distinct(tail_id)) from endtoendproject.plane
desc endtoendproject.airlines

-- COMMAND ----------

create or replace table dim_airlines (
  iata_code string,
  icao_code string,
  name string
  ) ;

-- COMMAND ----------

insert overwrite dim_airlines
select 
iata_code,
icao_code ,
name   
from endtoendproject.airlines

-- COMMAND ----------

select * from dim_airlines limit 1

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("ReadTableExample") \
-- MAGIC     .getOrCreate()
-- MAGIC df = spark.sql("SELECT * FROM dim_airlines")
-- MAGIC df.write.format("delta").mode("Overwrite").save("/mnt/raw_datalake_mart/dim_airlines")

-- COMMAND ----------


