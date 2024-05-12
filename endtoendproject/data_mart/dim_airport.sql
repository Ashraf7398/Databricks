-- Databricks notebook source
-- create database mart;
use mart

-- COMMAND ----------

select * from endtoendproject.airport limit 1 

-- COMMAND ----------

-- select count(tail_id),count(distinct(tail_id)) from endtoendproject.plane
desc endtoendproject.airport

-- COMMAND ----------

create or replace table dim_airport (
  code string,
  city string,
  country string,
  airport string
  ) ;

-- COMMAND ----------

insert overwrite dim_airport
select 
code ,
city ,
country ,
airport  
from endtoendproject.airport

-- COMMAND ----------

select * from dim_airport limit 1

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("ReadTableExample") \
-- MAGIC     .getOrCreate()
-- MAGIC df = spark.sql("SELECT * FROM dim_airport")
-- MAGIC df.write.format("delta").mode("Overwrite").save("/mnt/raw_datalake_mart/dim_airport")

-- COMMAND ----------


