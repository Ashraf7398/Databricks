-- Databricks notebook source
-- create database mart;
use mart

-- COMMAND ----------

select * from endtoendproject.plane  limit 1

-- COMMAND ----------

-- select count(tail_id),count(distinct(tail_id)) from endtoendproject.plane
desc endtoendproject.plane

-- COMMAND ----------

create or replace table dim_plane (
  tail_id string,
  type string,
  manufacturer string,
  issue_date date,
  model string,
  status string,
  aircraft_type string,
  engine_type string,
  year int
  ) ;

-- COMMAND ----------

insert overwrite dim_plane
select 
tail_id ,
type ,
manufacturer ,
issue_date ,
model ,
status ,
aircraft_type ,
engine_type ,
year 
from endtoendproject.plane

-- COMMAND ----------

select * from dim_plane limit 1

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("ReadTableExample") \
-- MAGIC     .getOrCreate()
-- MAGIC df = spark.sql("SELECT * FROM dim_plane")
-- MAGIC df.write.format("delta").mode("Overwrite").save("/mnt/raw_datalake_mart/dim_plane")

-- COMMAND ----------


