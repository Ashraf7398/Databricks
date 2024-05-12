-- Databricks notebook source
-- create database mart;
use mart

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("ReadTableExample") \
-- MAGIC     .getOrCreate()

-- COMMAND ----------

desc endtoendproject.flight

-- COMMAND ----------

-- select * from endtoendproject.flight limit 1 
select date,ArrDelay,DepDelay,Origin,Cancelled,CancellationCode,UniqueCarrier,FlightNum,TailNum,DepTime from endtoendproject.flight where flightnum=2891

-- COMMAND ----------

-- drop table reporting_flight
create
or replace table reporting_flight (
  date date,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Cancelled int,
  CancellationCode string,
  UniqueCarrier string,
  FlightNum int ,
  TailNum string,
  DepTime string
);

-- COMMAND ----------

insert
  into reporting_flight
select
  date,
  ArrDelay,
  DepDelay,
  Origin,
  Cancelled,
  CancellationCode,
  UniqueCarrier,
  FlightNum,
  TailNum,
  DepTime 
from endtoendproject.flight


-- COMMAND ----------

select * from reporting_flight limit 1

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("SELECT * FROM reporting_flight")
-- MAGIC df.write.format("delta").mode("Overwrite").save("/mnt/raw_datalake_mart/reporting_flight")

-- COMMAND ----------


