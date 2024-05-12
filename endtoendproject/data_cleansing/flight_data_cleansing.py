# Databricks notebook source
from functools import reduce
from pyspark.sql import *

# COMMAND ----------

paths=['/mnt/raw_datalake/flight']

# COMMAND ----------

def unionAll(*dfs):
    return reduce(DataFrame.unionAll,dfs)

# COMMAND ----------

dfs = [spark.read.format('csv').options(header='true').load(path) for path in paths]

# COMMAND ----------

df_flight=unionAll(*dfs)

# COMMAND ----------

display(df_flight)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, to_date, from_unixtime, unix_timestamp
df_base=df_flight.selectExpr(
"to_date(concat_ws('-',year,month,dayofmonth),'yyyy-MM-dd') as date",
"from_unixtime(unix_timestamp(case when DepTime = 2400 then 0 else DepTime end,'HHmm'),'HH:mm') as DepTime", 
"from_unixtime(unix_timestamp(case when CRSDepTime = 2400 then 0 else CRSDepTime end,'HHmm'),'HH:mm') as CRSDepTime", 
"from_unixtime(unix_timestamp(case when ArrTime = 2400 then 0 else ArrTime end,'HHmm'),'HH:mm') as ArrTime", 
"from_unixtime(unix_timestamp(case when CRSArrTime = 2400 then 0 else CRSArrTime end,'HHmm'),'HH:mm') as CRSArrTime", 
"UniqueCarrier",
"cast(FlightNum as int) as FlightNum",
"cast(TailNum as int) as TailNum",
"cast(ActualElapsedTime as int) as ActualElapsedTime",
"cast(CRSElapsedTime as int) as CRSElapsedTime",
"cast(AirTime as int) as AirTime",
"cast(ArrDelay as int) as ArrDelay",
"cast(DepDelay as int) as DepDelay",
"Origin",
"Dest",
"cast(Distance as int) as Distance",
"cast(TaxiIn as int) as TaxiIn",
"cast(TaxiOut as int) as TaxiOut",
"Cancelled",
"CancellationCode",
"cast(Diverted as int) as Diverted",
"cast(CarrierDelay as int) as CarrierDelay",
"cast(WeatherDelay as int) as WeatherDelay",
"cast(NASDelay as int) as NASDelay",
"cast(SecurityDelay as int) as SecurityDelay",
"cast(LateAircraftDelay as int) as LateAircraftDelay",
"to_date('Dartpart','yyyy-MM-dd') as Date_part"
)

# COMMAND ----------

df_base.createOrReplaceTempView('flight')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE endtoendproject.flight AS
# MAGIC SELECT * FROM flight;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SET spark.sql.legacy.timeParserPolicy=LEGACY;
# MAGIC -- SELECT * FROM flight LIMIT 10;
# MAGIC -- select count(*) from flight
# MAGIC select distinct year(date) as year from endtoendproject.flight group by year(date)

# COMMAND ----------

file_path="/mnt/raw_datalake_cleansed/flight"
df_base.write.format("delta").mode("Overwrite").save(file_path)

# COMMAND ----------


