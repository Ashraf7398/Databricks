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

df_flight.createOrReplaceTempView('flight')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct year from flight
# MAGIC select count(*) from flight

# COMMAND ----------

file_path="/mnt/raw_datalake_cleansed/flight"
df_flight.write.format("delta").mode("Overwrite").save(file_path)

# COMMAND ----------


