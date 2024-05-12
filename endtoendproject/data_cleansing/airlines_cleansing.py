# Databricks notebook source
df = (
         spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", "/dbfs/FileStore/schema/tables/airlines")
         .load('/mnt/raw_datalake/airlines')
     )

# COMMAND ----------

df=spark.read.json("/mnt/raw_datalake/airlines")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import explode
df1=df.select(explode("response"),"Datepart")
display(df1)

# COMMAND ----------

df_final=df1.select("col.*","Datepart")

# COMMAND ----------

df_final.write.format("Delta").mode("Overwrite").save("/mnt/raw_datalake_cleansed/airlines")

# COMMAND ----------

df=spark.read.format("delta").load('/mnt/raw_datalake_cleansed/airlines')
df.createOrReplaceTempView('airlines')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE endtoendproject.airlines AS
# MAGIC SELECT * FROM airlines;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from endtoendproject.airlines limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) from airlines
# MAGIC

# COMMAND ----------


