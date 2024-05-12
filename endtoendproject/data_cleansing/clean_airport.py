# Databricks notebook source
dbutils.fs.ls('/mnt/raw_datalake/')

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv').option("InferSchema",True)\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/schema/tables/airport")\
        .load('/mnt/raw_datalake/airport')

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()



# COMMAND ----------

df_base=df.selectExpr("code",
                      "split(Description,',')[0] as city",
                      "split(split(Description,',')[1],':')[0] as country",
                      "split(split(Description,',')[1],':')[1] as airport",
                      "to_date(Dartpart,'yy-MM-dd') as datepart")
display(df_base)
# df_base.writeStream.trigger(once=True)\
#     .format("delta")\
#     .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/PLANE")\
#     .start("/mnt/raw_datalake_cleansed/plane")

# COMMAND ----------

from delta.tables import DeltaTable
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/airport")\
    .start("/mnt/raw_datalake_cleansed/airport")

# COMMAND ----------

df=spark.read.format("delta").load('/mnt/raw_datalake_cleansed/airport')
df.createOrReplaceTempView('airport')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE endtoendproject.airport AS
# MAGIC -- SELECT * FROM airport;
# MAGIC desc history endtoendproject.airport

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airport

# COMMAND ----------


