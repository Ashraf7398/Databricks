# Databricks notebook source
dbutils.fs.ls('/mnt/raw_datalake/')

# COMMAND ----------

df = (
         spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "parquet")
         .option("cloudFiles.schemaLocation", "/dbfs/FileStore/schema/tables/Cancellation")
         .load('/mnt/raw_datalake/Cancellation')
     )

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# COMMAND ----------

df_base=df.selectExpr("replace(code,'\"','') as code",
                      "replace(Description,'\"','') as description",
                      "to_date(Datepart,'yy-MM-dd') as datepart")
display(df_base)

# COMMAND ----------

from delta.tables import DeltaTable
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Cancellation")\
    .start("/mnt/raw_datalake_cleansed/cancellation")

# COMMAND ----------

df=spark.read.format("delta").load('/mnt/raw_datalake_cleansed/cancellation')
df.createOrReplaceTempView('cancellation')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE endtoendproject.cancellation AS
# MAGIC SELECT * FROM cancellation;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cancellation limit 10

# COMMAND ----------


