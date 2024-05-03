# Databricks notebook source
df = (
         spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "parquet")
         .option("cloudFiles.schemaLocation", "/dbfs/FileStore/schema/tables/UNIQUE_CARRIERS")
         .load('/mnt/raw_datalake/UNIQUE_CARRIERS')
     )

# COMMAND ----------

display(df)

# COMMAND ----------

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# COMMAND ----------

df_base=df.selectExpr("replace(Code,'\"','') as code",
                      "replace(Description,'\"','') as description",
                      "to_date(Datepart,'yy-MM-dd') as datepart")
display(df_base)

# COMMAND ----------

from delta.tables import DeltaTable
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS")\
    .start("/mnt/raw_datalake_cleansed/unique_carriers")

# COMMAND ----------

df=spark.read.format("delta").load('/mnt/raw_datalake_cleansed/unique_carriers')
df.createOrReplaceTempView('unique_carriers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from unique_carriers limit 10

# COMMAND ----------


