# Databricks notebook source
df=spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv').option("InferSchema",True)\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/schema/tables/PLANE")\
        .load('/mnt/raw_datalake/PLANE')
    

# COMMAND ----------

df.printSchema

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()



# COMMAND ----------

from delta.tables import DeltaTable
df_base=df.selectExpr("tailnum as tail_id","type","manufacturer","to_date(issue_date) as issue_date","cast('year' as int) as year","model","status","aircraft_type","engine_type","to_date(datepart,'yy-MM-dd') as datepart")
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/PLANE")\
    .start("/mnt/raw_datalake_cleansed/plane")

# COMMAND ----------

def print_schema(df):
    try:
        schema=""
        for i in df.dtypes:
            schema=schema+ i[0] + " " + i[1] + ","
        return(schema[0:-1])
    except Exception as err:
        print("Error is",str(err))


# COMMAND ----------

def delta_cleansed_load(tablename,location,schema,database):
    try:
        spark.sql(f"""
                  create table {database}.{tablename}
                  ({schema})
                  using delta
                  location '{location}'
                  """)
    except Exception as err:
        print("Error is ",str(err))

# COMMAND ----------

df=spark.read.format("delta").load('/mnt/raw_datalake_cleansed/plane')
#schema=print_schema(df)
#print(schema)
#delta_cleansed_load('plane','/mnt/raw_datalake_cleansed/plane',schema,'cleansed_endtoendproject')
display(df)
df.createOrReplaceTempView('plane')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create database if not exists endtoendproject
# MAGIC CREATE TABLE endtoendproject.plane AS
# MAGIC SELECT * FROM plane;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from endtoendproject.plane limit 10

# COMMAND ----------


