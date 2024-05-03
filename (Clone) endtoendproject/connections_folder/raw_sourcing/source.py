# Databricks notebook source
# pip install tabula-py

# COMMAND ----------

# import tabula
# from datetime import date
# tabula.convert_into('/dbfs/mnt/blob_source/PLANE.pdf',f'/dbfs/mnt/raw_datalake/PLAIN/datepart={date.today()}/Plain.csv',output_format='csv',pages='all')


# COMMAND ----------

# dbutils.fs.ls('/mnt/blob_source/')

# COMMAND ----------

# import tabula
# from datetime import date
# import os

# # Verify the output directory exists, create it if necessary
# output_directory = f'/dbfs/mnt/raw_datalake/PLAIN/datepart={date.today()}'
# os.makedirs(output_directory, exist_ok=True)


# # Convert the PDF to CSV
# tabula.convert_into('/dbfs/mnt/blob_source/PLANE.pdf',
#                     f'{output_directory}/Plain.csv',
#                     output_format='csv',
#                     pages='all')

# COMMAND ----------

# list_files=[(i.name,i.name.split('.')[0]) for i in dbutils.fs.ls('/mnt/blob_source/') if (i.name.split('.')[1]=='pdf')]
# print(list_files)

# COMMAND ----------


import tabula
from datetime import date
import os

def f_source_datalake_parametrize(source_path, sink_path, output_format, pages, file_name):
    output_directory = f'{sink_path}{file_name.split(".")[0]}/datepart={date.today()}'
    os.makedirs(output_directory, exist_ok=True)

    # Convert the PDF to CSV
    tabula.convert_into(f'{source_path}{file_name}',
                        f'{output_directory}/{file_name}',
                        output_format=output_format,
                        pages=pages)

# COMMAND ----------

list_files=[(i.name,i.name.split('.')[0]) for i in dbutils.fs.ls('/mnt/blob_source/') if (i.name.split('.')[1]=='pdf')]
for i in list_files:
    f_source_datalake_parametrize('/dbfs/mnt/blob_source/','/dbfs/mnt/raw_datalake/','csv','all',i[0])

# COMMAND ----------


