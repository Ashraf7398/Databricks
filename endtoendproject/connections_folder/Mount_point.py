# Databricks notebook source
# %scala
# val containerName = dbutils.secrets.get(scope = "endtoendproject-scope", key = "conatinername")
# val storageAccountName = dbutils.secrets.get(scope = "endtoendproject-scope", key = "storageaccountname")
# val sas = dbutils.secrets.get(scope = "endtoendproject-scope", key = "sas")
# val config = s"fs.azure.sas.$containerName.$storageAccountName.blob.core.windows.net"

# val extraConfigs = Map(config -> sas)

# dbutils.fs.mount(
#   source = f"wasbs://source@endtoendprojecttblob.blob.core.windows.net",
#   mountPoint = "/mnt/blob_source/",
#   extraConfigs = extraConfigs
# )


# COMMAND ----------

# %py
# dbutils.fs.ls('/mnt/blob_source/')

# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
#            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#            "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="endtoendproject-scope", key="dataappid"),
#            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="endtoendproject-scope", key="datasecretid"),
#            "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="endtoendproject-scope", key="dataclientrefresh-url")}

# mountPoint = "/mnt/raw_datalake/"
# if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
#     dbutils.fs.mount(
#         source=dbutils.secrets.get(scope="endtoendproject-scope", key="datalake-raw"),
#         mount_point=mountPoint,
#         extra_configs=configs
#     )

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="endtoendproject-scope", key="dataappid"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="endtoendproject-scope", key="datasecretid"),
           "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="endtoendproject-scope", key="dataclientrefresh-url")}

mountPoint = "/mnt/raw_datalake_cleansed/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=dbutils.secrets.get(scope="endtoendproject-scope", key="datalake-cleansed"),
        mount_point=mountPoint,
        extra_configs=configs
    )

# COMMAND ----------

dbutils.fs.ls('/mnt/raw_datalake_cleansed/')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


