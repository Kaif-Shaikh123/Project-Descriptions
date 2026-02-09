# Databricks notebook source
import pyspark.sql.functions as F
import datetime 
import pandas as pd
import dateutil
from pyspark.sql import Window as W

# COMMAND ----------

ADLS_DEV_BASE_PATH = "abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/"
DELTALAKE_RAW_PATH = "DeltaLake/Raw/"

# COMMAND ----------

service_credential = dbutils.secrets.get(scope = "adbdevscope", key = "Client--Secret")
app_id = dbutils.secrets.get(scope = "adbdevscope", key = "app--id")
tenantid = dbutils.secrets.get(scope = "adbdevscope", key = "Tenant--ID")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope = "adbdevscope", key = "Client--Secret")
app_id = dbutils.secrets.get(scope = "adbdevscope", key = "app--id")
tenantid = dbutils.secrets.get(scope = "adbdevscope", key = "Tenant--ID")

spark.conf.set("fs.azure.account.auth.type.90110adlsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.90110adlsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.90110adlsdev.dfs.core.windows.net", app_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.90110adlsdev.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.90110adlsdev.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")

# COMMAND ----------

def writeRawToDeltaLake(entitydf,deltaLakePath):
    entitydf.write.option("overwriteSchema", "true").option("mergeSchema", "true").mode("overwrite").option("path",ADLS_DEV_BASE_PATH + deltaLakePath).save()

# COMMAND ----------

def readFromDeltaPath(entityName):
    df= (spark.read.format("delta")
         .option("path",f"{ADLS_DEV_BASE_PATH}/{DELTALAKE_RAW_PATH}/{entityName}")
         .load()
         )
    return df

# COMMAND ----------

def saveDeltaTableToCatlog(df,schema,tableName):
    schema = schema.lower()
    tableName = tableName.lower()
    spark.catalog.setCurrentCatalog("90110adbdev_7405605216081259")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"DROP TABLE IF EXISTS {schema}.{tableName}")
    df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{tableName}")

# COMMAND ----------

def appendToDeltaTable(df,schema,tableName):
    schema = schema.lower()
    tableName = tableName.lower()
    spark.catalog.setCurrentCatalog("90110adbdev_7405605216081259")
    df.write.format("delta").mode("append").option("mergeSchema","True").saveAsTable(f"{schema}.{tableName}")