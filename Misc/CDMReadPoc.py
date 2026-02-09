# Databricks notebook source
# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/ADLS_oauth

# COMMAND ----------

df = (spark.read.format("csv")
      .option("path","abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/Parties/")
      .load())
display(df)

# COMMAND ----------

service_credential = dbutils.secrets.get(scope = "adbdevscope", key = "Client--Secret")
app_id = dbutils.secrets.get(scope = "adbdevscope", key = "app--id")
tenantid = dbutils.secrets.get(scope = "adbdevscope", key = "Tenant--ID")

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType
)

parties_schema = StructType([
    StructField("PartyId", IntegerType(), True),
    StructField("PartyName", StringType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("PartyAddressCode", TimestampType(), True),
    StructField("EstablishedDate", StringType(), True),
    StructField("PartyEmailId", StringType(), True),
    StructField("PartyContactNumber", IntegerType(), True),
    StructField("RecordId", StringType(), True),
    StructField("TaxId", StringType(), True)
])

df_parties = (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(parties_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/Parties/")
)

display(df_parties)