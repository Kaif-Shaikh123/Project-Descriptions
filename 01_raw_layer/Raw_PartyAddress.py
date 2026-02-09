# Databricks notebook source
# MAGIC %md
# MAGIC ## Run SharedLiblaries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Entity

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType
)

PartyAddress_schema = StructType([
    StructField("PartyNumber", IntegerType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("ValidFrom", TimestampType(), True),
    StructField("ValidTo", TimestampType(), True),
    StructField("RecordId", StringType(), True)
   ])

df_PartyAddress = (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(PartyAddress_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/PartyAddress/")
)

display(df_PartyAddress)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "PartyAddress"
deltaLakePath ="DeltaLake/Raw/Purchase/"+ entity

# COMMAND ----------

df_PartyAddress.write.mode("overwrite").option("overwriteSchema", "true").option("path",ADLS_DEV_BASE_PATH + deltaLakePath).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_PartyAddress,deltaLakePath)