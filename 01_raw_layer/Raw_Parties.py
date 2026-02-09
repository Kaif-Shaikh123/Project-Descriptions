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

parties_schema = StructType([
    StructField("PartyId", IntegerType(), True),
    StructField("PartyName", StringType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("PartyAddressCode", TimestampType(), True),
    StructField("EstablishedDate", StringType(), True),
    StructField("PartyEmailId", StringType(), True),
    StructField("PartyContactNumber", StringType(), True),
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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "Parties"
deltaLakePath ="DeltaLake/Raw/Purchase/"+ entity

# COMMAND ----------

df_parties.write.mode("overwrite").option("overwriteSchema", "true").option("path",ADLS_DEV_BASE_PATH + deltaLakePath).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_parties,deltaLakePath)