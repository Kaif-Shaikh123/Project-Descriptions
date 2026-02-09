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

PurchContracts_schema = StructType([
    StructField("ContractId",StringType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("FirstParty",StringType(), True),
    StructField("SecondParty", StringType(), True),
    StructField("ValidFrom", TimestampType(), True),
    StructField("ValidTo", TimestampType(), True),
    StructField("IsActive", IntegerType(), True),
    StructField("RecordId",IntegerType(), True)
    ])

df_PurchContracts= (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(PurchContracts_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/PurchContracts/")
)

display(df_PurchContracts)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "PurchContracts"
deltaLakePath ="DeltaLake/Raw/Purchase/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_PurchContracts,deltaLakePath)