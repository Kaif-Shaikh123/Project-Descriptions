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

PurchItem_schema = StructType([
    StructField("ItemId",StringType(), True),
    StructField("Txt",StringType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("ValidFrom",TimestampType(), True),
    StructField("ValidTo",TimestampType(), True),
    StructField("Price", StringType(), True),
    StructField("RecordId",IntegerType(), True),
    StructField("CategoryID",IntegerType(), True)
    ])

df_PurchItem= (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(PurchItem_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/PurchItem/")
)

display(df_PurchItem)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "PurchItem"
deltaLakePath ="DeltaLake/Raw/Purchase/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_PurchItem,deltaLakePath)