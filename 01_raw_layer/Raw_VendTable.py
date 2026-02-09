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

VendTable_schema = StructType([
    StructField("VendId", IntegerType(), True),
    StructField("VendorName", StringType(), True),
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
    StructField("Active", IntegerType(), True),
    StructField("RecordId", StringType(), True),
    StructField("TaxId", StringType(), True),
    StructField("CurrencyCode", StringType(), True)
   ])

df_VendTable = (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(VendTable_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/VendTable/")
)

display(df_VendTable)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "VendTable"
deltaLakePath ="DeltaLake/Raw/Purchase/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_VendTable,deltaLakePath)