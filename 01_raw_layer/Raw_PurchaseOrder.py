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

PurchaseOrder_schema = StructType([
    StructField("PoNumber",StringType(), True),
    StructField("LineItem",IntegerType(), True),
    StructField("VendId",IntegerType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("Qty",IntegerType(), True),
    StructField("PurchasePrice", StringType(), True),
    StructField("TotalOrder", StringType(), True),
    StructField("ExchangeRate", StringType(), True),
    StructField("CostCenter", StringType(), True),
    StructField("Itemkey", StringType(), True),
    StructField("currencycode", StringType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("ShipDate", TimestampType(), True),
    StructField("DeliveredDate", TimestampType(), True),
    StructField("TrackingNumber",StringType (), True),
    StructField("Batchid", StringType(), True),
    StructField("CreatedBy", IntegerType(), True),
    StructField("RecordId",IntegerType(), True),
    StructField("CategoryID",IntegerType(), True)
    ])

df_PurchaseOrder= (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(PurchaseOrder_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/PurchaseOrder/")
)

display(df_PurchaseOrder)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "PurchaseOrder"
deltaLakePath ="DeltaLake/Raw/Purchase/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_PurchaseOrder,deltaLakePath)