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

SalesOrderLine_schema = StructType([
    StructField("SalesOrderNumber", StringType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("SalesOrderLine", IntegerType(), True),
    StructField("ItemId", StringType(), True),
    StructField("Qty", IntegerType(), True),
    StructField("Price", StringType(), True),
    StructField("VatPercentage", StringType(), True),
    StructField("CurrencyCode", StringType(), True),
    StructField("BookDate", TimestampType(), True),
    StructField("ShippedDate", TimestampType(), True),
    StructField("DeliveredDate", TimestampType(), True),
    StructField("TrackingNumber", StringType(), True),
    StructField("CustId", IntegerType(), True),
    StructField("PaymentTypeDesc", StringType(), True),
    StructField("RecordId", IntegerType(), True)
   ])
   

df_SalesOrderLine = (
    spark.read
        .option("header", "false")
        .option("delimiter", ",")
        .schema(SalesOrderLine_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Sales/SalesOrderLine/SalesOrderLine.csv")
)

display(df_SalesOrderLine)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "SalesOrderLine"
deltaLakePath ="DeltaLake/Raw/Sales/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_SalesOrderLine,deltaLakePath)