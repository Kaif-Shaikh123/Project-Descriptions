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

PaymentsTypes_schema = StructType([
    StructField("PaymentTypeId", IntegerType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("PaymentTypeName", StringType(), True),
    StructField("RecordId", IntegerType(), True)
   ])

df_PaymentsTypes = (
    spark.read
        .option("header", "false")
        .option("delimiter", ",")
        .schema(PaymentsTypes_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Sales/PaymentTypes/PaymentTypes.csv")
)

display(df_PaymentsTypes)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "PaymentTypes"
deltaLakePath ="DeltaLake/Raw/Sales/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_PaymentsTypes,deltaLakePath)