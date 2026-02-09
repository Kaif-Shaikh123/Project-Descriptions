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

CostCenter_schema = StructType([
    StructField("CostCenterNumber", IntegerType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("Vat", StringType(), True),
    StructField("RecordId", IntegerType(), True)
    
])

df_CostCenter= (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(CostCenter_schema)
    .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Others/CostCenter/")
)

display(df_CostCenter)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "CostCenter"
deltaLakePath ="DeltaLake/Raw/Others/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_CostCenter,deltaLakePath)