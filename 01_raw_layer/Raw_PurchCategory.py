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

PurchCategory_schema = StructType([
    StructField("CategoryId", IntegerType(), True),
    StructField("CategoryName", StringType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("CategoryGroupId", StringType(), True),
     StructField("RecordId", StringType(), True)
    ])

df_PurchCategory= (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(PurchCategory_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/PurchCategory/")
)

display(df_PurchCategory)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "PurchCategory"
deltaLakePath ="DeltaLake/Raw/Purchase/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_PurchCategory,deltaLakePath)