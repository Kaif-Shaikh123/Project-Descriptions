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

PromoTable_schema = StructType([
    StructField("PromotionId", IntegerType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("PromotionName", StringType(), True),
    StructField("PromoCode", IntegerType(), True),
    StructField("PromoType", StringType(), True),
    StructField("PromoPercentage", IntegerType(), True),
    StructField("ValidFrom", TimestampType(), True),
    StructField("ValidTo", TimestampType(), True),
    StructField("IsActive", IntegerType(), True),
    StructField("RecordId", IntegerType(), True)
   ])

df_PromoTable = (
    spark.read
        .option("header", "false")
        .option("delimiter", ",")
        .schema(PromoTable_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Sales/PromoTable/PromoTable.csv")
)

display(df_PromoTable)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "PromoTable"
deltaLakePath ="DeltaLake/Raw/Sales/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_PromoTable,deltaLakePath)