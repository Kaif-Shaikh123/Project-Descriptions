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

FiscalPeriod_schema = StructType([
    StructField("FiscalPeriodName", StringType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("FiscalStartDate", TimestampType(), True),
    StructField("FiscalEndDate", TimestampType(), True),
    StructField("FiscalMonth", IntegerType(), True),
    StructField("FiscalYearStart", TimestampType(), True),
    StructField("FiscalYearEnd", TimestampType(), True),
    StructField("FiscalQuarter", IntegerType(), True),
    StructField("FiscalQuarterStart", TimestampType(), True),
    StructField("FiscalQuarterEnd", TimestampType(), True),
    StructField("FiscalYear", IntegerType(), True),
    StructField("RecordId", StringType(), True)
   ])

df_FiscalPeriod = (
    spark.read
        .option("header", "false")
        .option("delimiter", ",")
        .schema(FiscalPeriod_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Others/FiscalPeriod/Fiscal Period.csv")
)

display(df_FiscalPeriod)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "FiscalPeriod"
deltaLakePath ="DeltaLake/Raw/Others/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_FiscalPeriod,deltaLakePath)