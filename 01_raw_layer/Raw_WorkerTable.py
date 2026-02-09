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

WorkerTable_schema = StructType([
    StructField("WorkerID", IntegerType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("SupervisorId", IntegerType(), True),
    StructField("WorkerName", StringType(), True),
    StructField("WorkerEmail", StringType(), True),
    StructField("Phone",StringType(), True),
    StructField("DOJ", TimestampType(), True),
    StructField("DOL", TimestampType(), True),
    StructField("Vertical", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("PayPerAnnum", IntegerType(), True),
    StructField("Rate", IntegerType(), True),
    StructField("RecordId",IntegerType(), True),
   ])

df_WorkerTable= (
    spark.read
        .option("header", "false")
        .option("delimiter", ",")
        .schema(WorkerTable_schema)
        .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/HR/WorkerTable/WorkerTable.csv")
)

display(df_WorkerTable)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "WorkerTable"
deltaLakePath ="DeltaLake/Raw/Hr/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_WorkerTable,deltaLakePath)