# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# DBTITLE 1,Cell 3
UpdateDateTime = datetime.datetime.now()
Entity = "dimworker"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

workerdf= spark.table("90110adbdev_7405605216081259.bronze.workertable")
verticaldf = spark.table("90110adbdev_7405605216081259.silver.dimvertical")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
dimworkerdf = workerdf.filter(workerdf.RecordId.isNotNull()
    ).join(
        verticaldf,workerdf.Vertical ==verticaldf.Vertical,"left"
    ).select(
        workerdf.WorkerID,
        F.when(workerdf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(workerdf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(workerdf.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
        workerdf.SupervisorId,
        F.trim(workerdf.WorkerName).alias("WorkerName"),
        F.trim(workerdf.WorkerEmail).alias("WorkerEmail"),
        F.trim(workerdf.Phone).alias("Phone"),
        F.from_utc_timestamp(workerdf.DOJ,"CST").alias("DOJ"),
        F.from_utc_timestamp(workerdf.DOL,"CST").alias("DOL"),
        verticaldf.VerticalId,
        workerdf.Type,
        workerdf.PayPerAnnum,
        workerdf.Rate,
        workerdf.RecordId.alias("WorkerRecordId")
        ).withColumn("UpdateDateTime",F.lit(UpdateDateTime)
        ).withColumn("WorkerHashKey",F.xxhash64("WorkerRecordId")
        )
display(dimworkerdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final DataFrame

# COMMAND ----------

df_final = dimworkerdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"silver",Entity) 