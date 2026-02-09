# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "CostCenter"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
CostCenter_Df = spark.table("90110adbdev_7405605216081259.bronze.CostCenter")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
CostCenterdf = CostCenter_Df.filter(CostCenter_Df.RecordId.isNotNull()
).select(
    (CostCenter_Df.CostCenterNumber).alias("CostCenterNumber"),
    F.when(CostCenter_Df.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(CostCenter_Df.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    F.from_utc_timestamp(CostCenter_Df.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
    F.trim(CostCenter_Df.Vat).alias("Vat"),
    (CostCenter_Df.RecordId).alias("RecordId"),
).withColumn("UpdateDateTime",F.lit(UpdateDateTime)
).withColumn("CostCenterHashKey",F.xxhash64("RecordId")
)
display(CostCenterdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final = CostCenterdf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)