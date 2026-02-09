# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "dimpurchitem"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
purchitemDf = spark.table("90110adbdev_7405605216081259.bronze.purchitem")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
dimpurchitemDf = purchitemDf.filter(purchitemDf.RecordId.isNotNull()
).select(
    F.trim(purchitemDf.ItemId).alias("ItemId"),
    F.trim(purchitemDf.Txt).alias("Txt"),
    F.when(purchitemDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(purchitemDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    F.from_utc_timestamp(purchitemDf.DataLakeModified_DateTime,'CST').alias("DateLakeModified_DateTime"),
    F.from_utc_timestamp(purchitemDf.ValidFrom,'CST').alias("ValidFrom"),
    F.from_utc_timestamp(purchitemDf.ValidTo,'CST').alias("ValidTo"),
    F.trim(purchitemDf.Price).alias("Price"),
    purchitemDf.RecordId.alias("RecordId"),
    purchitemDf.CategoryID.alias("CategoryID"),
).withColumn("UpdateDateTime",F.lit(UpdateDateTime)
).withColumn("purchitemHashKey",F.xxhash64("RecordId")
)
display(dimpurchitemDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final =  dimpurchitemDf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)