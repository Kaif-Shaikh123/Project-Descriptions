# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "dimcurrency"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
currencyDf = spark.table("90110adbdev_7405605216081259.bronze.currency")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
dimcurrencyDf = currencyDf.filter(currencyDf.RecordId.isNotNull()
).select(
    (currencyDf.CurrencyId).alias("CurrencyId"),
    F.trim(currencyDf.Code).alias("Code"),
    F.when(currencyDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(currencyDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    F.from_utc_timestamp(currencyDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
    F.trim(currencyDf.Country).alias("Country"),
    F.trim(currencyDf.CurrencyName).alias("CurrencyName"),
    (currencyDf.RecordId).alias("RecordId"),
).withColumn("UpdateDateTime",F.lit(UpdateDateTime)
).withColumn("currencyHashKey",F.xxhash64("RecordId")
)
display(dimcurrencyDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final =  dimcurrencyDf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)