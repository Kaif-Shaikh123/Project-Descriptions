# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "dimpurchasecategory"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
purchasecategoryDf = spark.table("90110adbdev_7405605216081259.bronze.purchcategory")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
dimpurchasecategoryDf = purchasecategoryDf.filter(purchasecategoryDf.RecordId.isNotNull()
).select(
    purchasecategoryDf.CategoryId.alias("CategoryId"),
    F.trim(purchasecategoryDf.CategoryName).alias("CategoryName"),
    F.when(purchasecategoryDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(purchasecategoryDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    F.from_utc_timestamp(purchasecategoryDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
    F.trim(purchasecategoryDf.CategoryGroupId).alias("CategoryGroupId"),
    purchasecategoryDf.RecordId.alias("RecordId"),
).withColumn("UpdateDateTime",F.lit(UpdateDateTime)
).withColumn("purchcategoryHashKey",F.xxhash64("RecordId"))
display(dimpurchasecategoryDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final =  dimpurchasecategoryDf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)