# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "promotable"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
promotableDf = spark.table("90110adbdev_7405605216081259.bronze.promotable")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
promotable_Df =promotableDf.filter(promotableDf.RecordId.isNotNull()
).select(
    promotableDf.PromotionId,
    F.when(promotableDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(promotableDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    F.from_utc_timestamp(promotableDf.DataLakeModified_DateTime,'CST').alias("DateLakeModified_DateTime"),
    F.trim(promotableDf.PromotionName).alias("PromotionName"),
    F.trim(promotableDf.PromoCode).alias("PromoCode"),
    F.trim(promotableDf.PromoType).alias("PromoType"),
    promotableDf.PromoPercentage,
    F.from_utc_timestamp(promotableDf.ValidFrom,'CST').alias("ValidFrom"),
    F.from_utc_timestamp(promotableDf.ValidTo,'CST').alias("ValidTo"),
    promotableDf.IsActive,
    promotableDf.RecordId.alias("PromoRecordId")
  ).withColumn("UpdateDateTime", F.lit(UpdateDateTime)
  ).withColumn("PrmomHashkey", F.xxhash64("PromoRecordId")
  )
display(promotable_Df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final =  promotable_Df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)