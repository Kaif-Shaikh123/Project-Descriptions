# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "dimvendor"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
VendorDf = spark.table("90110adbdev_7405605216081259.bronze.VendTable")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
dimvendorDf = VendorDf.filter(VendorDf.RecordId.isNotNull()
).select(
    VendorDf.VendId.alias("VendorId"),
    F.trim(VendorDf.VendorName).alias("VendorName"),
    F.when(VendorDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(VendorDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    F.from_utc_timestamp(VendorDf.ValidFrom,'CST').alias("DateLakeModified_DateTime"),
    F.trim(VendorDf.Address).alias("Address"),
    F.trim(VendorDf.City).alias("City"),
    F.trim(VendorDf.Country).alias("Country"),
    F.trim(VendorDf.ZipCode).alias("ZipCode"),
    F.trim(VendorDf.Region).alias("Region"),
    F.from_utc_timestamp(VendorDf.ValidFrom,'CST').alias("ValidFrom"),
    F.from_utc_timestamp(VendorDf.ValidTo,'CST').alias("ValidTo"),
    VendorDf.Active.alias("Active"),
    VendorDf.RecordId.alias("VendorRecordId"),
    F.trim(VendorDf.TaxId).alias("TaxId"),
    F.trim(VendorDf.CurrencyCode).alias("CurrencyCode"),
).withColumn("UpdateDateTime",F.lit(UpdateDateTime)
).withColumn("VendorHashKey",F.xxhash64("VendorRecordId")
).withColumn("VendorDiscount",F.when(F.col("Country")== "US",F.lit(0.01)).when(F.col("Country") == "UK",F.lit(0.006)).otherwise(F.lit(0)))
display(dimvendorDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final =  dimvendorDf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)