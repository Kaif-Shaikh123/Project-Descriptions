# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "dimparty"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
partiesDf = spark.table("90110adbdev_7405605216081259.bronze.parties")
partyaddressDf = spark.table("90110adbdev_7405605216081259.bronze.partyaddress")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

dimpartyDf = partiesDf.join(
    partyaddressDf,partiesDf.PartyId == partyaddressDf.PartyNumber,"left"
    ).filter(partiesDf.RecordId.isNotNull()
).select(
    partiesDf.PartyId,
    F.trim(partiesDf.PartyName).alias("PartyName"),
    F.when(partiesDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(partiesDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    F.from_utc_timestamp(partiesDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
    F.trim(partiesDf.PartyAddressCode).alias("PartyAddressCode"),
    F.from_utc_timestamp(partiesDf.EstablishedDate,'CST').alias("EstablishedDate"),
    F.trim(partiesDf.PartyEmailId).alias("PartyEmailId"),
    F.trim(partiesDf.PartyContactNumber).alias("PartyContactNumber"),
    partiesDf.RecordId.alias("PartyRecordId"),
    F.trim(partiesDf.TaxId).alias("TaxId"),
    F.trim(partyaddressDf.Address).alias("Address"),
    F.trim(partyaddressDf.City).alias("City"),
    F.trim(partyaddressDf.State).alias("State"),
    F.trim(partyaddressDf.Country).alias("Country"),
    # F.trim(partyaddressDf.ZipCode).alias("ZipCode"),
    F.trim(partyaddressDf.Region).alias("Region"),
    F.from_utc_timestamp(partyaddressDf.ValidFrom,'CST').alias("ValidFrom"),
    F.when(partyaddressDf.ValidTo.isNull(),"1900-01-01").otherwise(partyaddressDf.ValidTo).cast("timestamp").alias("ValidTo"),
    partyaddressDf.RecordId.alias("PartyAddressRecordId")
).withColumn("UpdateDateTime",F.lit(UpdateDateTime)
).withColumn("PartyHashKey",F.xxhash64("PartyRecordId")
)
display(dimpartyDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final =  dimpartyDf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)