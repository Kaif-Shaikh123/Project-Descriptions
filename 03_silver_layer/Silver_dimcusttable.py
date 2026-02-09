# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "custtable"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
custtableDf = spark.table("90110adbdev_7405605216081259.bronze.custtable")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
custtable_Df = custtableDf.filter(custtableDf.RecordId.isNotNull()
).select(
    custtableDf.CustomerId.alias("CustomerId"),
    F.when(custtableDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(custtableDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    F.from_utc_timestamp(custtableDf.DataLakeModified_DateTime,'CST').alias("DateLakeModified_DateTime"),
    F.trim(custtableDf.CustomerName).alias("CustomerName"),
    F.trim(custtableDf.Email).alias("Email"),
    F.trim(custtableDf.Phone).alias("Phone"),
    F.trim(custtableDf.Address).alias("Address"),
    F.trim(custtableDf.City).alias("City"),
    F.trim(custtableDf.State).alias("State"),
    F.trim(custtableDf.Country).alias("Country"),
    F.trim(custtableDf.ZipCode).alias("ZipCode"),
    F.trim(custtableDf.Region).alias("Region"),
    F.from_utc_timestamp(custtableDf.SignupDate,'CST').alias("SignupDate"),
    custtableDf.RecordId.alias("CustRecordId")
  ).withColumn("UpdateDateTime", F.lit(UpdateDateTime)
  ).withColumn("CustHashkey",  F.xxhash64("CustRecordId")
  )
display(custtable_Df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final =  custtable_Df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)