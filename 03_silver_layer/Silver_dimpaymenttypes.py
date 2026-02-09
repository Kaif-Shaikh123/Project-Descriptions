# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# DBTITLE 1,Cell 3
UpdateDateTime = datetime.datetime.now()
Entity = "dimpaymenttypes"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

salesorderlinedf= spark.table("90110adbdev_7405605216081259.bronze.salesorderline")
display(salesorderlinedf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create silver dimension table

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC  CREATE TABLE IF NOT EXISTS 90110adbdev_7405605216081259.silver.dimpaymenttypes(
# MAGIC     PaymentTypeID int,
# MAGIC     PaymentTypeDesc string
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
df = salesorderlinedf.select("PaymentTypeDesc").distinct()
display(df)

# COMMAND ----------

paymenttypedf = spark.table("90110adbdev_7405605216081259.silver.dimpaymenttypes")
display(paymenttypedf)

# COMMAND ----------

newrowsdf = df.exceptAll(paymenttypedf.select("PaymentTypeDesc"))
display(newrowsdf)


# COMMAND ----------

maxdf = spark.sql("select ifnull(max(PaymentTypeId),0) as maxid from {df}",df=paymenttypedf)
toprow = maxdf.head(1)
maxid = toprow[0][0]
print(maxid)

# COMMAND ----------

from pyspark.sql import Window as W

# COMMAND ----------

# DBTITLE 1,Cell 14
ids_df = newrowsdf.withColumn("paymentTypeId", F.row_number().over(W.orderBy(F.col("PaymentTypeDesc")))) 
display(ids_df)

# COMMAND ----------

idsFinal = ids_df.withColumn("paymentTypeId", F.col("PaymentTypeId")+maxid)
display(idsFinal)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 90110adbdev_7405605216081259.silver.dimpaymenttypes

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

DF_Final=idsFinal

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

appendToDeltaTable(DF_Final,"silver",Entity)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dimpaymenttypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into bronze.salesorderline(PaymentTypeDesc) values("Paypal")