# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "factpurchaseorder"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
purchaseorderDf = spark.table("90110adbdev_7405605216081259.bronze.purchaseorder")
dimcostcenterDf = spark.table("90110adbdev_7405605216081259.silver.dimcostcenter")
dimcurrencyDf = spark.table("90110adbdev_7405605216081259.silver.dimcurrency")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
factpurchaseorderDf = purchaseorderDf.filter(purchaseorderDf.RecordId.isNotNull()
    ).join(dimcostcenterDf, purchaseorderDf.CostCenter == dimcostcenterDf.CostCenterNumber, "left"
    ).join(dimcurrencyDf, purchaseorderDf.currencycode == dimcurrencyDf.Code, "left"
    ).select(
        purchaseorderDf.PoNumber,
        purchaseorderDf.LineItem,
        purchaseorderDf.VendId.alias("Vendorkey"),
        F.when(purchaseorderDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(purchaseorderDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(purchaseorderDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        purchaseorderDf.Qty,
        purchaseorderDf.PurchasePrice,
        purchaseorderDf.TotalOrder.cast("double").alias("TotalOrder"),
        purchaseorderDf.CostCenter.alias("CostCenterkey"),
        dimcostcenterDf.Vat.alias("VatPercentage"),
        F.round((purchaseorderDf.TotalOrder * dimcostcenterDf.Vat),4).alias("VatAmount"),
        F.round((purchaseorderDf.TotalOrder + (purchaseorderDf.TotalOrder * dimcostcenterDf.Vat)),4).alias("TotalAmount"),
        purchaseorderDf.ExchangeRate,
        purchaseorderDf.Itemkey,
        dimcurrencyDf.CurrencyId.alias("Currencykey"),
        F.from_utc_timestamp(purchaseorderDf.OrderDate,'CST').alias("OrderDate"),
        F.from_utc_timestamp(purchaseorderDf.ShipDate,'CST').alias("ShipDate"),
        F.from_utc_timestamp(purchaseorderDf.DeliveredDate,'CST').alias("DeliveredDate"),
        F.date_format(purchaseorderDf.OrderDate,'yyyyMMdd').alias("OrderDatekey"),
        F.date_format(purchaseorderDf.ShipDate,'yyyyMMdd').alias("ShipDatekey"),
        F.date_format(purchaseorderDf.DeliveredDate,'yyyyMMdd').alias("DeliveredDatekey"),
        purchaseorderDf.TrackingNumber,
        purchaseorderDf.Batchid.alias("BatchId"),
        purchaseorderDf.CreatedBy,
        purchaseorderDf.RecordId.alias("PurchaseOrderRecordId"),
        purchaseorderDf.CategoryID.alias("CategoryKey")
    ).withColumn("UpdateDateTime", F.lit(UpdateDateTime)
    ).withColumn("PurchaseOrderHashKey", F.xxhash64("PurchaseOrderRecordId")
    )
display(factpurchaseorderDf)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final = factpurchaseorderDf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)