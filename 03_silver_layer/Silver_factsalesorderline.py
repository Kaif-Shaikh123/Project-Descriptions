# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# DBTITLE 1,Cell 3
UpdateDateTime = datetime.datetime.now()
Entity = "factsalesorderline"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

salesorderlinedf= spark.table("90110adbdev_7405605216081259.bronze.salesorderline")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW vwPromotable 
# MAGIC AS
# MAGIC
# MAGIC SELECT 
# MAGIC   PromotionId,
# MAGIC   CASE PromotionName
# MAGIC     WHEN  'Volume Discount 11 to 20' THEN 11
# MAGIC     WHEN  'Volume Discount 21 to 40' THEN 21
# MAGIC     WHEN  'Volume Discount 41 to 60' THEN 41
# MAGIC     WHEN  'Volume Discount > 60' THEN 61
# MAGIC     ELSE NULL
# MAGIC   END  VolumeStart,
# MAGIC   CASE PromotionName
# MAGIC     WHEN  'Volume Discount 11 to 20' THEN 20
# MAGIC     WHEN  'Volume Discount 21 to 40' THEN 40
# MAGIC     WHEN  'Volume Discount 41 to 60' THEN 60
# MAGIC     WHEN  'Volume Discount > 60' THEN 9999999
# MAGIC       ELSE NULL
# MAGIC   END VolumeEnd,
# MAGIC   ValidFrom,
# MAGIC   ValidTo,
# MAGIC   PromoPercentage
# MAGIC   FROM
# MAGIC     90110adbdev_7405605216081259.bronze.promotable

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vwPromotable

# COMMAND ----------

# DBTITLE 1,Cell 9
# MAGIC %sql
# MAGIC     
# MAGIC  
# MAGIC CREATE OR REPLACE TEMP VIEW vwFactSalesOrderLine
# MAGIC AS
# MAGIC
# MAGIC SELECT 
# MAGIC   S.SalesOrderNumber,
# MAGIC   S.SalesOrderLine,
# MAGIC   CASE
# MAGIC       WHEN isnull(S.LastProcessedChange_DateTime)
# MAGIC           THEN '1900-01-01'
# MAGIC       ELSE
# MAGIC           S.LastProcessedChange_DateTime
# MAGIC   END  AS LastProcessedChange_DateTime,
# MAGIC   from_utc_timestamp(S.DataLakeModified_DateTime,'CST') AS DataLakeModified_DateTime,
# MAGIC   S.ItemId,
# MAGIC   S.Qty,
# MAGIC   S.Price,
# MAGIC   S.Qty *S.Price AS TotalAmount,
# MAGIC   CASE 
# MAGIC     WHEN PR.PromotionId is null THEN TotalAmount
# MAGIC   ELSE
# MAGIC    TotalAmount * (1-Pr.promoPercentage) 
# MAGIC   END AS TotalAmountWithDiscount,
# MAGIC    S.VatPercentage,
# MAGIC   TotalAmountWithDiscount * S.VatPercentage as VatAmount,
# MAGIC   TotalAmountWithDiscount + VatAmount As TotalOrderAmount,
# MAGIC   C.CurrencyId,
# MAGIC   from_utc_timestamp(S.BookDate,'CST') AS BookDate,
# MAGIC   cast(date_format(S.BookDate,'yyyyMMdd') AS INT) AS BookDateKey,
# MAGIC   from_utc_timestamp(S.ShippedDate,'CST') AS ShippedDate,
# MAGIC   cast(date_format(S.ShippedDate,'yyyyMMdd') AS INT) AS ShippedDateKey,
# MAGIC   from_utc_timestamp(S.DeliveredDate,'CST') AS DeliveredDate,
# MAGIC   cast(date_format(S.DeliveredDate,'yyyyMMdd') AS INT) AS DeliveredDateKey,
# MAGIC   S.TrackingNumber,
# MAGIC   S.CustId,
# MAGIC   P.PaymentTypeId,
# MAGIC   PR.PromotionId,
# MAGIC   current_timestamp() AS UpdatedDateTime,
# MAGIC   xxhash64(S.RecordId) As SalesOrderLineRecordId
# MAGIC FROM 90110adbdev_7405605216081259.bronze.salesorderline AS S
# MAGIC LEFT JOIN 90110adbdev_7405605216081259.bronze.currency AS C On S.CurrencyCode = C.Code
# MAGIC LEFT JOIN 90110adbdev_7405605216081259.silver.dimpaymenttype AS P On S.PaymentTypeDesc = P.PaymentTypeDesc
# MAGIC LEFT JOIN vwPromotable AS Pr On 
# MAGIC   CASE
# MAGIC     WHEN month(S.BookDate) = 1 THEN S.BookDate BETWEEN  PR.ValidFrom AND  PR.ValidTo
# MAGIC   ELSE
# MAGIC    S.Qty BETWEEN PR.VolumeStart AND PR.VolumeEnd
# MAGIC   END

# COMMAND ----------

factsalesorderlinedf = spark.table("vwFactSalesOrderLine")
display(factsalesorderlinedf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final = factsalesorderlinedf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final, "silver", Entity)