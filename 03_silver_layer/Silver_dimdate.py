# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

UpdateDateTime = datetime.datetime.now()
Entity = "dimdate"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

# DBTITLE 1,Untitled
fiscalperiodDf = spark.table("90110adbdev_7405605216081259.bronze.fiscalperiod")

# COMMAND ----------

start_date = datetime.date(2018,1,1)
end_date = start_date + dateutil.relativedelta.relativedelta(years=8,months=12,day=31)


start_date = datetime.datetime.strptime(
    f"{start_date}", "%Y-%m-%d"
)
end_date = datetime.datetime.strptime(
    f"{end_date}", "%Y-%m-%d"
)
print(start_date)
print(end_date)

# COMMAND ----------

datedf = pd.date_range(start_date, end_date, freq="D").to_frame(name='Date')
datedf=spark.createDataFrame(datedf)
display(datedf)

# COMMAND ----------

joindf = (
    datedf.join(
        fiscalperiodDf.filter(fiscalperiodDf.RecordId.isNotNull()),
        (datedf.Date >= fiscalperiodDf.FiscalStartDate) 
        & (datedf.Date <= fiscalperiodDf.FiscalEndDate),
        "left"
    ))
display(joindf)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
datedimdf = joindf.select(
    "Date",
    F.date_format(F.col("Date"),"yyyyMMdd").cast("int").alias("DateId"),
    F.year(F.col("Date")).alias("Year"),
    F.month(F.col("Date")).alias("Month"),
    F.date_format(F.col("Date"),"MMM").cast("string").alias("MonthName"),
    F.dayofmonth(F.col("Date")).alias("Day"),
    F.date_format(F.col("Date"),"E").cast("string").alias("DayName"),
    F.quarter(F.col("Date")).alias("Quarter"),
    F.col("FiscalPeriodName").alias("FiscalPeriod"),
    "FiscalStartDate",
    "FiscalEndDate",
    "FiscalMonth",
    "FiscalYearStart",
    "FiscalYearEnd",
    "FiscalQuarter",
    "FiscalQuarterStart",
    "FiscalQuarterEnd",
    F.concat(F.lit("FY"),"FiscalYear").alias("FiscalYear"),
    F.lit(UpdateDateTime).alias("UpdateDateTime"),
    F.xxhash64("DateId").alias("DateKey")
)
display(datedimdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Final dataframe

# COMMAND ----------

df_final =  datedimdf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatlog(df_final,"Silver",Entity)