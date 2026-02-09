# Databricks notebook source
# MAGIC %md
# MAGIC ## Run SharedLiblaries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Entity

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType
)

Currency_schema = StructType([
    StructField("CurrencyId", IntegerType(), True),
    StructField("Code", StringType(), True),
    StructField("LastProcessedChange_DateTime", TimestampType(), True),
    StructField("DataLakeModified_DateTime", TimestampType(), True),
    StructField("Country", StringType(), True),
    StructField("CurrencyName", StringType(), True),
    StructField("RecordId", StringType(), True)
    
])

df_Currency= (
    spark.read
        .option("header", "False")
        .option("delimiter", ",")
        .schema(Currency_schema)
    .csv("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Others/Currency/")
)

display(df_Currency)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Variables

# COMMAND ----------

entity = "Currency"
deltaLakePath ="DeltaLake/Raw/Others/"+ entity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To DeltaLake

# COMMAND ----------

writeRawToDeltaLake(df_Currency,deltaLakePath)