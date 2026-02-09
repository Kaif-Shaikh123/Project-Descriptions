# Databricks notebook source
# MAGIC %md
# MAGIC ## Run SharedLiblaries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLiblaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables 

# COMMAND ----------

Entity = "CostCenter"
EntityPath = f"Others/{Entity}/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read From Delta Raw Path

# COMMAND ----------

CostCenterdf = readFromDeltaPath(EntityPath)
display(CostCenterdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save to Bronze schema

# COMMAND ----------

# DBTITLE 1,Untitled
saveDeltaTableToCatlog(CostCenterdf,"bronze",Entity)