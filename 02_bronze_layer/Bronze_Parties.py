# Databricks notebook source
# MAGIC %md
# MAGIC ## Run SharedLiblaries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables 

# COMMAND ----------

Entity = "Parties"
EntityPath = f"Purchase/{Entity}/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read From Delta Raw Path

# COMMAND ----------

partiesdf = readFromDeltaPath(EntityPath)
display(partiesdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save to Bronze schema

# COMMAND ----------

# DBTITLE 1,Untitled
saveDeltaTableToCatlog(partiesdf,"bronze",Entity)