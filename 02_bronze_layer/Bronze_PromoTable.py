# Databricks notebook source
# MAGIC %md
# MAGIC ## Run SharedLiblaries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables 

# COMMAND ----------

Entity = "PromoTable"
EntityPath = f"Sales/{Entity}/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read From Delta Raw Path

# COMMAND ----------

PromoTabledf = readFromDeltaPath(EntityPath)
display(PromoTabledf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save to Bronze schema

# COMMAND ----------

# DBTITLE 1,Untitled
saveDeltaTableToCatlog(PromoTabledf,"bronze",Entity)