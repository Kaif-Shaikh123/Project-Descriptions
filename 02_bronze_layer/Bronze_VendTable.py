# Databricks notebook source
# MAGIC %md
# MAGIC ## Run SharedLiblaries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Variables 

# COMMAND ----------

Entity = "VendTable"
EntityPath = f"Purchase/{Entity}/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read From Delta Raw Path

# COMMAND ----------

VendTabledf = readFromDeltaPath(EntityPath)
display(VendTabledf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save to Bronze schema

# COMMAND ----------

# DBTITLE 1,Untitled
saveDeltaTableToCatlog(VendTabledf,"bronze",Entity)