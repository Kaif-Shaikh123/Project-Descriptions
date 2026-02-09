# Databricks notebook source
# MAGIC %md
# MAGIC ##Run Shared Libraries

# COMMAND ----------

# MAGIC %run /Workspace/Users/azureserviced@gmail.com/modernDW_databricks/Misc/SharedLiblaries

# COMMAND ----------

# DBTITLE 1,Cell 3
UpdateDateTime = datetime.datetime.now()
Entity = "dimvertical"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze table

# COMMAND ----------

workerdf= spark.table("90110adbdev_7405605216081259.bronze.workertable")
display(workerdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS 90110adbdev_7405605216081259.silver.dimvertical(
# MAGIC   VerticalId BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   Vertical STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Cell 7
df = workerdf.select(F.expr("trim(Vertical) AS Vertical")).distinct()
display(df)

# COMMAND ----------

# DBTITLE 1,Untitled
verticaldf = spark.table("90110adbdev_7405605216081259.silver.dimvertical")
display(verticaldf)

# COMMAND ----------

# DBTITLE 1,Cell 9
newrowsdf = df.filter(F.col("Vertical").isNotNull()).exceptAll(verticaldf.select("vertical"))
display(newrowsdf)

# COMMAND ----------

# DBTITLE 1,Cell 11
spark.sql("insert into 90110adbdev_7405605216081259.silver.dimvertical(vertical) select vertical from {newrowsdf}", newrowsdf=newrowsdf)

# COMMAND ----------

# DBTITLE 1,Cell 12
display(spark.table("90110adbdev_7405605216081259.silver.dimvertical"))

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into 90110adbdev_7405605216081259.bronze.workertable(vertical)values("Date & AI")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM 90110adbdev_7405605216081259.bronze.workertable WHERE Vertical = "Date & AI"