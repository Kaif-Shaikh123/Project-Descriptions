# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

service_credential = dbutils.secrets.get(scope = "adbdevscope", key = "Client--Secret")
app_id = dbutils.secrets.get(scope = "adbdevscope", key = "app--id")
tenantid = dbutils.secrets.get(scope = "adbdevscope", key = "Tenant--ID")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope = "adbdevscope", key = "Client--Secret")
app_id = dbutils.secrets.get(scope = "adbdevscope", key = "app--id")
tenantid = dbutils.secrets.get(scope = "adbdevscope", key = "Tenant--ID")

spark.conf.set("fs.azure.account.auth.type.90110adlsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.90110adlsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.90110adlsdev.dfs.core.windows.net", app_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.90110adlsdev.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.90110adlsdev.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://oaonoperationsdev@90110adlsdev.dfs.core.windows.net"))