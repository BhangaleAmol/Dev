# Databricks notebook source
import os
ENV_NAME = os.getenv('ENV_NAME')
DATALAKE_ENDPOINT = 'abfss://datalake@edmans{0}data001.dfs.core.windows.net'.format(ENV_NAME)

# COMMAND ----------

# import os

# envName = os.getenv('ENV_NAME')
# secretScope = 'edm-ans-{}-dbr-scope'.format(envName)
# configOption = 'fs.azure.account.key.edmans{}data001.dfs.core.windows.net'.format(envName)
# accessKey = dbutils.secrets.get(scope = secretScope, key = "ls-azu-datalake-key")
# spark.conf.set(configOption, accessKey)
# fileSystemUrl = "abfss://datalake@edmans{}data001.dfs.core.windows.net".format(envName)

# COMMAND ----------

envName = os.getenv('ENV_NAME')
secretScope = 'edm-ans-{}-dbr-scope'.format(envName)
clientId = dbutils.secrets.get(scope = secretScope, key = "svc-edm-dbr-id")
clientSecret = dbutils.secrets.get(scope = secretScope, key = "svc-edm-dbr-secret")

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "{0}".format(clientId),
  "fs.azure.account.oauth2.client.secret": "{0}".format(clientSecret),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/e49ea3fe-87f8-44df-a7ba-2131f4fb91f1/oauth2/token"
}

try:
  dbutils.fs.mount(
    source = "abfss://mdm@edmans{}data001.dfs.core.windows.net".format(envName),
    mount_point = "/mnt/mdm",
    extra_configs = configs
  )
except Exception as e:
  print("Already mounted")

# COMMAND ----------

from pyspark import StorageLevel

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS EBS")
spark.sql("USE EBS")
