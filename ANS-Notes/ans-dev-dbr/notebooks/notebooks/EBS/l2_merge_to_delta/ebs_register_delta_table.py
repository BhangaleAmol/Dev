# Databricks notebook source
# MAGIC %run ../SHARED/bootstrap

# COMMAND ----------

import json

# COMMAND ----------

dbutils.widgets.text("source_folder", "","") 
sourceFolder = getArgument("source_folder")
sourceFolder = '/datalake/EBS/raw_data/full_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("item", "","") 
item = getArgument("item")
inputData = json.loads(item)

# COMMAND ----------

tableName = inputData["TABLE_NAME"].strip()
print(inputData)

# COMMAND ----------

sourceFileUrl = fileSystemUrl + sourceFolder + '/' + tableName + '.par'
print(sourceFileUrl)

# COMMAND ----------

spark.sql("USE EBS")
 
tableExists = spark.sql("SHOW TABLES LIKE '{0}'".format(tableName)).count() == 1 
if tableExists:
  print("Table {0} already registered".format(tableName))
else:
  dbutils.fs.ls(sourceFileUrl)
  spark.sql("CREATE TABLE {0} USING DELTA LOCATION '{1}'".format(tableName, sourceFileUrl))   
