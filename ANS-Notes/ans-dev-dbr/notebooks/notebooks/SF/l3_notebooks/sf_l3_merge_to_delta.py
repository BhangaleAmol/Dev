# Databricks notebook source
dbutils.widgets.removeAll()

dbutils.widgets.text("source_folder", "","") 
sourceFolder = getArgument("source_folder")
sourceFolder = '/datalake/SALESFORCE/raw_data/delta_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("source_folder_pk", "","") 
sourceFolderPk = getArgument("source_folder_pk")
sourceFolderPk = '/datalake/SALESFORCE/raw_data/full_data_pk' if sourceFolderPk == '' else sourceFolderPk

dbutils.widgets.text("target_folder", "","") 
targetFolder = getArgument("target_folder")
targetFolder = '/datalake/SALESFORCE/raw_data/full_data' if targetFolder == '' else targetFolder

dbutils.widgets.text("table_name", "","") 
tableName = getArgument("table_name")

dbutils.widgets.text("keys", "","") 
keys = getArgument("keys")

dbutils.widgets.text("integration_key", "","") 
integrationKey = getArgument("integration_key")
integrationKey = (integrationKey.lower() == 'true')

dbutils.widgets.text("handle_delete", "","")
handleDelete = getArgument("handle_delete")
handleDelete = (handleDelete.lower() == 'true')

dbutils.widgets.text("incremental", "","") 
incremental = getArgument("incremental")
incremental = (incremental.lower() == 'true')

dbutils.widgets.text("partition_column", "","") 
partitionColumn = getArgument("partition_column")

# COMMAND ----------

# tableName = 'Contact'
# keys = 'Id'
# incremental = True
# partitionColumn = 'CreatedDate'

# COMMAND ----------

import os
ENV_NAME = os.getenv('ENV_NAME')
DATALAKE_ENDPOINT = 'abfss://datalake@edmans{0}data001.dfs.core.windows.net'.format(ENV_NAME)

sourceFileUrl = DATALAKE_ENDPOINT + sourceFolder + '/' + tableName + '.par'
sourceFilePkUrl = DATALAKE_ENDPOINT + sourceFolderPk + '/' + tableName + '.par'
targetFileUrl = DATALAKE_ENDPOINT + targetFolder + '/' + tableName + '.par'

print(sourceFileUrl)
print(sourceFilePkUrl)
print(targetFileUrl)

# COMMAND ----------

import pyspark.sql.functions as f

df = spark.read.format('parquet').load(sourceFileUrl)

if partitionColumn in df.columns:
  df = df.withColumn('_PART', f.date_format(df[partitionColumn], "yyyy-MM"))

df.createOrReplaceTempView('df')
display(df)

# COMMAND ----------

try:
  dbutils.fs.ls(targetFileUrl)
  fileExists = True
  print(targetFileUrl + ' exists')
except Exception as e:
  fileExists = False

if not fileExists:
  if "_PART" in df.columns:
    df.write.format("delta").partitionBy("_PART").save(targetFileUrl)
  else:
    df.write.format("delta").save(targetFileUrl)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS SF")
spark.sql("USE SF") 
spark.sql("CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}'".format(tableName, targetFileUrl))
spark.sql("OPTIMIZE {0}".format(tableName))

try:
  spark.sql("ALTER TABLE {0} ADD COLUMNS (DELETE_FLG STRING)".format(tableName))
  spark.sql("UPDATE {0} SET DELETE_FLG = 'N' WHERE DELETE_FLG IS NULL".format(tableName))
except Exception as e:
  print("Column already exists")

# COMMAND ----------

# INITIAL LOAD
if not fileExists:
  print("INITIAL LOAD")

# MERGE DATA 
elif (keys != ''):
  print("DEFAULT MERGE")  
  keysArray = [key.strip() for key in keys.split(',')]
  join = ' AND '.join(["{0}.{1} = df.{1}".format(tableName, key) for key in keysArray])
  query = "MERGE INTO {0} USING df ON {1} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *".format(tableName, join)
  print(query)
  spark.sql(query)

# COMMAND ----------

# INITIAL LOAD
if not fileExists:
  print("INITIAL LOAD")
  
# HANDLE_DELETE
elif (handleDelete == True) and (keys != ''):
  print("HANDLE DELETE")
  
  df = spark.read.format('parquet').load(sourceFilePkUrl)
  df.createOrReplaceTempView("df")
  
  keysArray = [key.strip() for key in keys.split(',')]
  where = ' AND '.join(["{0}.{1} = df.{1}".format(tableName, key) for key in keysArray])
  query = "UPDATE {0} SET DELETE_FLG = 'Y' WHERE NOT EXISTS (SELECT {1} FROM df WHERE {2})".format(tableName, keys, where)
  print(query)
  spark.sql(query)

# COMMAND ----------


