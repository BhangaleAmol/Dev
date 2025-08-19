# Databricks notebook source
# MAGIC %run ../SHARED/bootstrap

# COMMAND ----------

dbutils.widgets.text("source_folder", "","")
sourceFolder = getArgument("source_folder")
sourceFolder = '/datalake/EBS/raw_data/delta_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("source_folder_pk", "","")
sourceFolderPk = getArgument("source_folder_pk")
sourceFolderPk = '/datalake/EBS/raw_data/full_data_pk' if sourceFolderPk == '' else sourceFolderPk

dbutils.widgets.text("target_folder", "","")
targetFolder = getArgument("target_folder")
targetFolder = '/datalake/EBS/raw_data/full_data' if targetFolder == '' else targetFolder

dbutils.widgets.text("table_name", "","")
tableName = getArgument("table_name")

dbutils.widgets.text("keys", "","")
keys = getArgument("keys")

dbutils.widgets.text("incremental", "","")
incremental = getArgument("incremental")
incremental = (incremental.lower() == 'true')

dbutils.widgets.text("partition_column", "","")
partitionColumn = getArgument("partition_column")

dbutils.widgets.text("handle_delete", "","")
handleDelete = getArgument("handle_delete")
handleDelete = (handleDelete.lower() == 'true')

dbutils.widgets.text("append_only", "","")
appendOnly = getArgument("append_only")
appendOnly = (appendOnly.lower() == 'true')

dbutils.widgets.text("timestamp_column", "","")
timestampColumn = getArgument("timestamp_column")

# COMMAND ----------

sourceFileUrl = DATALAKE_ENDPOINT + sourceFolder + '/' + tableName + '.par'
sourceFilePkUrl = DATALAKE_ENDPOINT + sourceFolderPk + '/' + tableName + '.par'
targetFileUrl = DATALAKE_ENDPOINT + targetFolder + '/' + tableName + '.par'
print(sourceFileUrl)
print(targetFileUrl)

# COMMAND ----------

import pyspark.sql.functions as f

df = spark.read.format('parquet').load(sourceFileUrl)
if partitionColumn in df.columns:
  df = df.withColumn('_PART', f.date_format(df[partitionColumn], "yyyy-MM"))

df = df.withColumn('_MODIFIED', f.current_date())
df = df.withColumn('_DELETED', f.lit(False))
df.createOrReplaceTempView('df')

# COMMAND ----------

try:
  dbutils.fs.ls(targetFileUrl)
  fileExists = True
except Exception as e:
  fileExists = False

if not fileExists:
  print("Creating new file")
  if "_PART" in df.columns:
    df.write.format("delta").partitionBy("_PART").save(targetFileUrl)
  else:
    df.write.format("delta").save(targetFileUrl)

# COMMAND ----------

spark.sql("set spark.databricks.delta.optimizeWrite.enabled = true")
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS EBS")
spark.sql("USE EBS")
spark.sql("CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}'".format(tableName, targetFileUrl))
spark.sql("ANALYZE TABLE {0} COMPUTE STATISTICS NOSCAN".format(tableName))
# spark.sql("OPTIMIZE {0}".format(tableName))

# COMMAND ----------

# INITIAL LOAD
if not fileExists:
  print("INITIAL LOAD")
  
# DEFAULT MERGE
elif (keys != ''):
  print("DEFAULT MERGE")
  
  keysArray = [key.strip() for key in keys.split(',')]
  join = ' AND '.join(["{0}.{1} = df.{1}".format(tableName, key) for key in keysArray])  
  query = "MERGE INTO {0} USING df ON {1} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *".format(tableName, join)
  print(query)
  spark.sql(query)

# APPEND ONLY
elif (appendOnly == True) and (timestampColumn != ''):
  print("APPEND ONLY")
  
  query = "DELETE FROM {0} WHERE {1} >= (SELECT MIN({1}) FROM df)".format(tableName, timestampColumn)
  print(query)  
  spark.sql(query)
  
  query = "INSERT INTO {0} SELECT * FROM df".format(tableName)
  print(query)
  spark.sql(query)  

# FULL OVERWRITE
elif (incremental == False):
  print("FULL OVERWRITE")
  
  if "_PART" in df.columns:
    df.write.format("delta").partitionBy("_PART").mode("overwrite").save(targetFileUrl)
  else:
    df.write.format("delta").mode("overwrite").save(targetFileUrl)

else:
  raise Exception("Merge was not possible")

# COMMAND ----------

# INITIAL LOAD
if not fileExists:
  print("INITIAL LOAD")

# HANDLE DELETE
elif (handleDelete == True) and (keys != ''):
  print("HANDLE DELETE")
  
  df = spark.read.format('parquet').load(sourceFilePkUrl)
  df.createOrReplaceTempView("df")
  
  keysArray = [key.strip() for key in keys.split(',')]
  where = ' AND '.join(["delta.{0} = df.{0}".format(key) for key in keysArray])
  query = "DELETE FROM {0} delta WHERE NOT EXISTS (SELECT {1} FROM df WHERE {2})".format(tableName, keys, where)
  print(query)
  spark.sql(query)
