# Databricks notebook source
dbutils.widgets.removeAll()

dbutils.widgets.text("source_folder", "","")
sourceFolder = getArgument("source_folder")
sourceFolder = '/datalake/OBIEE/raw_data/delta_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("target_folder", "","")
targetFolder = getArgument("target_folder")
targetFolder = '/datalake/OBIEE/raw_data/full_data' if targetFolder == '' else targetFolder

dbutils.widgets.text("schema_name", "","")
schemaName = getArgument("schema_name")

dbutils.widgets.text("table_name", "","")
tableName = getArgument("table_name")

dbutils.widgets.text("keys", "","")
keys = getArgument("keys")

dbutils.widgets.text("incremental", "","")
incremental = getArgument("incremental")
incremental = (incremental.lower() == 'true')

dbutils.widgets.text("partition_column", "","")
partitionColumn = getArgument("partition_column")

dbutils.widgets.text("timestamp_column", "","")
timestampColumn = getArgument("timestamp_column")

tableName = schemaName + '_' + tableName

# COMMAND ----------

sourceFileUrl = '/mnt/datalake' + sourceFolder + '/' + tableName + '.par'
targetFileUrl = '/mnt/datalake' + targetFolder + '/' + tableName + '.par'
print(sourceFileUrl)
print(targetFileUrl)

# COMMAND ----------

import pyspark.sql.functions as f

df = spark.read.format('parquet').load(sourceFileUrl)
if partitionColumn in df.columns:
  df = df.withColumn('_PART', f.date_format(df[partitionColumn], "yyyy-MM"))
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

spark.sql("CREATE DATABASE IF NOT EXISTS OBIEE")
spark.sql("USE OBIEE")
spark.sql("CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}'".format(tableName, targetFileUrl))
spark.sql("ALTER TABLE {0} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(tableName))
spark.sql("ANALYZE TABLE {0} COMPUTE STATISTICS NOSCAN".format(tableName))
spark.sql("OPTIMIZE {0}".format(tableName))

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

# FULL OVERWRITE
elif (incremental == False):
  print("FULL OVERWRITE")
  
  if "_PART" in df.columns:
    df.write.format("delta").partitionBy("_PART").mode("overwrite").save(targetFileUrl)
  else:
    df.write.format("delta").mode("overwrite").save(targetFileUrl)

else:
  raise Exception("Merge was not possible")
