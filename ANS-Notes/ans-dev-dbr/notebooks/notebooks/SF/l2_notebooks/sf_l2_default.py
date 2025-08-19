# Databricks notebook source
dbutils.widgets.removeAll()

dbutils.widgets.text("source_folder", "","") 
sourceFolder = getArgument("source_folder")
sourceFolder = '/datalake/SALESFORCE/raw_data/delta_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("target_folder", "","") 
targetFolder = getArgument("target_folder")
targetFolder = '/datalake/SALESFORCE/stage_data' if targetFolder == '' else targetFolder

dbutils.widgets.text("table_name", "","") 
tableName = getArgument("table_name")

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
targetFileUrl = DATALAKE_ENDPOINT + targetFolder + '/' + tableName + '.par'

print(sourceFileUrl)
print(targetFileUrl)

# COMMAND ----------

import pyspark.sql.functions as f
df = spark.read.format('parquet').load(sourceFileUrl)
df = df.withColumn('DELETE_FLG', f.lit("N"))
display(df)

# COMMAND ----------

df.write.format("parquet").mode("overwrite").save(targetFileUrl)
