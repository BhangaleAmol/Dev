# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("source_folder", "","")
sourceFolder = getArgument("source_folder")
sourceFolder = '/datalake/SALESFORCE/raw_data/delta_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("target_folder", "","") 
targetFolder = getArgument("target_folder")
targetFolder = '/datalake/SALESFORCE/stage_data' if targetFolder == '' else targetFolder



dbutils.widgets.text("archive", "","") 
archive = getArgument("archive")

tableName = 'CurrencyType'

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

CURRENCY_TYPE_DF = spark.read.format('parquet').load(sourceFileUrl)
CURRENCY_TYPE_DF.createOrReplaceTempView("CURRENCY_TYPE")
display(CURRENCY_TYPE_DF)

# COMMAND ----------

CURRENCY_TYPE_DF2 = spark.sql("""
  WITH CURRENCY_TYPE AS (
    SELECT 
      *,
      CAST(CONCAT(IF(MONTH(CURRENT_DATE()) < 7, YEAR(CURRENT_DATE()) - 1, YEAR(CURRENT_DATE())), '-07', '-01') AS DATE) AS EFFECTIVE_START_DATE, 
      CAST(CONCAT(IF(MONTH(CURRENT_DATE()) < 7, YEAR(CURRENT_DATE()), YEAR(CURRENT_DATE()) + 1), '-06', '-30') AS DATE) AS EFFECTIVE_END_DATE,
      "N" AS DELETE_FLG
    FROM CURRENCY_TYPE
  )
  SELECT
    CONCAT(Id,'_', REPLACE(EFFECTIVE_START_DATE, '-', '_')) AS INTEGRATION_ID,
    *
  FROM CURRENCY_TYPE
""")
display(CURRENCY_TYPE_DF2)

# COMMAND ----------

CURRENCY_TYPE_DF2.write.format("parquet").mode("overwrite").save(targetFileUrl)
