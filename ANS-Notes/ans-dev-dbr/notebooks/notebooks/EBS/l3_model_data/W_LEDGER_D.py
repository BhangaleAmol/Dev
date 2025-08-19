# Databricks notebook source
# MAGIC %run ../SHARED/bootstrap

# COMMAND ----------

# Get parameters
dbutils.widgets.text("source_folder", "","")
sourceFolder = getArgument("source_folder")
sourceFolder = '/datalake/EBS/raw_data/delta_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("target_folder", "","")
targetFolder = getArgument("target_folder")
targetFolder = '/datalake/EBS/stage_data' if targetFolder == '' else targetFolder

dbutils.widgets.text("table_name", "","")
tableName = getArgument("table_name")
tableName = 'W_LEDGER_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET
sourceFileUrl = sourceFolderUrl + 'apps.gl_ledgers.par'
GL_LEDGERS_INC = spark.read.parquet(sourceFileUrl)
GL_LEDGERS_INC.createOrReplaceTempView("GL_LEDGERS_INC")

# COMMAND ----------

W_LEDGER_D = spark.sql("""
  SELECT
    INT(GL_LEDGERS_INC.LEDGER_ID) INTEGRATION_ID,
	GL_LEDGERS_INC.CREATION_DATE CREATED_ON_DT,
	GL_LEDGERS_INC.LAST_UPDATE_DATE CHANGED_ON_DT,
	GL_LEDGERS_INC.CREATED_BY CREATED_BY_ID,
	GL_LEDGERS_INC.LAST_UPDATED_BY CHANGED_BY_ID,
	DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT,
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
    GL_LEDGERS_INC.NAME LEDGER_NAME,
    GL_LEDGERS_INC.SHORT_NAME LEDGER_SHORT_NAME,
    GL_LEDGERS_INC.DESCRIPTION LEDGER_DESC,
	GL_LEDGERS_INC.CHART_OF_ACCOUNTS_ID CHART_OF_ACCOUNTS,
	GL_LEDGERS_INC.CURRENCY_CODE CURRENCY_CODE,
	GL_LEDGERS_INC.PERIOD_SET_NAME CALENDER_NAME,
	GL_LEDGERS_INC.SLA_ACCOUNTING_METHOD_CODE SLA_ACCOUNTING_METHOD_CODE,
	GL_PERIOD_TYPES.USER_PERIOD_TYPE PERIOD_TYPE,
    GL_LEDGERS_INC.LEDGER_CATEGORY_CODE LEDGER_CATEGORY_CODE
  FROM
    GL_LEDGERS_INC,
    GL_PERIOD_TYPES
  WHERE
    GL_LEDGERS_INC.OBJECT_TYPE_CODE = 'L'
    AND NVL(GL_LEDGERS_INC.COMPLETE_FLAG,'Y') = 'Y'
    AND GL_LEDGERS_INC.ACCOUNTED_PERIOD_TYPE = GL_PERIOD_TYPES.PERIOD_TYPE
""")

W_LEDGER_D.createOrReplaceTempView("W_LEDGER_D")
W_LEDGER_D.cache()
W_LEDGER_D.count()

# COMMAND ----------

count = W_LEDGER_D.select("INTEGRATION_ID").count()
countDistinct = W_LEDGER_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_LEDGER_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

GL_LEDGERS_INC.unpersist()
W_LEDGER_D.unpersist()
