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
tableName = 'W_XACT_SOURCE_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET
sourceFileUrl = sourceFolderUrl + 'apps.oe_order_sources.par'
OE_ORDER_SOURCES_INC = spark.read.parquet(sourceFileUrl)
OE_ORDER_SOURCES_INC.createOrReplaceTempView("OE_ORDER_SOURCES_INC")

# COMMAND ----------

W_XACT_SOURCE_D = spark.sql("""
  SELECT 
    CONCAT('SALES_ORDSRC-', COALESCE(INT(OE_ORDER_SOURCES_INC.ORDER_SOURCE_ID), '')) INTEGRATION_ID,
    DATE_FORMAT(OE_ORDER_SOURCES_INC.CREATION_DATE,'yyyy-MM-dd HH:mm:ss.SSS') CREATED_ON_DT,
    DATE_FORMAT(OE_ORDER_SOURCES_INC.LAST_UPDATE_DATE,'yyyy-MM-dd HH:mm:ss.SSS') CHANGED_ON_DT,
    OE_ORDER_SOURCES_INC.CREATED_BY CREATED_BY_ID,
    OE_ORDER_SOURCES_INC.LAST_UPDATED_BY CHANGED_BY_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT,
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP(),'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
    OE_ORDER_SOURCES_INC.NAME SOURCE_NAME,
    OE_ORDER_SOURCES_INC.DESCRIPTION SOURCE_DESC,
    'ORDER ENTRY' SOURCE_TYPE,
    OE_ORDER_SOURCES_INC.ENABLED_FLAG ACTIVE_FLG,
    'N' DELETE_FLG
  FROM OE_ORDER_SOURCES_INC
""")

W_XACT_SOURCE_D.createOrReplaceTempView("W_XACT_SOURCE_D")
W_XACT_SOURCE_D.cache()
W_XACT_SOURCE_D.count()

# COMMAND ----------

count = W_XACT_SOURCE_D.select("INTEGRATION_ID").count()
countDistinct = W_XACT_SOURCE_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_XACT_SOURCE_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

W_XACT_SOURCE_D.unpersist()
