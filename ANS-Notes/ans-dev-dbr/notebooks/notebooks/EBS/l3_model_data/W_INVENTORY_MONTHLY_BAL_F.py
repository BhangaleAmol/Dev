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
tableName = 'W_INVENTORY_MONTHLY_BAL_F' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET
sourceFileUrl = sourceFolderUrl + 'apps.mtl_onhand_quantities_detail.par'
MTL_ONHAND_QUANTITIES_DETAIL_INC = spark.read.parquet(sourceFileUrl)
MTL_ONHAND_QUANTITIES_DETAIL_INC.createOrReplaceTempView("MTL_ONHAND_QUANTITIES_DETAIL_INC")

# COMMAND ----------

W_INVENTORY_MONTHLY_BAL_F = spark.sql("""
  SELECT 
    CONCAT(DATE_FORMAT(COALESCE(LAST_DAY(DATE_SUB(CURRENT_DATE,1))), 'yyyyMMdd'), '-',
      COALESCE(INT(DF.INVENTORY_ITEM_ID), ''),'-',
      COALESCE(INT(DF.ORGANIZATION_ID), ''),'-',
      COALESCE(UPPER(DF.SUBINVENTORY_CODE), ''),'-',
      COALESCE(UPPER(DF.LOT_NUMBER), '')) INTEGRATION_ID,
    CAST(NULL AS DATE) CREATED_ON_DT,
    CAST(NULL AS DATE) CHANGED_ON_DT,
    CAST(NULL AS INT) CREATED_BY_ID,
    CAST(NULL AS INT) CHANGED_BY_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd hh:mm:ss.SSS') INSERT_DT,             
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd hh:mm:ss.SSS') UPDATE_DT,
    'EBS' DATASOURCE_NUM_ID,
    STRING(DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddhhmmssSSS')) LOAD_BATCH_ID,
    UPPER(DF.LOT_NUMBER) LOT_NUMBER,
    UPPER(DF.SUBINVENTORY_CODE) SUBINVENTORY_CODE,
    SUM(
      CASE 
        WHEN DF.IS_CONSIGNED = 2 THEN DF.PRIMARY_TRANSACTION_QUANTITY 
        ELSE 0 
      END
    ) AVAILABLE_QTY,
    INT(DF.INVENTORY_ITEM_ID) INVENTORY_ITEM_ID, 
    STRING(DATE_FORMAT(COALESCE(LAST_DAY(DATE_SUB(CURRENT_DATE, 1))), 'yyyyMMdd')) MONTH_END_WID,
    INT(DF.ORGANIZATION_ID) OPERATING_UNIT_ID,
    INT(DF.ORGANIZATION_ID) ORGANIZATION_ID
  FROM MTL_ONHAND_QUANTITIES_DETAIL_INC DF
  GROUP BY
    DF.INVENTORY_ITEM_ID, 
    DF.ORGANIZATION_ID, 
    UPPER(DF.SUBINVENTORY_CODE), 
    UPPER(DF.LOT_NUMBER)
""")

W_INVENTORY_MONTHLY_BAL_F.createOrReplaceTempView("W_INVENTORY_MONTHLY_BAL_F")
W_INVENTORY_MONTHLY_BAL_F.cache()
W_INVENTORY_MONTHLY_BAL_F.count()

# COMMAND ----------

count = W_INVENTORY_MONTHLY_BAL_F.select("INTEGRATION_ID").count()
countDistinct = W_INVENTORY_MONTHLY_BAL_F.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_INVENTORY_MONTHLY_BAL_F.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

MTL_ONHAND_QUANTITIES_DETAIL_INC.unpersist()
W_INVENTORY_MONTHLY_BAL_F.unpersist()
