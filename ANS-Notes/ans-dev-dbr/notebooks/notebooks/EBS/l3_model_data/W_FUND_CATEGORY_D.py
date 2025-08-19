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
tableName = 'W_FUND_CATEGORY_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET
# Guy 2020/06/10 : no additional incremental logic needed
sourceFileUrl = sourceFolderUrl + 'apps.ams_categories_b.par'
AMS_CATEGORIES_B_INC = spark.read.parquet(sourceFileUrl)
AMS_CATEGORIES_B_INC.createOrReplaceTempView("AMS_CATEGORIES_B_INC")

# COMMAND ----------

W_FUND_CATEGORY_D = spark.sql("""
  SELECT
    AMS_CATEGORIES_B_INC.CATEGORY_ID INTEGRATION_ID,
    DATE_FORMAT(AMS_CATEGORIES_B_INC.CREATION_DATE,'yyyy-MM-dd HH:mm:ss.SSS') CREATED_ON_DT,
    DATE_FORMAT(AMS_CATEGORIES_B_INC.LAST_UPDATE_DATE,'yyyy-MM-dd HH:mm:ss.SSS') CHANGED_ON_DT,
    AMS_CATEGORIES_B_INC.CREATED_BY CREATED_BY_ID,
    AMS_CATEGORIES_B_INC.LAST_UPDATED_BY CHANGED_BY_ID, 
    DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,
    DATE_FORMAT(CURRENT_DATE,'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT,  
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(current_timestamp(),'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
    AMS_CATEGORIES_B_INC.PARENT_CATEGORY_ID PARENT_CATEGORY_ID,
    AMS_CATEGORIES_TL.CATEGORY_NAME CATEGORY_NAME, 
    AMS_CATEGORIES_TL.DESCRIPTION DESCRIPTION, 
    AMS_CATEGORIES_B_INC.ENABLED_FLAG ENABLED_FLAG,
    'N' DELETE_FLG
  FROM
    AMS_CATEGORIES_B_INC 
    INNER JOIN AMS_CATEGORIES_TL ON AMS_CATEGORIES_B_INC.category_id = AMS_CATEGORIES_TL.category_id 
    WHERE 
      AMS_CATEGORIES_B_INC.enabled_flag = 'Y' 
      AND AMS_CATEGORIES_TL.language = 'US'
""")

W_FUND_CATEGORY_D.createOrReplaceTempView("W_FUND_CATEGORY_D")
W_FUND_CATEGORY_D.cache()
W_FUND_CATEGORY_D.count()

# COMMAND ----------

count = W_FUND_CATEGORY_D.select("INTEGRATION_ID").count()
countDistinct = W_FUND_CATEGORY_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_FUND_CATEGORY_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

AMS_CATEGORIES_B_INC.unpersist()
W_FUND_CATEGORY_D.unpersist()
