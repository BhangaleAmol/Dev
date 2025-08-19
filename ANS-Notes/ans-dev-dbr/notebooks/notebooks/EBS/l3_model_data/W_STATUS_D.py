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
tableName = 'W_STATUS_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
sourceFolderCsvUrl = DATALAKE_ENDPOINT + '/datalake/EBS/raw_data/domain_values/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'


print(sourceFolderUrl)
print(sourceFolderCsvUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET
sourceFileUrl = sourceFolderUrl + 'apps.fnd_lookup_values.par'
FND_LOOKUP_VALUES_INC = spark.read.parquet(sourceFileUrl)
FND_LOOKUP_VALUES_INC.createOrReplaceTempView("FND_LOOKUP_VALUES_INC")

# CSV
sourceFileUrl = sourceFolderCsvUrl + 'domainValues_OrderOverallStatus_ora11i.csv'
DOMAIN_VALUE_STATUS = spark.read.option("header", "true").csv(sourceFileUrl)
DOMAIN_VALUE_STATUS.createOrReplaceTempView("DOMAIN_VALUE_STATUS")

# COMMAND ----------

W_STATUS_D = spark.sql("""
  SELECT
    CONCAT('SALES_ORDER_PROCESS', '-', COALESCE(FND_LOOKUP_VALUES_INC.LOOKUP_CODE, '')) INTEGRATION_ID,
    DATE_FORMAT(FND_LOOKUP_VALUES_INC.CREATION_DATE, 'yyyy-MM-dd HH:mm:ss.SSS') CREATED_ON_DT,
    DATE_FORMAT(FND_LOOKUP_VALUES_INC.LAST_UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss.SSS') CHANGED_ON_DT,
    FND_LOOKUP_VALUES_INC.CREATED_BY CREATED_BY_ID,
    FND_LOOKUP_VALUES_INC.LAST_UPDATED_BY CHANGED_BY_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT,
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
    'SALES_ORDER_PROCESS' W_STATUS_CLASS,
    FND_LOOKUP_VALUES_INC.LOOKUP_CODE STATUS_CODE,
    FND_LOOKUP_VALUES_INC.MEANING STATUS_NAME,
    FND_LOOKUP_VALUES_INC.DESCRIPTION STATUS_DESC,
    DOMAIN_VALUE_STATUS.W_STATUS_CODE W_STATUS_CODE,
    DOMAIN_VALUE_STATUS.W_STATUS_DESC W_STATUS_DESC,
    FND_LOOKUP_VALUES_INC.ENABLED_FLAG ACTIVE_FLG,
    'N' DELETE_FLG,
    NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'Y') INCLUDE_IN_ACCOUNT_ALLOCATIONS
  FROM
    FND_LOOKUP_VALUES_INC 
    LEFT JOIN DOMAIN_VALUE_STATUS 
      ON FND_LOOKUP_VALUES_INC.LOOKUP_CODE = DOMAIN_VALUE_STATUS.STATUS_CODE    
    LEFT JOIN (SELECT 
                    LOOKUP_CODE,
                    'N' INCLUDE_FLAG
                FROM FND_LOOKUP_VALUES
       WHERE     LOOKUP_TYPE = 'XX_ITEMDEMAND_LINESTATUS'
         AND ENABLED_FLAG = 'Y'
         AND LANGUAGE = 'US') ALLOCATION_FLAG
         ON  FND_LOOKUP_VALUES_INC.LOOKUP_CODE = ALLOCATION_FLAG.LOOKUP_CODE
  WHERE
    FND_LOOKUP_VALUES_INC.LOOKUP_TYPE = 'LINE_FLOW_STATUS'
    AND FND_LOOKUP_VALUES_INC.language = 'US'
    AND FND_LOOKUP_VALUES_INC.VIEW_APPLICATION_ID = 660
    AND FND_LOOKUP_VALUES_INC.SECURITY_GROUP_ID = 0
""")

W_STATUS_D.createOrReplaceTempView("W_STATUS_D")
W_STATUS_D.cache()
W_STATUS_D.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC     FND_LOOKUP_VALUES_INC 
# MAGIC     LEFT JOIN DOMAIN_VALUE_STATUS 
# MAGIC       ON FND_LOOKUP_VALUES_INC.LOOKUP_CODE = DOMAIN_VALUE_STATUS.STATUS_CODE  
# MAGIC  where FND_LOOKUP_VALUES_INC.LOOKUP_TYPE = 'LINE_FLOW_STATUS'
# MAGIC     AND FND_LOOKUP_VALUES_INC.language = 'US'
# MAGIC     AND FND_LOOKUP_VALUES_INC.VIEW_APPLICATION_ID = 660
# MAGIC     AND FND_LOOKUP_VALUES_INC.SECURITY_GROUP_ID = 0
# MAGIC     order by 3

# COMMAND ----------

#%sql
#SELECT 
#                   count( *)
#                FROM ebs.FND_LOOKUP_VALUES

# COMMAND ----------

#%sql
#select * from w_status_d
#where integration_id = 'SALES_ORDER_PROCESS-CANCELLED'
#order by 1

# COMMAND ----------

#%sql
#select * from w_status_d
#order by 1

# COMMAND ----------

count = W_STATUS_D.select("INTEGRATION_ID").count()
countDistinct = W_STATUS_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_STATUS_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

FND_LOOKUP_VALUES_INC.unpersist()
W_STATUS_D.unpersist()
