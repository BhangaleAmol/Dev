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
tableName = 'W_EXCH_RATE_G' if tableName == '' else tableName

# COMMAND ----------

# Create paths

sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET
# Guy 2020/06/10 : no additional incremental logic needed
sourceFileUrl = sourceFolderUrl + 'gl.gl_daily_rates.par'
GL_DAILY_RATES_INC = spark.read.parquet(sourceFileUrl)
GL_DAILY_RATES_INC.createOrReplaceTempView("GL_DAILY_RATES_INC")

# COMMAND ----------

W_EXCH_RATE_G = spark.sql("""
  SELECT 
    CONCAT(GL_DAILY_RATES_INC.FROM_CURRENCY, '-', 
    COALESCE(GL_DAILY_RATES_INC.TO_CURRENCY,''), '-', 
    COALESCE(GL_DAILY_RATES_INC.CONVERSION_TYPE,''),'-', 
    DATE_FORMAT(GL_DAILY_RATES_INC.CONVERSION_DATE,"yyyyMMdd")) INTEGRATION_ID,
    DATE_FORMAT(GL_DAILY_RATES_INC.CREATION_DATE, 'yyyy-MM-dd HH:mm:ss.SSS' ) CREATED_ON_DT,
    DATE_FORMAT(GL_DAILY_RATES_INC.LAST_UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss.SSS') CHANGED_ON_DT,
    GL_DAILY_RATES_INC.CREATED_BY CREATED_BY_ID,
    GL_DAILY_RATES_INC.LAST_UPDATED_BY CHANGED_BY_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT,
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(current_timestamp(),'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
    GL_DAILY_RATES_INC.FROM_CURRENCY FROM_CURCY_CD,
    GL_DAILY_RATES_INC.TO_CURRENCY TO_CURCY_CD,
    GL_DAILY_RATES_INC.CONVERSION_TYPE RATE_TYPE,
    'Y' ACTIVE_FLG,
    'N' DELETE_FLG,
    DATE_FORMAT(GL_DAILY_RATES_INC.CONVERSION_DATE,"yyyy-MM-dd") START_DT,
    DATE_ADD(GL_DAILY_RATES_INC.CONVERSION_DATE,1) END_DT,
    GL_DAILY_RATES_INC.CONVERSION_RATE EXCH_RATE,
    GL_DAILY_RATES_INC.CONVERSION_DATE EXCH_DT
  FROM 
    GL_DAILY_RATES_INC
""")

W_EXCH_RATE_G.createOrReplaceTempView("W_EXCH_RATE_G")
W_EXCH_RATE_G.cache()
W_EXCH_RATE_G.count()

# COMMAND ----------

count = W_EXCH_RATE_G.select("INTEGRATION_ID").count()
countDistinct = W_EXCH_RATE_G.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_EXCH_RATE_G.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

GL_DAILY_RATES_INC.unpersist()
W_EXCH_RATE_G.unpersist()
