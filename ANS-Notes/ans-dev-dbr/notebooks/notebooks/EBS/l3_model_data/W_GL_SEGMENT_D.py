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
tableName = 'W_GL_SEGMENT_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET

# guy 2020/06/10 : need to include incremental dataset based on unique FND_FLEX_VALUES.FLEX_VALUE and FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID from delta file 


sourceFileUrl = sourceFolderUrl + 'apps.fnd_flex_values.par'
FND_FLEX_VALUES_INC = spark.read.parquet(sourceFileUrl)
FND_FLEX_VALUES_INC.createOrReplaceTempView("FND_FLEX_VALUES_INC")

sourceFileUrl = sourceFolderUrl + 'apps.fnd_flex_value_sets.par'
FND_FLEX_VALUE_SETS_INC = spark.read.parquet(sourceFileUrl)
FND_FLEX_VALUE_SETS_INC.createOrReplaceTempView("FND_FLEX_VALUE_SETS_INC")

INCREMENT_STAGE_TABLE = spark.sql("""
 SELECT  FLEX_VALUE_SET_ID FROM FND_FLEX_VALUES_INC 
 UNION
 SELECT  FLEX_VALUE_SET_ID FROM FND_FLEX_VALUE_SETS_INC 
""")

INCREMENT_STAGE_TABLE.createOrReplaceTempView("INCREMENT_STAGE_TABLE")
INCREMENT_STAGE_TABLE.cache()
INCREMENT_STAGE_TABLE.count()



# COMMAND ----------

W_GL_SEGMENT_D = spark.sql("""
  SELECT
    UPPER(CONCAT(INT(COALESCE(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID, '')), '-' , STRING(COALESCE(FND_FLEX_VALUES.FLEX_VALUE, '')) )) INTEGRATION_ID,
    CASE 
      WHEN DATE_FORMAT(MAX(FND_FLEX_VALUES.CREATION_DATE), 'yyyy') < '1900' 
      THEN TO_DATE(CONCAT('20', DATE_FORMAT(MAX(FND_FLEX_VALUES.CREATION_DATE), 'yy'), DATE_FORMAT(MAX(FND_FLEX_VALUES.CREATION_DATE), 'MMdd'), 'yyyyMMdd'))
      ELSE DATE_FORMAT(MAX(FND_FLEX_VALUES.CREATION_DATE), 'yyyy-MM-dd')
    END CREATED_ON_DT,    
    CASE 
      WHEN DATE_FORMAT(MAX(FND_FLEX_VALUES.LAST_UPDATE_DATE), 'yyyy') < '1900'
      THEN TO_DATE(CONCAT('20', DATE_FORMAT(MAX(FND_FLEX_VALUES.LAST_UPDATE_DATE), 'yy'), DATE_FORMAT(MAX(FND_FLEX_VALUES.LAST_UPDATE_DATE), 'MMdd'), 'yyyyMMdd'))
      ELSE DATE_FORMAT(MAX(FND_FLEX_VALUES.LAST_UPDATE_DATE), 'yyyy-MM-dd')
    END CHANGED_ON_DT,
    INT(MAX(FND_FLEX_VALUES.CREATED_BY)) CREATED_BY_ID,
    INT(MAX(FND_FLEX_VALUES.LAST_UPDATED_BY)) CHANGED_BY_ID,
    INT(UPPER(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID)) SEGMENT_LOV_ID,          
    UPPER(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_NAME) SEGMENT_LOV_NAME,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT,
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
    UPPER(FND_FLEX_VALUES.FLEX_VALUE) SEGMENT_VAL_CODE,
    MAX(UPPER(FND_FLEX_VALUES_TL.DESCRIPTION)) SEGMENT_VAL_DESC,
    'Y' ACTIVE_FLG,
    'N' DELETE_FLG
  FROM
 INCREMENT_STAGE_TABLE
 LEFT OUTER JOIN FND_FLEX_VALUES ON INCREMENT_STAGE_TABLE.FLEX_VALUE_SET_ID  =  FND_FLEX_VALUES.FLEX_VALUE_SET_ID
 INNER JOIN FND_FLEX_VALUE_SETS ON FND_FLEX_VALUES.FLEX_VALUE_SET_ID = FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID
 INNER JOIN FND_FLEX_VALUES_TL ON   FND_FLEX_VALUES.FLEX_VALUE_ID = FND_FLEX_VALUES_TL.FLEX_VALUE_ID
    WHERE FND_FLEX_VALUES_TL.LANGUAGE = 'US'    
  GROUP BY
    UPPER(CONCAT(INT(COALESCE(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID, '')), '-' , STRING(COALESCE(FND_FLEX_VALUES.FLEX_VALUE, '')))),
    UPPER(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID),
    UPPER(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_NAME),
    UPPER(FND_FLEX_VALUES.FLEX_VALUE)
""")

W_GL_SEGMENT_D.createOrReplaceTempView("W_GL_SEGMENT_D")
W_GL_SEGMENT_D.cache()
W_GL_SEGMENT_D.count()

# COMMAND ----------

count = W_GL_SEGMENT_D.select("INTEGRATION_ID").count()
countDistinct = W_GL_SEGMENT_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_GL_SEGMENT_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

FND_FLEX_VALUES_INC.unpersist()
FND_FLEX_VALUE_SETS_INC.unpersist()
INCREMENT_STAGE_TABLE.unpersist()
W_GL_SEGMENT_D.unpersist()
