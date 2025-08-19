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
tableName = 'W_USER_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET

sourceFileUrl = sourceFolderUrl + 'apps.fnd_user.par'
FND_USER_INC = spark.read.parquet(sourceFileUrl)
FND_USER_INC.createOrReplaceTempView("FND_USER_INC")


sourceFileUrl = sourceFolderUrl + 'apps.xx_per_all_people_f.par'
XX_PER_ALL_PEOPLE_F_INC = spark.read.parquet(sourceFileUrl)
XX_PER_ALL_PEOPLE_F_INC.createOrReplaceTempView("XX_PER_ALL_PEOPLE_F_INC")


USER_INCREMENT_STAGE_TABLE = spark.sql("""
SELECT DISTINCT USER_ID FROM FND_USER_INC
""")

USER_INCREMENT_STAGE_TABLE.createOrReplaceTempView("USER_INCREMENT_STAGE_TABLE")
USER_INCREMENT_STAGE_TABLE.cache()
USER_INCREMENT_STAGE_TABLE.count()    

EMPLOYEE_INCREMENT_STAGE_TABLE = spark.sql("""
SELECT DISTINCT PERSON_ID EMPLOYEE_ID FROM XX_PER_ALL_PEOPLE_F_INC
""")

EMPLOYEE_INCREMENT_STAGE_TABLE.createOrReplaceTempView("EMPLOYEE_INCREMENT_STAGE_TABLE")
EMPLOYEE_INCREMENT_STAGE_TABLE.cache()
EMPLOYEE_INCREMENT_STAGE_TABLE.count()


# COMMAND ----------

PER_ALL_PEOPLE_F_MAX = spark.sql("""
  SELECT PERSON_ID, MAX(EFFECTIVE_START_DATE) MAX_EFF_START_DATE FROM XX_PER_ALL_PEOPLE_F GROUP BY PERSON_ID
""")

PER_ALL_PEOPLE_F_MAX.createOrReplaceTempView("PER_ALL_PEOPLE_F_MAX")
PER_ALL_PEOPLE_F_MAX.cache()
PER_ALL_PEOPLE_F_MAX.count()

# COMMAND ----------

W_USER_D = spark.sql("""
  SELECT 
 STRING(ROUND(A.USER_ID,0))              INTEGRATION_ID,
 DATE_FORMAT(A.CREATION_DATE, 'yyyy-MM-dd HH:mm:ss.SSS')        CREATED_ON_DT,
 DATE_FORMAT(A.LAST_UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss.SSS')     CHANGED_ON_DT,
 A.CREATED_BY           CREATED_BY_ID, 
 A.LAST_UPDATED_BY      CHANGED_BY_ID, 
 DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')           INSERT_DT,
 DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')           UPDATE_DT,  
 'EBS'                  DATASOURCE_NUM_ID,
 date_format(current_timestamp(),'yyyyMMddHHmmssSSS') as LOAD_BATCH_ID,
 B.FIRST_NAME           FIRST_NAME,
 B.LAST_NAME            LAST_NAME,
 B.FULL_NAME            FULL_NAME,
 A.EMAIL_ADDRESS        PRIMARY_EMAIL_ADDRESS,
(CASE WHEN B.CURRENT_EMPLOYEE_FLAG = 'Y' THEN 'Y' ELSE 'N' END)   CURRENT_EMPLOYEE_FLAG,
 A.USER_NAME            LOGIN,
 'N'                    DELETE_FLG
 FROM
   FND_USER A 
   LEFT OUTER JOIN PER_ALL_PEOPLE_F_MAX C  ON A.EMPLOYEE_ID = C.PERSON_ID
   LEFT OUTER JOIN XX_PER_ALL_PEOPLE_F B
    ON C.PERSON_ID = B.PERSON_ID AND C.MAX_EFF_START_DATE = B.EFFECTIVE_START_DATE
   WHERE A.USER_ID IN (SELECT USER_ID FROM USER_INCREMENT_STAGE_TABLE)
     OR B.PERSON_ID IN (SELECT EMPLOYEE_ID FROM EMPLOYEE_INCREMENT_STAGE_TABLE)
""")

W_USER_D.cache()
W_USER_D.count()
W_USER_D.createOrReplaceTempView("W_USER_D")


# COMMAND ----------

count = W_USER_D.select("INTEGRATION_ID").count()
countDistinct = W_USER_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_USER_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

USER_INCREMENT_STAGE_TABLE.unpersist()
PER_ALL_PEOPLE_F_MAX.unpersist()
W_USER_D.unpersist()

# COMMAND ----------


