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
tableName = 'W_CUSTOMER_ACCOUNT_D' if tableName == '' else tableName

dbutils.widgets.text("gold_folder", "","") 
goldFolder = getArgument("target_folder")
goldFolder = '/datalake/EBS/gold_data' if targetFolder == '' else targetFolder



# COMMAND ----------

# Create paths

sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'
goldFileurl = DATALAKE_ENDPOINT + goldFolder  + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)
print(goldFileurl)

# COMMAND ----------

# INCREMENTAL DATASET

# guy 2020/06/10 : need to include incremental dataset based on unique HZ_CUST_ACCOUNTS.PARTY_ID and HZ_PARTIES.PARTY_ID from delta file 

sourceFileUrl = sourceFolderUrl + 'apps.hz_cust_accounts.par'
HZ_CUST_ACCOUNTS_INC = spark.read.parquet(sourceFileUrl)
HZ_CUST_ACCOUNTS_INC.createOrReplaceTempView("HZ_CUST_ACCOUNTS_INC")

sourceFileUrl = sourceFolderUrl + 'apps.hz_parties.par'
HZ_PARTIES_INC = spark.read.parquet(sourceFileUrl)
HZ_PARTIES_INC.createOrReplaceTempView("HZ_PARTIES_INC")


INCREMENT_STAGE_TABLE = spark.sql("""
 SELECT  PARTY_ID FROM HZ_CUST_ACCOUNTS_INC
 UNION
SELECT  PARTY_ID FROM HZ_PARTIES_INC
""")

INCREMENT_STAGE_TABLE.createOrReplaceTempView("INCREMENT_STAGE_TABLE")
INCREMENT_STAGE_TABLE.cache()
INCREMENT_STAGE_TABLE.count()



# COMMAND ----------

W_CUSTOMER_ACCOUNT_D = spark.sql("""
  SELECT 
    INT(HZ_CA.CUST_ACCOUNT_ID) as INTEGRATION_ID,
    DATE_FORMAT(HZ_CA.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS") CREATED_ON_DT,
    DATE_FORMAT(HZ_CA.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS") CHANGED_ON_DT,
    INT(HZ_CA.CREATED_BY) CREATED_BY_ID,
    INT(HZ_CA.LAST_UPDATED_BY) CHANGED_BY_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,    
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT, 
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    INT(HZ_CA.CUST_ACCOUNT_ID) CUST_ACCOUNT_ID, 
    INT(HZ_CA.PARTY_ID) PARTY_ID, 
    HZ_CA.ACCOUNT_NUMBER ACCOUNT_NUMBER, 
    HZ_CA.ACCOUNT_NAME ACCOUNT_NAME,   
    CASE 
      WHEN HZ_CA.CUSTOMER_TYPE = 'I' THEN 'Internal'
      WHEN HZ_CA.CUSTOMER_TYPE = 'R' THEN 'External'
      ELSE 'Unspecified'
    END CUSTOMER_TYPE,
    HZ_PARTIES.PARTY_NUMBER REGISTRATION_ID,
    'N' DELETE_FLG,
    HZ_CA.ATTRIBUTE11 CUSTOMER_VERTICAL
  FROM 
 INCREMENT_STAGE_TABLE 
  LEFT OUTER JOIN   HZ_CUST_ACCOUNTS HZ_CA  ON INCREMENT_STAGE_TABLE.PARTY_ID = HZ_CA.PARTY_ID
  LEFT OUTER JOIN HZ_PARTIES ON HZ_CA.PARTY_ID = HZ_PARTIES.PARTY_ID
  WHERE HZ_PARTIES.PARTY_TYPE = 'ORGANIZATION'
""")

W_CUSTOMER_ACCOUNT_D.createOrReplaceTempView("W_CUSTOMER_ACCOUNT_D")
W_CUSTOMER_ACCOUNT_D.cache()
W_CUSTOMER_ACCOUNT_D.count()

# COMMAND ----------

count = W_CUSTOMER_ACCOUNT_D.select("INTEGRATION_ID").count()
countDistinct = W_CUSTOMER_ACCOUNT_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_CUSTOMER_ACCOUNT_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

W_CUSTOMER_ACCOUNT_D.unpersist()
HZ_CUST_ACCOUNTS_INC.unpersist()
HZ_PARTIES_INC.unpersist()
INCREMENT_STAGE_TABLE.unpersist()
W_CUSTOMER_ACCOUNT_D.unpersist()

# COMMAND ----------

#%sql
#select * from w_customer_account_d

# COMMAND ----------


