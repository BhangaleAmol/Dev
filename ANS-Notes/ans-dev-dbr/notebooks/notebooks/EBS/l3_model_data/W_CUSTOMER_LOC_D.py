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
tableName = 'W_CUSTOMER_LOC_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths

sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET

# guy 2020/06/10 : No incremental logic included : need to include incremental datasets based on unique : 
# HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID 
# HZ_PARTIES.PARTY_ID 
# HZ_LOCATIONS.LOCATION_ID from delta file 
# also last_update_date logic for Email, Phone subqueries to be included if needed (not in current logic) 

sourceFileUrl = sourceFolderUrl + 'apps.hz_cust_acct_sites_all.par'
HZ_CUST_ACCT_SITES_ALL_INC = spark.read.parquet(sourceFileUrl)
HZ_CUST_ACCT_SITES_ALL_INC.createOrReplaceTempView("HZ_CUST_ACCT_SITES_ALL_INC")


sourceFileUrl = sourceFolderUrl + 'apps.hz_party_sites.par'
HZ_PARTY_SITES_INC = spark.read.parquet(sourceFileUrl)
HZ_PARTY_SITES_INC.createOrReplaceTempView("HZ_PARTY_SITES_INC")

sourceFileUrl = sourceFolderUrl + 'apps.hz_locations.par'
HZ_LOCATIONS_INC = spark.read.parquet(sourceFileUrl)
HZ_LOCATIONS_INC.createOrReplaceTempView("HZ_LOCATIONS_INC")

INCREMENT_STAGE_TABLE = spark.sql("""
  SELECT DISTINCT
    HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID ,
    HZ_PARTY_SITES.LOCATION_ID
  FROM 
    HZ_CUST_ACCT_SITES_ALL_INC HZ_CUST_ACCT_SITES_ALL,
    HZ_PARTY_SITES_INC HZ_PARTY_SITES,
    HZ_LOCATIONS_INC HZ_LOCATIONS
  WHERE      
    HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID = HZ_PARTY_SITES.PARTY_SITE_ID
    AND HZ_PARTY_SITES.LOCATION_ID = HZ_LOCATIONS.LOCATION_ID
""")

INCREMENT_STAGE_TABLE.createOrReplaceTempView("INCREMENT_STAGE_TABLE")
INCREMENT_STAGE_TABLE.cache()
INCREMENT_STAGE_TABLE.count()           
            
#sourceFileUrl = sourceFolderUrl + 'HZ_CUST_ACCT_SITES_ALL.par'
#HZ_CUST_ACCT_SITES_ALL_INC = spark.read.parquet(sourceFileUrl)
#HZ_CUST_ACCT_SITES_ALL_INC.createOrReplaceTempView("HZ_CUST_ACCT_SITES_ALL_INC")

# COMMAND ----------

CT_EMAIL = spark.sql("""
  SELECT   
    CP.EMAIL_ADDRESS,
    CP.OWNER_TABLE_ID,
    ROW_NUMBER() OVER(PARTITION BY CP.OWNER_TABLE_ID ORDER BY CP.LAST_UPDATE_DATE DESC) ROW_NUM
  FROM HZ_CONTACT_POINTS CP
  WHERE
    CP.OWNER_TABLE_NAME = 'HZ_PARTY_SITES' 
    AND CP.CONTACT_POINT_TYPE = 'EMAIL'
    AND CP.PRIMARY_FLAG = 'Y'
    AND CP.STATUS = 'A' 
""")

CT_EMAIL.createOrReplaceTempView("CT_EMAIL")
CT_EMAIL.cache()
CT_EMAIL.count()

# COMMAND ----------

CT_PHONE_NUMBER = spark.sql("""
  SELECT 
    CP.RAW_PHONE_NUMBER,
    CP.OWNER_TABLE_ID,
    ROW_NUMBER() OVER(PARTITION BY CP.OWNER_TABLE_ID ORDER BY CP.LAST_UPDATE_DATE DESC) ROW_NUM
  FROM HZ_CONTACT_POINTS CP 
  WHERE 
    CP.OWNER_TABLE_NAME = 'HZ_PARTY_SITES' 
    AND CP.CONTACT_POINT_TYPE = 'PHONE'
    AND CP.PHONE_LINE_TYPE = 'GEN'
    AND CP.STATUS = 'A'
""")

CT_PHONE_NUMBER.createOrReplaceTempView("CT_PHONE_NUMBER")
CT_PHONE_NUMBER.cache()
CT_PHONE_NUMBER.count()

# COMMAND ----------

HZ_CUST_SITE_USES_ALL_SUB = spark.sql("""
  SELECT
    CUST_ACCT_SITE_ID,
    WAREHOUSE_ID,
    SITE_USE_CODE
  FROM HZ_CUST_SITE_USES_ALL
  WHERE  SITE_USE_CODE =  'SHIP_TO'
  AND STATUS = 'A'
""")

HZ_CUST_SITE_USES_ALL_SUB.createOrReplaceTempView("HZ_CUST_SITE_USES_ALL_SUB")
HZ_CUST_SITE_USES_ALL_SUB.cache()
HZ_CUST_SITE_USES_ALL_SUB.count()

# COMMAND ----------


CASUPD = spark.sql("""
  SELECT CAS1.CUST_ACCT_SITE_ID
  FROM HZ_CUST_ACCT_SITES_ALL CAS1  
  UNION
  SELECT CAS2.CUST_ACCT_SITE_ID
  FROM 
    HZ_CUST_ACCT_SITES_ALL CAS2, 
    HZ_PARTY_SITES PS2
  WHERE CAS2.PARTY_SITE_ID = PS2.PARTY_SITE_ID    
  UNION
  SELECT CAS3.CUST_ACCT_SITE_ID
  FROM 
    HZ_CUST_ACCT_SITES_ALL CAS3, 
    HZ_PARTY_SITES PS3, 
    HZ_LOCATIONS L3
  WHERE 
    CAS3.PARTY_SITE_ID = PS3.PARTY_SITE_ID 
    AND PS3.LOCATION_ID = L3.LOCATION_ID    
  UNION
  SELECT CAS4.CUST_ACCT_SITE_ID
  FROM 
    HZ_CUST_ACCT_SITES_ALL CAS4, 
    HZ_CONTACT_POINTS CP4
  WHERE
    CP4.OWNER_TABLE_NAME = 'HZ_PARTY_SITES'
    AND CP4.OWNER_TABLE_ID = CAS4.PARTY_SITE_ID
    AND CP4.CONTACT_POINT_TYPE IN ('EMAIL','WEB', 'PHONE')    
    AND CP4.PRIMARY_FLAG = 'Y'
 """)

CASUPD.createOrReplaceTempView("CASUPD")
CASUPD.cache()
CASUPD.count()

# COMMAND ----------

# INCREMENT_STAGE_TABLE.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

W_CUSTOMER_LOC_D = spark.sql("""
  SELECT 
    INT(HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID) INTEGRATION_ID,
    DATE_FORMAT(HZ_LOCATIONS.CREATION_DATE, 'yyyy-MM-dd HH:mm:ss.SSS') CREATED_ON_DT,
    DATE_FORMAT(HZ_LOCATIONS.LAST_UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss.SSS') CHANGED_ON_DT,
    HZ_LOCATIONS.CREATED_BY CREATED_BY_ID,
    HZ_LOCATIONS.LAST_UPDATED_BY CHANGED_BY_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT,
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
    HZ_PARTY_SITES.PARTY_SITE_NUMBER,
    HZ_CUST_ACCT_SITES_ALL.CUST_ACCOUNT_ID,
    HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID,
    HZ_LOCATIONS.ADDRESS1 LOCATION_NAME,
    HZ_LOCATIONS.ADDRESS2 ADDRESS1,
    HZ_LOCATIONS.ADDRESS3 ADDRESS2,
    HZ_LOCATIONS.ADDRESS4 ADDRESS3,
    HZ_LOCATIONS.CITY,
    HZ_LOCATIONS.STATE,
    HZ_LOCATIONS.COUNTRY,
    HZ_LOCATIONS.POSTAL_CODE,
    CT_EMAIL.EMAIL_ADDRESS EMAIL,
    CT_PHONE_NUMBER.RAW_PHONE_NUMBER PHONE_NUMBER,
    'N' DELETE_FLG,
    INT(COALESCE(HZ_CUST_SITE_USES_ALL_SUB.WAREHOUSE_ID, 0)) ORGANIZATION_ID
FROM 
  INCREMENT_STAGE_TABLE 
  LEFT OUTER JOIN HZ_CUST_ACCT_SITES_ALL 
    ON INCREMENT_STAGE_TABLE.PARTY_SITE_ID = HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID
  LEFT OUTER JOIN HZ_PARTY_SITES 
    ON HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID = HZ_PARTY_SITES.PARTY_SITE_ID
  LEFT OUTER JOIN HZ_LOCATIONS  
    ON INCREMENT_STAGE_TABLE.LOCATION_ID = HZ_LOCATIONS.LOCATION_ID
  LEFT OUTER JOIN HZ_CUST_SITE_USES_ALL_SUB 
    ON HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID = HZ_CUST_SITE_USES_ALL_SUB.CUST_ACCT_SITE_ID
  LEFT OUTER JOIN CT_EMAIL 
    ON CT_EMAIL.OWNER_TABLE_ID = HZ_PARTY_SITES.PARTY_SITE_ID AND CT_EMAIL.ROW_NUM = 1
  LEFT OUTER JOIN CT_PHONE_NUMBER 
    ON CT_PHONE_NUMBER.OWNER_TABLE_ID = HZ_PARTY_SITES.PARTY_SITE_ID AND CT_PHONE_NUMBER.ROW_NUM = 1 
""")

#W_CUSTOMER_LOC_D.createOrReplaceTempView("W_CUSTOMER_LOC_D")
W_CUSTOMER_LOC_D.persist(StorageLevel.DISK_ONLY)
W_CUSTOMER_LOC_D.count()

# COMMAND ----------



# COMMAND ----------

count = W_CUSTOMER_LOC_D.select("INTEGRATION_ID").count()
countDistinct = W_CUSTOMER_LOC_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_CUSTOMER_LOC_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

HZ_CUST_ACCT_SITES_ALL_INC.unpersist()
HZ_PARTY_SITES_INC.unpersist()
HZ_LOCATIONS_INC.unpersist()
INCREMENT_STAGE_TABLE.unpersist()
CT_EMAIL.unpersist()
CT_PHONE_NUMBER.unpersist()
HZ_CUST_SITE_USES_ALL_SUB.unpersist()
CASUPD.unpersist()
W_CUSTOMER_LOC_D.unpersist()
