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
tableName = 'W_INT_ORG_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET

# guy 2020/06/10 : need to include incremental dataset based on unique HR_ALL_ORGANIZATION_UNITS.ORGANIZATION_ID and HR_ORGANIZATION_INFORMATION.ORGANIZATION_ID from delta file 

sourceFileUrl = sourceFolderUrl + 'apps.hr_all_organization_units.par'
HR_ALL_ORGANIZATION_UNITS_INC = spark.read.parquet(sourceFileUrl)
HR_ALL_ORGANIZATION_UNITS_INC.createOrReplaceTempView("HR_ALL_ORGANIZATION_UNITS_INC")

sourceFileUrl = sourceFolderUrl + 'apps.hr_organization_information.par'
HR_ORGANIZATION_INFORMATION_INC = spark.read.parquet(sourceFileUrl)
HR_ORGANIZATION_INFORMATION_INC.createOrReplaceTempView("HR_ORGANIZATION_INFORMATION_INC")

INCREMENT_STAGE_TABLE = spark.sql("""
 SELECT ORGANIZATION_ID FROM HR_ALL_ORGANIZATION_UNITS_INC 
 UNION
 SELECT  ORGANIZATION_ID FROM HR_ORGANIZATION_INFORMATION_INC 
""")

INCREMENT_STAGE_TABLE.createOrReplaceTempView("INCREMENT_STAGE_TABLE")
INCREMENT_STAGE_TABLE.cache()
INCREMENT_STAGE_TABLE.count()



# COMMAND ----------

OP_ORG = spark.sql("""
  SELECT 
    MIN(HOI1.ORGANIZATION_ID) INV_ORG, 
    CAST(HOI1.ORG_INFORMATION3 as INT) OP_UNIT, 
    GL.CURRENCY_CODE
  FROM
    HR_ORGANIZATION_INFORMATION HOI1
    JOIN HR_ORGANIZATION_INFORMATION HOI2 ON HOI2.ORGANIZATION_ID = CAST(HOI1.ORG_INFORMATION3 AS INT)
    LEFT OUTER JOIN GL_LEDGERS GL ON GL.LEDGER_ID = CAST(HOI1.ORG_INFORMATION1 AS INT)
  WHERE 
    HOI1.ORG_INFORMATION_CONTEXT = 'Accounting Information'
    AND HOI2.ORG_INFORMATION_CONTEXT = 'CLASS'
    AND HOI2.org_information1 = 'OPERATING_UNIT'
  GROUP BY 
   CAST(HOI1.ORG_INFORMATION3 AS INT), 
   GL.CURRENCY_CODE
""")

OP_ORG.createOrReplaceTempView("OP_ORG")
OP_ORG.cache()
OP_ORG.count()

# COMMAND ----------

W_INT_ORG_D = spark.sql("""
  SELECT 
    INT(HAOU.ORGANIZATION_ID) INTEGRATION_ID,
    DATE_FORMAT(HAOU.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS") CREATED_ON_DT,
    DATE_FORMAT(HAOU.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS") CHANGED_ON_DT,
    INT(HAOU.CREATED_BY) CREATED_BY_ID,
    INT(HAOU.LAST_UPDATED_BY) CHANGED_BY_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT,    
    DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT, 
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(current_timestamp(),'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
    INT(HAOU.ORGANIZATION_ID) ORGANIZATION_ID,
    HAOU.NAME ORG_NAME,  
    CASE 
      WHEN HOI1.ORG_INFORMATION1 = 'OPERATING_UNIT' THEN 'Y' 
      ELSE 'N' 
    END OPERATING_UNIT_FLG,
    CASE 
      WHEN HOI1.ORG_INFORMATION1 = 'INV' THEN 'Y' 
      ELSE 'N' 
    END INV_ORG_FLG,
    CASE 
      WHEN HOI1.ORG_INFORMATION1 = 'OPERATING_UNIT' THEN 'Y' 
      ELSE 'N' 
    END SALES_ORG_FLG,
    NVL(GL.CURRENCY_CODE,OP_ORG.CURRENCY_CODE) CURRENCY_CODE, 
    MP.ORGANIZATION_CODE ORGANIZATION_CODE,
    'N' DELETE_FLG,
    INT(HOI2.ORG_INFORMATION3) OPERATING_UNIT_ID
  FROM 
  INCREMENT_STAGE_TABLE LEFT OUTER JOIN HR_ALL_ORGANIZATION_UNITS HAOU ON INCREMENT_STAGE_TABLE.ORGANIZATION_ID = HAOU.ORGANIZATION_ID 
  LEFT OUTER JOIN OP_ORG ON HAOU.ORGANIZATION_ID = OP_ORG.OP_UNIT
  JOIN HR_ORGANIZATION_INFORMATION HOI1 ON HOI1.ORGANIZATION_ID = HAOU.ORGANIZATION_ID 
  LEFT OUTER JOIN (
    SELECT * 
    FROM HR_ORGANIZATION_INFORMATION 
    WHERE ORG_INFORMATION_CONTEXT = 'Accounting Information'
  ) HOI2 ON HAOU.ORGANIZATION_ID = HOI2.ORGANIZATION_ID
  LEFT OUTER JOIN MTL_PARAMETERS MP ON MP.ORGANIZATION_ID = HAOU.ORGANIZATION_ID 
  LEFT OUTER JOIN GL_LEDGERS GL ON CAST(HOI2.ORG_INFORMATION1 AS INT) = GL.LEDGER_ID
  WHERE 
    HOI1.ORG_INFORMATION_CONTEXT = 'CLASS'
    AND HOI1.ORG_INFORMATION2 = 'Y' 
    AND HOI1.ORG_INFORMATION1 IN ('OPERATING_UNIT', 'INV')
""")

W_INT_ORG_D.createOrReplaceTempView("W_INT_ORG_D")
W_INT_ORG_D.cache()
W_INT_ORG_D.count()

# COMMAND ----------

count = W_INT_ORG_D.select("INTEGRATION_ID").count()
countDistinct = W_INT_ORG_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_INT_ORG_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

HR_ALL_ORGANIZATION_UNITS_INC.unpersist()
HR_ORGANIZATION_INFORMATION_INC.unpersist()
INCREMENT_STAGE_TABLE.unpersist()
OP_ORG.unpersist()
W_INT_ORG_D.unpersist()
