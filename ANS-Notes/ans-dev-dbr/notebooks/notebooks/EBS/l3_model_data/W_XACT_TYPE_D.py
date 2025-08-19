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
tableName = 'W_XACT_TYPE_D' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
sourceFolderCsvUrl = DATALAKE_ENDPOINT + '/datalake/EBS/raw_data/domain_values/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET


# NO INCREMENTAL LOGIC DEFINED


#INCREMENT_STAGE_TABLE.createOrReplaceTempView("INCREMENT_STAGE_TABLE")
#INCREMENT_STAGE_TABLE.cache()
#INCREMENT_STAGE_TABLE.count()      

# CSV
sourceFileUrl = sourceFolderCsvUrl + 'domainValues_OrderTypes_ora11i.csv'
DOMAIN_VALUE_ORDER = spark.read.option("header", "true").csv(sourceFileUrl)
DOMAIN_VALUE_ORDER.createOrReplaceTempView("DOMAIN_VALUE_ORDER")

# COMMAND ----------

SALES_IVCLNS = spark.sql("""
SELECT 
    CONCAT('SALES_IVCLNS' ,'-',
    COALESCE(int(RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID), '') ,'-',
    COALESCE(int(RA_CUST_TRX_TYPES_ALL.ORG_ID), ''),'-', 
    COALESCE(B.DESCRIPTIVE_FLEX_CONTEXT_CODE, ''))     INTEGRATION_ID,
    date_format(RA_CUST_TRX_TYPES_ALL.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(RA_CUST_TRX_TYPES_ALL.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    RA_CUST_TRX_TYPES_ALL.CREATED_BY                      CREATED_BY_ID,
    RA_CUST_TRX_TYPES_ALL.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,

    'SALES_IVCLNS'                      XACT_CODE,
    RA_CUST_TRX_TYPES_ALL.TYPE          XACT_TYPE_CODE,
    RA_CUST_TRX_TYPES_ALL.TYPE          XACT_TYPE_DESC,
    RA_CUST_TRX_TYPES_ALL.NAME          XACT_SUBTYPE_CODE, 
    RA_CUST_TRX_TYPES_ALL.NAME          XACT_SUBTYPE_NAME, 
    RA_CUST_TRX_TYPES_ALL.DESCRIPTION   XACT_SUBTYPE_DESC, 
    RA_CUST_TRX_TYPES_ALL.TYPE          W_XACT_TYPE_CODE,
    RA_CUST_TRX_TYPES_ALL.TYPE          W_XACT_TYPE_DESC,
    B.DESCRIPTIVE_FLEX_CONTEXT_CODE     XACT_TYPE_CODE1,   
    ''                                  XACT_TYPE_NAME1, 
    T.DESCRIPTION                       XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    'Y'                                 INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
    RA_CUST_TRX_TYPES_ALL 
    cross join FND_DESCR_FLEX_CONTEXTS_TL T
    join FND_DESCR_FLEX_CONTEXTS B on 
    B.APPLICATION_ID = T.APPLICATION_ID AND
    B.DESCRIPTIVE_FLEXFIELD_NAME = T.DESCRIPTIVE_FLEXFIELD_NAME AND
    B.DESCRIPTIVE_FLEX_CONTEXT_CODE = T.DESCRIPTIVE_FLEX_CONTEXT_CODE
where  
    B.DESCRIPTIVE_FLEXFIELD_NAME = 'RA_INTERFACE_LINES' AND
    T.LANGUAGE = 'US' AND B.APPLICATION_ID=222
    AND B.DESCRIPTIVE_FLEX_CONTEXT_CODE NOT IN ('Global_Procurement', 'India Invoices')
    
    UNION
    
    (SELECT
      CONCAT('SALES_IVCLNS' , '-' ,COALESCE(int(CUST_TRX_TYPE_ID), '') ,'-',COALESCE(int(RA_CUST_TRX_TYPES_ALL.ORG_ID), ''),'-')
                                        INTEGRATION_ID,
     date_format(RA_CUST_TRX_TYPES_ALL.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(RA_CUST_TRX_TYPES_ALL.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    RA_CUST_TRX_TYPES_ALL.CREATED_BY                      CREATED_BY_ID,
    RA_CUST_TRX_TYPES_ALL.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
  
     'SALES_IVCLNS'                      XACT_CODE,
    RA_CUST_TRX_TYPES_ALL.TYPE          XACT_TYPE_CODE,
    RA_CUST_TRX_TYPES_ALL.TYPE          XACT_TYPE_DESC,
    RA_CUST_TRX_TYPES_ALL.NAME          XACT_SUBTYPE_CODE, 
    RA_CUST_TRX_TYPES_ALL.NAME          XACT_SUBTYPE_NAME, 
    RA_CUST_TRX_TYPES_ALL.DESCRIPTION   XACT_SUBTYPE_DESC, 
    RA_CUST_TRX_TYPES_ALL.TYPE          W_XACT_TYPE_CODE,
    RA_CUST_TRX_TYPES_ALL.TYPE          W_XACT_TYPE_DESC,
    NULL                                XACT_TYPE_CODE1,   
    NULL                                XACT_TYPE_NAME1, 
    NULL                                XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    'Y'                                 INCLUDE_IN_ACCOUNT_ALLOCATIONS    
FROM
RA_CUST_TRX_TYPES_ALL)

""")
SALES_IVCLNS.createOrReplaceTempView("SALES_IVCLNS")
SALES_IVCLNS.cache()
SALES_IVCLNS.count()

# COMMAND ----------

# any additional order types should be added
SALES_ORDLNS = spark.sql("""
-- Order - X = 1, Y = 1
SELECT
    CONCAT('SALES_ORDLNS','-',COALESCE(FND_LOOKUP_VALUES.LOOKUP_CODE, ''),'-',int(COALESCE(OE_TRANSACTION_TYPES_TL.TRANSACTION_TYPE_ID, '')),'-INTERNAL-DROP SHIP')          INTEGRATION_ID,
    date_format(FND_LOOKUP_VALUES.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(FND_LOOKUP_VALUES.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    FND_LOOKUP_VALUES.CREATED_BY                      CREATED_BY_ID,
    FND_LOOKUP_VALUES.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    
    'SALES_ORDLNS'                      XACT_CODE,
    FND_LOOKUP_VALUES.LOOKUP_CODE       XACT_TYPE_CODE,
    FND_LOOKUP_VALUES.DESCRIPTION       XACT_TYPE_DESC,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_CODE,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_NAME,
    OE_TRANSACTION_TYPES_TL.DESCRIPTION XACT_SUBTYPE_DESC, 
    DVO.W_XACT_TYPE_CODE W_XACT_TYPE_CODE,
    DVO.W_XACT_TYPE_DESC W_XACT_TYPE_DESC,
    'INTERNAL'                          XACT_TYPE_CODE1,
    ''                                  XACT_TYPE_NAME1,
   'This order is an internal order, i.e resulted because of a purchase request.'   XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'N') INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
 (SELECT * FROM FND_LOOKUP_VALUES 
       WHERE FND_LOOKUP_VALUES.VIEW_APPLICATION_ID = 660 
             AND FND_LOOKUP_VALUES.LANGUAGE = 'US'  
             AND FND_LOOKUP_VALUES.LOOKUP_TYPE = 'LINE_CATEGORY'
             AND FND_LOOKUP_VALUES.LOOKUP_CODE = 'ORDER'
 ) FND_LOOKUP_VALUES
 LEFT JOIN DOMAIN_VALUE_ORDER DVO
   ON FND_LOOKUP_VALUES.LOOKUP_CODE = DVO.XACT_TYPE_CODE
 CROSS JOIN
 (SELECT * FROM OE_TRANSACTION_TYPES_TL
     WHERE  OE_TRANSACTION_TYPES_TL.LANGUAGE = 'US') OE_TRANSACTION_TYPES_TL
  LEFT JOIN (SELECT 
              AOTT.NAME,
              'Y' INCLUDE_FLAG
            FROM 
              OE_TRANSACTION_TYPES_TL AOTT,
              FND_LOOKUP_VALUES AFLV
            WHERE   
              AOTT.LANGUAGE = 'US'
              AND AFLV.LOOKUP_TYPE = 'XX_OTC_ITEMDEMAND_ORDERTYPES'
              AND AFLV.ENABLED_FLAG = 'Y'
              AND AFLV.LANGUAGE = 'US'
              AND UPPER (AOTT.NAME) = UPPER (AFLV.LOOKUP_CODE))  ALLOCATION_FLAG
   ON OE_TRANSACTION_TYPES_TL.NAME  = ALLOCATION_FLAG.NAME
     
-- Order - X = 1, Y = 2

UNION 

SELECT
    CONCAT('SALES_ORDLNS','-',COALESCE(FND_LOOKUP_VALUES.LOOKUP_CODE, ''),'-',int(COALESCE(OE_TRANSACTION_TYPES_TL.TRANSACTION_TYPE_ID, '')),'-INTERNAL-SELF SHIP')         INTEGRATION_ID,
    date_format(FND_LOOKUP_VALUES.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(FND_LOOKUP_VALUES.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    FND_LOOKUP_VALUES.CREATED_BY                      CREATED_BY_ID,
    FND_LOOKUP_VALUES.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    
    'SALES_ORDLNS'                      XACT_CODE,
    FND_LOOKUP_VALUES.LOOKUP_CODE       XACT_TYPE_CODE,
    FND_LOOKUP_VALUES.DESCRIPTION       XACT_TYPE_DESC,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_CODE,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_NAME,
    OE_TRANSACTION_TYPES_TL.DESCRIPTION XACT_SUBTYPE_DESC, 
    DVO.W_XACT_TYPE_CODE W_XACT_TYPE_CODE,
    DVO.W_XACT_TYPE_DESC W_XACT_TYPE_DESC,
    'INTERNAL'                          XACT_TYPE_CODE1,
    ''                                  XACT_TYPE_NAME1,
   'This order is an internal order, i.e resulted because of a purchase request.'   XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'N') INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
 (SELECT * FROM FND_LOOKUP_VALUES 
       WHERE FND_LOOKUP_VALUES.VIEW_APPLICATION_ID = 660 
             AND FND_LOOKUP_VALUES.LANGUAGE = 'US'  
             AND FND_LOOKUP_VALUES.LOOKUP_TYPE = 'LINE_CATEGORY'
             AND FND_LOOKUP_VALUES.LOOKUP_CODE = 'ORDER'
 ) FND_LOOKUP_VALUES
 LEFT JOIN DOMAIN_VALUE_ORDER DVO
   ON FND_LOOKUP_VALUES.LOOKUP_CODE = DVO.XACT_TYPE_CODE
 CROSS JOIN
 (SELECT * FROM OE_TRANSACTION_TYPES_TL
     WHERE  OE_TRANSACTION_TYPES_TL.LANGUAGE = 'US') OE_TRANSACTION_TYPES_TL
  LEFT JOIN (SELECT 
              AOTT.NAME,
              'Y' INCLUDE_FLAG
            FROM 
              OE_TRANSACTION_TYPES_TL AOTT,
              FND_LOOKUP_VALUES AFLV
            WHERE   
              AOTT.LANGUAGE = 'US'
              AND AFLV.LOOKUP_TYPE = 'XX_OTC_ITEMDEMAND_ORDERTYPES'
              AND AFLV.ENABLED_FLAG = 'Y'
              AND AFLV.LANGUAGE = 'US'
              AND UPPER (AOTT.NAME) = UPPER (AFLV.LOOKUP_CODE))  ALLOCATION_FLAG
   ON OE_TRANSACTION_TYPES_TL.NAME  = ALLOCATION_FLAG.NAME
   
UNION
 -- Order - X = 2, Y = 1
 
SELECT
    CONCAT('SALES_ORDLNS','-',COALESCE(FND_LOOKUP_VALUES.LOOKUP_CODE, ''),'-',int(COALESCE(OE_TRANSACTION_TYPES_TL.TRANSACTION_TYPE_ID, '')),'-EXTERNAL-DROP SHIP')         INTEGRATION_ID,
    date_format(FND_LOOKUP_VALUES.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(FND_LOOKUP_VALUES.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    FND_LOOKUP_VALUES.CREATED_BY                      CREATED_BY_ID,
    FND_LOOKUP_VALUES.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    
    'SALES_ORDLNS'                      XACT_CODE,
    FND_LOOKUP_VALUES.LOOKUP_CODE       XACT_TYPE_CODE,
    FND_LOOKUP_VALUES.DESCRIPTION       XACT_TYPE_DESC,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_CODE,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_NAME,
    OE_TRANSACTION_TYPES_TL.DESCRIPTION XACT_SUBTYPE_DESC, 
    DVO.W_XACT_TYPE_CODE W_XACT_TYPE_CODE,
    DVO.W_XACT_TYPE_DESC W_XACT_TYPE_DESC,
    'EXTERNAL'                          XACT_TYPE_CODE1,
    ''                                  XACT_TYPE_NAME1,
    'This order is an external order.'  XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
     NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'N') INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
 (SELECT * FROM FND_LOOKUP_VALUES 
       WHERE FND_LOOKUP_VALUES.VIEW_APPLICATION_ID = 660 
             AND FND_LOOKUP_VALUES.LANGUAGE = 'US'  
             AND FND_LOOKUP_VALUES.LOOKUP_TYPE = 'LINE_CATEGORY'
             AND FND_LOOKUP_VALUES.LOOKUP_CODE = 'ORDER'
 ) FND_LOOKUP_VALUES
 LEFT JOIN DOMAIN_VALUE_ORDER DVO
   ON FND_LOOKUP_VALUES.LOOKUP_CODE = DVO.XACT_TYPE_CODE
 CROSS JOIN
 (SELECT * FROM OE_TRANSACTION_TYPES_TL
     WHERE  OE_TRANSACTION_TYPES_TL.LANGUAGE = 'US') OE_TRANSACTION_TYPES_TL
  LEFT JOIN (SELECT 
              AOTT.NAME,
              'Y' INCLUDE_FLAG
            FROM 
              OE_TRANSACTION_TYPES_TL AOTT,
              FND_LOOKUP_VALUES AFLV
            WHERE   
              AOTT.LANGUAGE = 'US'
              AND AFLV.LOOKUP_TYPE = 'XX_OTC_ITEMDEMAND_ORDERTYPES'
              AND AFLV.ENABLED_FLAG = 'Y'
              AND AFLV.LANGUAGE = 'US'
              AND UPPER (AOTT.NAME) = UPPER (AFLV.LOOKUP_CODE))  ALLOCATION_FLAG
   ON OE_TRANSACTION_TYPES_TL.NAME  = ALLOCATION_FLAG.NAME     

UNION
 -- Order - X = 2, Y = 2
 
SELECT
    CONCAT('SALES_ORDLNS','-',COALESCE(FND_LOOKUP_VALUES.LOOKUP_CODE, ''),'-',int(COALESCE(OE_TRANSACTION_TYPES_TL.TRANSACTION_TYPE_ID, '')),'-EXTERNAL-SELF SHIP' )         INTEGRATION_ID,
    date_format(FND_LOOKUP_VALUES.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(FND_LOOKUP_VALUES.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    FND_LOOKUP_VALUES.CREATED_BY                      CREATED_BY_ID,
    FND_LOOKUP_VALUES.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    
    'SALES_ORDLNS'                      XACT_CODE,
    FND_LOOKUP_VALUES.LOOKUP_CODE       XACT_TYPE_CODE,
    FND_LOOKUP_VALUES.DESCRIPTION       XACT_TYPE_DESC,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_CODE,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_NAME,
    OE_TRANSACTION_TYPES_TL.DESCRIPTION XACT_SUBTYPE_DESC, 
    DVO.W_XACT_TYPE_CODE W_XACT_TYPE_CODE,
    DVO.W_XACT_TYPE_DESC W_XACT_TYPE_DESC,
    'EXTERNAL'                          XACT_TYPE_CODE1,
    ''                                  XACT_TYPE_NAME1,
    'This order is an external order.'  XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'N') INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
 (SELECT * FROM FND_LOOKUP_VALUES 
       WHERE FND_LOOKUP_VALUES.VIEW_APPLICATION_ID = 660 
             AND FND_LOOKUP_VALUES.LANGUAGE = 'US'  
             AND FND_LOOKUP_VALUES.LOOKUP_TYPE = 'LINE_CATEGORY'
             AND FND_LOOKUP_VALUES.LOOKUP_CODE = 'ORDER'
 ) FND_LOOKUP_VALUES
 LEFT JOIN DOMAIN_VALUE_ORDER DVO
   ON FND_LOOKUP_VALUES.LOOKUP_CODE = DVO.XACT_TYPE_CODE
 CROSS JOIN
 (SELECT * FROM OE_TRANSACTION_TYPES_TL
     WHERE  OE_TRANSACTION_TYPES_TL.LANGUAGE = 'US') OE_TRANSACTION_TYPES_TL
  LEFT JOIN (SELECT 
              AOTT.NAME,
              'Y' INCLUDE_FLAG
            FROM 
              OE_TRANSACTION_TYPES_TL AOTT,
              FND_LOOKUP_VALUES AFLV
            WHERE   
              AOTT.LANGUAGE = 'US'
              AND AFLV.LOOKUP_TYPE = 'XX_OTC_ITEMDEMAND_ORDERTYPES'
              AND AFLV.ENABLED_FLAG = 'Y'
              AND AFLV.LANGUAGE = 'US'
              AND UPPER (AOTT.NAME) = UPPER (AFLV.LOOKUP_CODE))  ALLOCATION_FLAG
   ON OE_TRANSACTION_TYPES_TL.NAME  = ALLOCATION_FLAG.NAME

 UNION
 
 -- RETURN - X = 1, Y = 1
SELECT
    CONCAT('SALES_ORDLNS','-',COALESCE(FND_LOOKUP_VALUES.LOOKUP_CODE, ''),'-',int(COALESCE(OE_TRANSACTION_TYPES_TL.TRANSACTION_TYPE_ID, '')),'-INTERNAL-DROP SHIP')          INTEGRATION_ID,
    date_format(FND_LOOKUP_VALUES.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(FND_LOOKUP_VALUES.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    FND_LOOKUP_VALUES.CREATED_BY                      CREATED_BY_ID,
    FND_LOOKUP_VALUES.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    
    'SALES_ORDLNS'                      XACT_CODE,
    FND_LOOKUP_VALUES.LOOKUP_CODE       XACT_TYPE_CODE,
    FND_LOOKUP_VALUES.DESCRIPTION       XACT_TYPE_DESC,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_CODE,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_NAME,
    OE_TRANSACTION_TYPES_TL.DESCRIPTION XACT_SUBTYPE_DESC, 
    DVO.W_XACT_TYPE_CODE W_XACT_TYPE_CODE,
    DVO.W_XACT_TYPE_DESC W_XACT_TYPE_DESC,
    'INTERNAL'                          XACT_TYPE_CODE1,
    ''                                  XACT_TYPE_NAME1,
   'This order is an internal order, i.e resulted because of a purchase request.'   XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'N') INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
 (SELECT * FROM FND_LOOKUP_VALUES 
       WHERE FND_LOOKUP_VALUES.VIEW_APPLICATION_ID = 660 
             AND FND_LOOKUP_VALUES.LANGUAGE = 'US'  
             AND FND_LOOKUP_VALUES.LOOKUP_TYPE = 'LINE_CATEGORY'
             AND FND_LOOKUP_VALUES.LOOKUP_CODE = 'RETURN'
 ) FND_LOOKUP_VALUES
 LEFT JOIN DOMAIN_VALUE_ORDER DVO
   ON FND_LOOKUP_VALUES.LOOKUP_CODE = DVO.XACT_TYPE_CODE
 CROSS JOIN
 (SELECT * FROM OE_TRANSACTION_TYPES_TL
     WHERE  OE_TRANSACTION_TYPES_TL.LANGUAGE = 'US') OE_TRANSACTION_TYPES_TL
  LEFT JOIN (SELECT 
              AOTT.NAME,
              'Y' INCLUDE_FLAG
            FROM 
              OE_TRANSACTION_TYPES_TL AOTT,
              FND_LOOKUP_VALUES AFLV
            WHERE   
              AOTT.LANGUAGE = 'US'
              AND AFLV.LOOKUP_TYPE = 'XX_OTC_ITEMDEMAND_ORDERTYPES'
              AND AFLV.ENABLED_FLAG = 'Y'
              AND AFLV.LANGUAGE = 'US'
              AND UPPER (AOTT.NAME) = UPPER (AFLV.LOOKUP_CODE))  ALLOCATION_FLAG
   ON OE_TRANSACTION_TYPES_TL.NAME  = ALLOCATION_FLAG.NAME

-- RETURN - X = 1, Y = 2

UNION 

SELECT
    CONCAT('SALES_ORDLNS','-',COALESCE(FND_LOOKUP_VALUES.LOOKUP_CODE, ''),'-',int(COALESCE(OE_TRANSACTION_TYPES_TL.TRANSACTION_TYPE_ID, '')),'-INTERNAL-SELF SHIP')         INTEGRATION_ID,
    date_format(FND_LOOKUP_VALUES.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(FND_LOOKUP_VALUES.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    FND_LOOKUP_VALUES.CREATED_BY                      CREATED_BY_ID,
    FND_LOOKUP_VALUES.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    
    'SALES_ORDLNS'                      XACT_CODE,
    FND_LOOKUP_VALUES.LOOKUP_CODE       XACT_TYPE_CODE,
    FND_LOOKUP_VALUES.DESCRIPTION       XACT_TYPE_DESC,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_CODE,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_NAME,
    OE_TRANSACTION_TYPES_TL.DESCRIPTION XACT_SUBTYPE_DESC, 
    DVO.W_XACT_TYPE_CODE W_XACT_TYPE_CODE,
    DVO.W_XACT_TYPE_DESC W_XACT_TYPE_DESC,
    'INTERNAL'                          XACT_TYPE_CODE1,
    ''                                  XACT_TYPE_NAME1,
   'This order is an internal order, i.e resulted because of a purchase request.'   XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'N') INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
 (SELECT * FROM FND_LOOKUP_VALUES 
       WHERE FND_LOOKUP_VALUES.VIEW_APPLICATION_ID = 660 
             AND FND_LOOKUP_VALUES.LANGUAGE = 'US'  
             AND FND_LOOKUP_VALUES.LOOKUP_TYPE = 'LINE_CATEGORY'
             AND FND_LOOKUP_VALUES.LOOKUP_CODE = 'RETURN'
 ) FND_LOOKUP_VALUES
 LEFT JOIN DOMAIN_VALUE_ORDER DVO
   ON FND_LOOKUP_VALUES.LOOKUP_CODE = DVO.XACT_TYPE_CODE
 CROSS JOIN
 (SELECT * FROM OE_TRANSACTION_TYPES_TL
     WHERE  OE_TRANSACTION_TYPES_TL.LANGUAGE = 'US') OE_TRANSACTION_TYPES_TL
  LEFT JOIN (SELECT 
              AOTT.NAME,
              'Y' INCLUDE_FLAG
            FROM 
              OE_TRANSACTION_TYPES_TL AOTT,
              FND_LOOKUP_VALUES AFLV
            WHERE   
              AOTT.LANGUAGE = 'US'
              AND AFLV.LOOKUP_TYPE = 'XX_OTC_ITEMDEMAND_ORDERTYPES'
              AND AFLV.ENABLED_FLAG = 'Y'
              AND AFLV.LANGUAGE = 'US'
              AND UPPER (AOTT.NAME) = UPPER (AFLV.LOOKUP_CODE))  ALLOCATION_FLAG
   ON OE_TRANSACTION_TYPES_TL.NAME  = ALLOCATION_FLAG.NAME

UNION
 -- Order - X = 2, Y = 1
 
SELECT
    CONCAT('SALES_ORDLNS','-',COALESCE(FND_LOOKUP_VALUES.LOOKUP_CODE, ''),'-',int(COALESCE(OE_TRANSACTION_TYPES_TL.TRANSACTION_TYPE_ID, '')),'-EXTERNAL-DROP SHIP')         INTEGRATION_ID,
    date_format(FND_LOOKUP_VALUES.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(FND_LOOKUP_VALUES.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    FND_LOOKUP_VALUES.CREATED_BY                      CREATED_BY_ID,
    FND_LOOKUP_VALUES.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    
    'SALES_ORDLNS'                      XACT_CODE,
    FND_LOOKUP_VALUES.LOOKUP_CODE       XACT_TYPE_CODE,
    FND_LOOKUP_VALUES.DESCRIPTION       XACT_TYPE_DESC,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_CODE,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_NAME,
    OE_TRANSACTION_TYPES_TL.DESCRIPTION XACT_SUBTYPE_DESC, 
    DVO.W_XACT_TYPE_CODE W_XACT_TYPE_CODE,
    DVO.W_XACT_TYPE_DESC W_XACT_TYPE_DESC,
    'EXTERNAL'                          XACT_TYPE_CODE1,
    ''                                  XACT_TYPE_NAME1,
    'This order is an external order.'  XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'N') INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
 (SELECT * FROM FND_LOOKUP_VALUES 
       WHERE FND_LOOKUP_VALUES.VIEW_APPLICATION_ID = 660 
             AND FND_LOOKUP_VALUES.LANGUAGE = 'US'  
             AND FND_LOOKUP_VALUES.LOOKUP_TYPE = 'LINE_CATEGORY'
             AND FND_LOOKUP_VALUES.LOOKUP_CODE = 'RETURN'
 ) FND_LOOKUP_VALUES
 LEFT JOIN DOMAIN_VALUE_ORDER DVO
   ON FND_LOOKUP_VALUES.LOOKUP_CODE = DVO.XACT_TYPE_CODE
 CROSS JOIN
 (SELECT * FROM OE_TRANSACTION_TYPES_TL
     WHERE  OE_TRANSACTION_TYPES_TL.LANGUAGE = 'US') OE_TRANSACTION_TYPES_TL
  LEFT JOIN (SELECT 
              AOTT.NAME,
              'Y' INCLUDE_FLAG
            FROM 
              OE_TRANSACTION_TYPES_TL AOTT,
              FND_LOOKUP_VALUES AFLV
            WHERE   
              AOTT.LANGUAGE = 'US'
              AND AFLV.LOOKUP_TYPE = 'XX_OTC_ITEMDEMAND_ORDERTYPES'
              AND AFLV.ENABLED_FLAG = 'Y'
              AND AFLV.LANGUAGE = 'US'
              AND UPPER (AOTT.NAME) = UPPER (AFLV.LOOKUP_CODE))  ALLOCATION_FLAG
   ON OE_TRANSACTION_TYPES_TL.NAME  = ALLOCATION_FLAG.NAME     

UNION
 -- Order - X = 2, Y = 2
 
SELECT
    CONCAT('SALES_ORDLNS','-',COALESCE(FND_LOOKUP_VALUES.LOOKUP_CODE, ''),'-',int(COALESCE(OE_TRANSACTION_TYPES_TL.TRANSACTION_TYPE_ID, '')),'-EXTERNAL-SELF SHIP' )         INTEGRATION_ID,
    date_format(FND_LOOKUP_VALUES.CREATION_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                    CREATED_ON_DT,
    date_format(FND_LOOKUP_VALUES.LAST_UPDATE_DATE,"yyyy-MM-dd HH:mm:ss.SSS")                 CHANGED_ON_DT,
    FND_LOOKUP_VALUES.CREATED_BY                      CREATED_BY_ID,
    FND_LOOKUP_VALUES.LAST_UPDATED_BY                 CHANGED_BY_ID,
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              INSERT_DT,    
    date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS')                                              UPDATE_DT, 
    'EBS'                                                     DATASOURCE_NUM_ID,
    date_format(current_timestamp(),'yyyyMMddHHmmssSSS')   as LOAD_BATCH_ID,
    
    'SALES_ORDLNS'                      XACT_CODE,
    FND_LOOKUP_VALUES.LOOKUP_CODE       XACT_TYPE_CODE,
    FND_LOOKUP_VALUES.DESCRIPTION       XACT_TYPE_DESC,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_CODE,
    OE_TRANSACTION_TYPES_TL.NAME        XACT_SUBTYPE_NAME,
    OE_TRANSACTION_TYPES_TL.DESCRIPTION XACT_SUBTYPE_DESC, 
    DVO.W_XACT_TYPE_CODE W_XACT_TYPE_CODE,
    DVO.W_XACT_TYPE_DESC W_XACT_TYPE_DESC,
    'EXTERNAL'                          XACT_TYPE_CODE1,
    ''                                  XACT_TYPE_NAME1,
    'This order is an external order.'  XACT_TYPE_DESC1,
    'N'                                 DELETE_FLG,
    NVL(ALLOCATION_FLAG.INCLUDE_FLAG, 'N') INCLUDE_IN_ACCOUNT_ALLOCATIONS
FROM
 (SELECT * FROM FND_LOOKUP_VALUES 
       WHERE FND_LOOKUP_VALUES.VIEW_APPLICATION_ID = 660 
             AND FND_LOOKUP_VALUES.LANGUAGE = 'US'  
             AND FND_LOOKUP_VALUES.LOOKUP_TYPE = 'LINE_CATEGORY'
             AND FND_LOOKUP_VALUES.LOOKUP_CODE = 'RETURN'
 ) FND_LOOKUP_VALUES
 LEFT JOIN DOMAIN_VALUE_ORDER DVO
   ON FND_LOOKUP_VALUES.LOOKUP_CODE = DVO.XACT_TYPE_CODE
 CROSS JOIN
 (SELECT * FROM OE_TRANSACTION_TYPES_TL
     WHERE  OE_TRANSACTION_TYPES_TL.LANGUAGE = 'US') OE_TRANSACTION_TYPES_TL
  LEFT JOIN (SELECT 
              AOTT.NAME,
              'Y' INCLUDE_FLAG
            FROM 
              OE_TRANSACTION_TYPES_TL AOTT,
              FND_LOOKUP_VALUES AFLV
            WHERE   
              AOTT.LANGUAGE = 'US'
              AND AFLV.LOOKUP_TYPE = 'XX_OTC_ITEMDEMAND_ORDERTYPES'
              AND AFLV.ENABLED_FLAG = 'Y'
              AND AFLV.LANGUAGE = 'US'
              AND UPPER (AOTT.NAME) = UPPER (AFLV.LOOKUP_CODE))  ALLOCATION_FLAG
   ON OE_TRANSACTION_TYPES_TL.NAME  = ALLOCATION_FLAG.NAME

""")
SALES_ORDLNS.createOrReplaceTempView("SALES_ORDLNS")
SALES_ORDLNS.cache()
SALES_ORDLNS.count()
 

# COMMAND ----------

W_XACT_TYPE_D = spark.sql("""
SELECT * FROM SALES_IVCLNS
UNION
SELECT * FROM SALES_ORDLNS
  
""")

W_XACT_TYPE_D.cache()
W_XACT_TYPE_D.count()
W_XACT_TYPE_D.createOrReplaceTempView("W_XACT_TYPE_D")

# COMMAND ----------

count = W_XACT_TYPE_D.select("INTEGRATION_ID").count()
countDistinct = W_XACT_TYPE_D.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_XACT_TYPE_D.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

SALES_IVCLNS.unpersist()
SALES_ORDLNS.unpersist()
W_XACT_TYPE_D.unpersist()

# COMMAND ----------


