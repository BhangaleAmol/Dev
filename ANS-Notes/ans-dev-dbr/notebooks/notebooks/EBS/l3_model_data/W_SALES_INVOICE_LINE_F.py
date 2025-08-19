# Databricks notebook source
# MAGIC %run ../SHARED/bootstrap

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val maximumOutputRowRatio = 1000L
# MAGIC spark.conf.set("spark.databricks.queryWatchdog.enabled", true)
# MAGIC spark.conf.set("spark.databricks.queryWatchdog.outputRatioThreshold", maximumOutputRowRatio)

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
tableName = 'W_SALES_INVOICE_LINE_F' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET
sourceFileUrl = sourceFolderUrl + 'apps.ra_customer_trx_lines_all.par'
RA_CUSTOMER_TRX_LINES_ALL_INC = spark.read.parquet(sourceFileUrl)
RA_CUSTOMER_TRX_LINES_ALL_INC.createOrReplaceTempView("RA_CUSTOMER_TRX_LINES_ALL_INC")
RA_CUSTOMER_TRX_LINES_ALL_INC.count()

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import when, col, lit

RA_CUSTOMER_TRX_LINES_ALL_INC = (
  RA_CUSTOMER_TRX_LINES_ALL_INC
    .withColumn(
      "rctla", 
      when(col("INTERFACE_LINE_CONTEXT") == lit("GLOBAL_PROCUREMENT"), 0)
      .otherwise(col("INTERFACE_LINE_ATTRIBUTE6").cast(IntegerType()))
    )
)
RA_CUSTOMER_TRX_LINES_ALL_INC.createOrReplaceTempView("RA_CUSTOMER_TRX_LINES_ALL_INC")

# COMMAND ----------

LINE_ORD_NO = spark.sql("""
  SELECT
    CONCAT(COALESCE (A.CUSTOMER_TRX_ID, ''), '-', COALESCE (A.LINE_NUMBER, '')) LINE_KEY,
    A.SALES_ORDER,
    A.SALES_ORDER_LINE,
    A.SALES_ORDER_DATE,
    B.SCHEDULE_SHIP_DATE,
    B.ACTUAL_SHIPMENT_DATE
  FROM RA_CUSTOMER_TRX_LINES_ALL_INC A
  INNER JOIN OE_ORDER_LINES_ALL B ON A.INTERFACE_LINE_ATTRIBUTE6 = B.LINE_ID
  WHERE A.LINE_TYPE = 'LINE' AND A.SALES_ORDER_LINE IS NOT NULL
 """)

LINE_ORD_NO.createOrReplaceTempView("LINE_ORD_NO")
#LINE_ORD_NO.cache()
LINE_ORD_NO.persist(StorageLevel.DISK_ONLY)
LINE_ORD_NO.count()

# COMMAND ----------

AR_RECEIVABLE_APPS_ALL_VW = spark.sql("""
  SELECT 
    ARAA.APPLIED_CUSTOMER_TRX_ID,
    SUM(ARAA.EARNED_DISCOUNT_TAKEN) EARNED_DISCOUNT_TAKEN,
    SUM(ARAA.UNEARNED_DISCOUNT_TAKEN) UNEARNED_DISCOUNT_TAKEN
  FROM AR_RECEIVABLE_APPLICATIONS_ALL ARAA
  INNER JOIN AR_CASH_RECEIPTS_ALL ACRA ON ARAA.CASH_RECEIPT_ID = ACRA.CASH_RECEIPT_ID
  WHERE ARAA.STATUS = 'APP' AND ARAA.CASH_RECEIPT_ID IS NOT NULL
  GROUP BY ARAA.APPLIED_CUSTOMER_TRX_ID
""")

AR_RECEIVABLE_APPS_ALL_VW.createOrReplaceTempView("AR_RECEIVABLE_APPS_ALL_VW")
#AR_RECEIVABLE_APPS_ALL_VW.cache()
AR_RECEIVABLE_APPS_ALL_VW.persist(StorageLevel.DISK_ONLY)
AR_RECEIVABLE_APPS_ALL_VW.count()

# COMMAND ----------

RA_CUSTOMER_TRX_LINES_ALL_VW = spark.sql("""
  SELECT 
    CUSTOMER_TRX_ID,
    SUM((QUANTITY_INVOICED * UNIT_SELLING_PRICE)) TOTAL_INVOICED_AMOUNT
  FROM RA_CUSTOMER_TRX_LINES_ALL_INC
  GROUP BY CUSTOMER_TRX_ID
""")

RA_CUSTOMER_TRX_LINES_ALL_VW.createOrReplaceTempView("RA_CUSTOMER_TRX_LINES_ALL_VW")
#RA_CUSTOMER_TRX_LINES_ALL_VW.cache()
RA_CUSTOMER_TRX_LINES_ALL_VW.persist(StorageLevel.DISK_ONLY)
RA_CUSTOMER_TRX_LINES_ALL_VW.count()

# COMMAND ----------

CUSTOMER_LOC_USE_D = spark.sql("""
  SELECT
    HCSUA.SITE_USE_ID, 
    HCSUA.LAST_UPDATED_BY,
    HCSUA.CREATION_DATE,
    HCSUA.CREATED_BY,
    HCA.CUST_ACCOUNT_ID,
    HCSUA.SITE_USE_CODE,
    HCSUA.LOCATION,
    HL.LOCATION_ID,
    HCASA.CUST_ACCT_SITE_ID,
    HPS.PARTY_SITE_ID,
    HCASA.LAST_UPDATE_DATE,
    HCSUA.STATUS,
    HCSUA.TERRITORY_ID
  FROM HZ_CUST_ACCT_SITES_ALL HCASA
  INNER JOIN HZ_CUST_SITE_USES_ALL HCSUA ON HCSUA.CUST_ACCT_SITE_ID = HCASA.CUST_ACCT_SITE_ID
  INNER JOIN HZ_PARTY_SITES HPS ON HCASA.PARTY_SITE_ID = HPS.PARTY_SITE_ID
  INNER JOIN HZ_CUST_ACCOUNTS HCA ON HCA.CUST_ACCOUNT_ID = HCASA.CUST_ACCOUNT_ID
  INNER JOIN HZ_LOCATIONS HL ON HPS.LOCATION_ID = HL.LOCATION_ID
  WHERE HCSUA.SITE_USE_CODE = 'SHIP_TO'
""")

CUSTOMER_LOC_USE_D.createOrReplaceTempView("CUSTOMER_LOC_USE_D")
#CUSTOMER_LOC_USE_D.cache()
CUSTOMER_LOC_USE_D.persist(StorageLevel.DISK_ONLY)
CUSTOMER_LOC_USE_D.count()

# COMMAND ----------

QP_LIST_LINES_V = spark.sql('''
SELECT
    LIST_LINE_ID,
    CREATION_DATE,
    LIST_HEADER_ID,
    PRODUCT_ATTRIBUTE_CONTEXT,
    END_DATE_ACTIVE,
    LIST_LINE_TYPE_CODE,
    OPERAND,
    PRICING_PHASE_ID,
    PRODUCT_ID,
    PRICING_ATTRIBUTE_ID,
    PRODUCT_ATTRIBUTE,
    START_DATE_ACTIVE,
    C1_PRICING_ATTRIBUTE_ID,
    C2_PRICING_ATTRIBUTE_ID,
    C3_LIST_LINE_ID
FROM
    (
        SELECT
            QPLL.LIST_LINE_ID    LIST_LINE_ID,
            QPLL.CREATION_DATE   CREATION_DATE,
            QPLL.LIST_HEADER_ID,
            QPPR.PRODUCT_ATTRIBUTE_CONTEXT,
            QPLL.END_DATE_ACTIVE,
            QPLL.LIST_LINE_TYPE_CODE,
            QPLL.OPERAND,
            QPLL.PRICING_PHASE_ID,
            QPPR.PRICING_ATTRIBUTE_ID,
            QPPR.PRODUCT_ATTRIBUTE,
            CASE
                WHEN UPPER(QPPR.PRODUCT_ATTR_VALUE) = 'ALL' THEN NULL
                ELSE QPPR.PRODUCT_ATTR_VALUE
            END PRODUCT_ID,
            QPLL.START_DATE_ACTIVE,
            (
                SELECT
                    MAX(PRICING_ATTRIBUTE_ID)
                FROM
                    QP_PRICING_ATTRIBUTES
                WHERE
                    QPPR.LIST_LINE_ID = LIST_LINE_ID
                    AND PRICING_ATTRIBUTE_CONTEXT = 'PRICING ATTRIBUTE'
                    AND PRICING_ATTRIBUTE = 'PRICING_ATTRIBUTE11'
            ) C1_PRICING_ATTRIBUTE_ID,
            (
                SELECT
                    MAX(PRICING_ATTRIBUTE_ID)
                FROM
                    QP_PRICING_ATTRIBUTES
                WHERE
                    QPPR.LIST_LINE_ID = LIST_LINE_ID
                    AND PRICING_ATTRIBUTE_CONTEXT IS NULL
                    AND PRICING_ATTRIBUTE IS NULL
                    AND EXCLUDER_FLAG = 'N'
            ) C2_PRICING_ATTRIBUTE_ID,
            (
                SELECT
                    MAX(QP_PRICING_ATTRIBUTES.LIST_LINE_ID) LIST_LINE_ID
                FROM
                    QP_PRICING_ATTRIBUTES
                WHERE
                    QPPR.LIST_LINE_ID = LIST_LINE_ID
                    --AND PRICING_ATTRIBUTE_CONTEXT = 'PRICING ATTRIBUTE'
                    AND PRODUCT_ATTRIBUTE = 'PRICING_ATTRIBUTE11'
            ) C3_LIST_LINE_ID
        FROM
            QP_LIST_LINES QPLL,
            QP_PRICING_ATTRIBUTES QPPR
        WHERE
            QPPR.LIST_LINE_ID = QPLL.LIST_LINE_ID
            AND QPLL.LIST_LINE_TYPE_CODE IN (
                'PLL',
                'PBH'
            )
            AND QPPR.PRICING_PHASE_ID = 1
            AND QPPR.QUALIFICATION_IND IN (
                4,
                6,
                20,
                22
            )
            AND QPLL.PRICING_PHASE_ID = 1
            AND QPLL.QUALIFICATION_IND IN (
                4,
                6,
                20,
                22
            )
            AND QPPR.LIST_HEADER_ID = QPLL.LIST_HEADER_ID
    ) Q1
WHERE
    Q1.PRICING_ATTRIBUTE_ID = C1_PRICING_ATTRIBUTE_ID
    OR ( Q1.PRICING_ATTRIBUTE_ID = C2_PRICING_ATTRIBUTE_ID
         AND C3_LIST_LINE_ID IS NULL )
''')

QP_LIST_LINES_V.createOrReplaceTempView("QP_LIST_LINES_V")
#QP_LIST_LINES_V.cache()
QP_LIST_LINES_V.persist(StorageLevel.DISK_ONLY)
QP_LIST_LINES_V.count()

# COMMAND ----------

# PRIOR_PRICE = spark.sql("""
# SELECT
#     LIST_HEADER_ID,
#     INVENTORY_ITEM_ID,
#     START_DATE_ACTIVE,
#     END_DATE_ACTIVE,
#     PRICE_LIST_NAME,
#     OLD_PRICE,
#     PRODUCT_UOM_CODE,
#     LIST_LINE_ID
# FROM
#     (
#         SELECT
#             LIST_HEADER_ID,
#             INVENTORY_ITEM_ID,
#             START_DATE_ACTIVE,
#             END_DATE_ACTIVE,
#             PRICE_LIST_NAME,
#             OLD_PRICE,
#             PRODUCT_UOM_CODE,
#             LIST_LINE_ID,
#             ROW_NUMBER() OVER(
#                 PARTITION BY LIST_HEADER_ID, INVENTORY_ITEM_ID
#                 ORDER BY
#                     END_DATE_ACTIVE DESC
#             ) SEQN,
#             LEVEL
#         FROM
# -- price list based on item number
#             (
#                 SELECT
#                     QPLH.LIST_HEADER_ID,
#                     MTL.INVENTORY_ITEM_ID,
#                     QPLL.START_DATE_ACTIVE,
#                     QPLL.END_DATE_ACTIVE,
#                     QPLTL.NAME     PRICE_LIST_NAME,
#     --ROW_NUMBER() OVER (PARTITION BY QPLH.LIST_HEADER_ID, MTL.INVENTORY_ITEM_ID INVENTORY_ITEM_ID SEQN,
#                     QPLL.OPERAND   OLD_PRICE,
#                    --
#                                                 --                                                                apps.xx_order_item_demand_pkg.uom_conv 
#                                                 --                                                                (ool.order_quantity_uom,
#                                                 --                                                                qppr.product_uom_code,
#                                                 --                                                                mtl.inventory_item_id) )old_price   
#                     QPPR.PRODUCT_UOM_CODE,
#                     QPLL.LIST_LINE_ID,
#                     'ITEM' LEVEL
#                 FROM
#                     QP_LIST_HEADERS_B QPLH,
#                     QP_LIST_LINES QPLL,
#                     QP_PRICING_ATTRIBUTES QPPR,
#                     MTL_SYSTEM_ITEMS_B MTL,
#                     QP_LIST_HEADERS_TL QPLTL
#                 WHERE
#                     QPLH.LIST_HEADER_ID = QPLL.LIST_HEADER_ID
#                     AND QPLL.END_DATE_ACTIVE IS NOT NULL
#                     AND QPPR.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
#                     AND QPPR.LIST_LINE_ID = QPLL.LIST_LINE_ID
#                     AND INT(QPPR.PRODUCT_ATTR_VALUE) = MTL.INVENTORY_ITEM_ID
#                     AND MTL.ORGANIZATION_ID = 124
#                     AND QPLL.LIST_LINE_TYPE_CODE IN (
#                         'PLL',
#                         'PBH'
#                     )
#                     AND QPPR.PRICING_PHASE_ID = 1
#                     AND QPPR.QUALIFICATION_IND IN (
#                         4,
#                         6,
#                         20,
#                         22
#                     )
#                     AND QPLL.PRICING_PHASE_ID = 1
#                     AND QPLL.QUALIFICATION_IND IN (
#                         4,
#                         6,
#                         20,
#                         22
#                     )
#     --and a.list_header_id = ool.price_list_id
#                     AND QPLL.END_DATE_ACTIVE IS NOT NULL
#                     AND QPPR.PRODUCT_ATTR_VALUE IS NOT NULL
#                     AND QPLH.LIST_TYPE_CODE = 'PRL'
#                     AND QPLH.LIST_HEADER_ID = QPLTL.LIST_HEADER_ID
#                     AND QPLTL.LANGUAGE = 'US'
#      --   AND REGEXP_EXTRACT( QPPR.PRODUCT_ATTR_VALUE, '^[[:digit:]]+$')
#                 UNION -- Pricelists based on Variant
#                 SELECT
#                     QLHB.LIST_HEADER_ID,
#                     MSI1.INVENTORY_ITEM_ID,
#                     QPLL.START_DATE_ACTIVE,
#                     QPLL.END_DATE_ACTIVE,
#                     QPLTL.NAME     PRICE_LIST_NAME,
#                     QPLL.OPERAND   OLD_PRICE,
#                     QPPR.PRODUCT_UOM_CODE,
#                     QPLL.LIST_LINE_ID,
#                     'VARIANT' LEVEL
#                 FROM
#                     QP_LIST_HEADERS_B QLHB,
#                     QP_LIST_LINES_V QPLL,
#                     QP_PRICING_ATTRIBUTES QPPR,
#                     MTL_ITEM_CATEGORIES MIC,
#                     MTL_SYSTEM_ITEMS_B MSI1,
#                     QP_LIST_HEADERS_TL QPLTL
#                 WHERE
#                     1 = 1
#                     AND QPLL.LIST_HEADER_ID = QLHB.LIST_HEADER_ID
#                     AND QPLL.LIST_HEADER_ID = QPLTL.LIST_HEADER_ID
#                     AND QPLTL.LANGUAGE = 'US'
#                     AND UPPER(QLHB.ACTIVE_FLAG) = 'Y'
#                     AND UPPER(QLHB.ACTIVE_FLAG) = 'Y'
#                     AND QLHB.LIST_TYPE_CODE = 'PRL'
#                     AND QPLL.PRODUCT_ATTRIBUTE_CONTEXT = QPPR.PRODUCT_ATTRIBUTE_CONTEXT
#                     AND QPPR.LIST_LINE_ID = QPLL.LIST_LINE_ID
#                     AND QPPR.PRICING_PHASE_ID = QPLL.PRICING_PHASE_ID
#                     AND QPLL.PRODUCT_ATTRIBUTE = QPPR.PRODUCT_ATTRIBUTE
#                     AND QPPR.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
#                     AND QLHB.LIST_HEADER_ID = QPPR.LIST_HEADER_ID
#                     AND QPLL.PRODUCT_ID = MIC.CATEGORY_ID
# --    AND MIC.INVENTORY_ITEM_ID = MSI1.INVENTORY_ITEM_ID
#                     AND MIC.ORGANIZATION_ID = 124
#                     AND QPLL.END_DATE_ACTIVE IS NOT NULL
#                     AND QLHB.LIST_TYPE_CODE = 'PRL'
#                     AND QPLL.PRODUCT_ATTRIBUTE_CONTEXT = QPPR.PRODUCT_ATTRIBUTE_CONTEXT
#                     AND QPPR.LIST_LINE_ID = QPLL.LIST_LINE_ID
#                     AND QPPR.PRICING_PHASE_ID = QPLL.PRICING_PHASE_ID
#                     AND QPLL.PRODUCT_ATTRIBUTE = QPPR.PRODUCT_ATTRIBUTE
#                     AND QPPR.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
#                     AND QLHB.LIST_HEADER_ID = QPPR.LIST_HEADER_ID
#                     AND QPLL.PRODUCT_ID = MIC.CATEGORY_ID
#                     AND MIC.INVENTORY_ITEM_ID = MSI1.INVENTORY_ITEM_ID
#                     AND MIC.ORGANIZATION_ID = 124
#                     AND QPLL.END_DATE_ACTIVE IS NOT NULL
#                     AND MIC.ORGANIZATION_ID = MSI1.ORGANIZATION_ID
#                     AND MIC.ORGANIZATION_ID = 124
#                     AND (QPPR.PRICING_ATTRIBUTE_ID = C1_PRICING_ATTRIBUTE_ID
#                         OR ( QPPR.PRICING_ATTRIBUTE_ID = C2_PRICING_ATTRIBUTE_ID
#                              AND C3_LIST_LINE_ID IS NULL ))
# --  --  AND REGEXP_EXTRACT( QPLL.PRODUCT_ID, '^[[:digit:]]+$')
#             )
#     )
# WHERE
#     1 = 1
#     AND SEQN = 1
#   """)
# PRIOR_PRICE.createOrReplaceTempView("PRIOR_PRICE")
# PRIOR_PRICE.persist(StorageLevel.DISK_ONLY)
# PRIOR_PRICE.count()

# COMMAND ----------

# Modification to get May 2020 Price only
PRIOR_PRICE = spark.sql("""
SELECT
    LIST_HEADER_ID,
    INVENTORY_ITEM_ID,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    PRICE_LIST_NAME,
    OLD_PRICE,
    PRODUCT_UOM_CODE,
    LIST_LINE_ID, 
    LEVEL, 
    SEQN
FROM
    (
        SELECT
            LIST_HEADER_ID,
            INVENTORY_ITEM_ID,
            START_DATE_ACTIVE,
            END_DATE_ACTIVE,
            PRICE_LIST_NAME,
            OLD_PRICE,
            PRODUCT_UOM_CODE,
            LIST_LINE_ID,
            ROW_NUMBER() OVER(
                PARTITION BY LIST_HEADER_ID, INVENTORY_ITEM_ID
                ORDER BY
                    END_DATE_ACTIVE DESC
            ) SEQN,
            LEVEL
        FROM
-- price list based on item number
            (
                SELECT
                    QPLH.LIST_HEADER_ID,
                    MTL.INVENTORY_ITEM_ID,
                    QPLL.START_DATE_ACTIVE,
                    QPLL.END_DATE_ACTIVE,
                    QPLTL.NAME     PRICE_LIST_NAME,
    --ROW_NUMBER() OVER (PARTITION BY QPLH.LIST_HEADER_ID, MTL.INVENTORY_ITEM_ID INVENTORY_ITEM_ID SEQN,
                    QPLL.OPERAND   OLD_PRICE,
                   --
                                                --                                                                apps.xx_order_item_demand_pkg.uom_conv 
                                                --                                                                (ool.order_quantity_uom,
                                                --                                                                qppr.product_uom_code,
                                                --                                                                mtl.inventory_item_id) )old_price   
                    QPPR.PRODUCT_UOM_CODE,
                    QPLL.LIST_LINE_ID,
                    'ITEM' LEVEL
                FROM
                    QP_LIST_HEADERS_B QPLH,
                    QP_LIST_LINES QPLL,
                    QP_PRICING_ATTRIBUTES QPPR,
                    MTL_SYSTEM_ITEMS_B MTL,
                    QP_LIST_HEADERS_TL QPLTL
                WHERE
                    QPLH.LIST_HEADER_ID = QPLL.LIST_HEADER_ID
                    AND QPLL.END_DATE_ACTIVE IS NOT NULL
                    AND QPPR.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
                    AND QPPR.LIST_LINE_ID = QPLL.LIST_LINE_ID
                    AND INT(QPPR.PRODUCT_ATTR_VALUE) = MTL.INVENTORY_ITEM_ID
                    AND MTL.ORGANIZATION_ID = 124
                    AND '202005' BETWEEN  DATE_FORMAT(QPLL.START_DATE_ACTIVE, 'yyyyMM') AND  DATE_FORMAT(QPLL.END_DATE_ACTIVE, 'yyyyMM')   -- Only get price valid in May 2020  -- Only get price valid in May 2020
                    AND QPLL.LIST_LINE_TYPE_CODE IN (
                        'PLL',
                        'PBH'
                    )
                    AND QPPR.PRICING_PHASE_ID = 1
                    AND QPPR.QUALIFICATION_IND IN (
                        4,
                        6,
                        20,
                        22
                    )
                    AND QPLL.PRICING_PHASE_ID = 1
                    AND QPLL.QUALIFICATION_IND IN (
                        4,
                        6,
                        20,
                        22
                    )
    --and a.list_header_id = ool.price_list_id
                    AND QPLL.END_DATE_ACTIVE IS NOT NULL
                    AND QPPR.PRODUCT_ATTR_VALUE IS NOT NULL
                    AND QPLH.LIST_TYPE_CODE = 'PRL'
                    AND QPLH.LIST_HEADER_ID = QPLTL.LIST_HEADER_ID
                    AND QPLTL.LANGUAGE = 'US'
     --   AND REGEXP_EXTRACT( QPPR.PRODUCT_ATTR_VALUE, '^[[:digit:]]+$')
                UNION -- Pricelists based on Variant
                SELECT
                    QLHB.LIST_HEADER_ID,
                    MSI1.INVENTORY_ITEM_ID,
                    QPLL.START_DATE_ACTIVE,
                    QPLL.END_DATE_ACTIVE,
                    QPLTL.NAME     PRICE_LIST_NAME,
                    QPLL.OPERAND   OLD_PRICE,
                    QPPR.PRODUCT_UOM_CODE,
                    QPLL.LIST_LINE_ID,
                    'VARIANT' LEVEL
                FROM
                    QP_LIST_HEADERS_B QLHB,
                    QP_LIST_LINES_V QPLL,
                    QP_PRICING_ATTRIBUTES QPPR,
                    MTL_ITEM_CATEGORIES MIC,
                    MTL_SYSTEM_ITEMS_B MSI1,
                    QP_LIST_HEADERS_TL QPLTL
                WHERE
                    1 = 1
                    AND QPLL.LIST_HEADER_ID = QLHB.LIST_HEADER_ID
                    AND QPLL.LIST_HEADER_ID = QPLTL.LIST_HEADER_ID
                    AND QPLTL.LANGUAGE = 'US'
                    AND UPPER(QLHB.ACTIVE_FLAG) = 'Y'
                    AND QLHB.LIST_TYPE_CODE = 'PRL'
                    AND QPLL.PRODUCT_ATTRIBUTE_CONTEXT = QPPR.PRODUCT_ATTRIBUTE_CONTEXT
                    AND QPPR.LIST_LINE_ID = QPLL.LIST_LINE_ID
                    AND QPPR.PRICING_PHASE_ID = QPLL.PRICING_PHASE_ID
                    AND QPLL.PRODUCT_ATTRIBUTE = QPPR.PRODUCT_ATTRIBUTE
                    AND QPPR.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
                    AND QLHB.LIST_HEADER_ID = QPPR.LIST_HEADER_ID
                    AND QPLL.PRODUCT_ID = MIC.CATEGORY_ID
                    AND MIC.ORGANIZATION_ID = 124
                    AND QPLL.END_DATE_ACTIVE IS NOT NULL
                    AND MIC.INVENTORY_ITEM_ID = MSI1.INVENTORY_ITEM_ID
    
                    AND '202005' BETWEEN  DATE_FORMAT(QPLL.START_DATE_ACTIVE, 'yyyyMM') AND  DATE_FORMAT(QPLL.END_DATE_ACTIVE, 'yyyyMM')   -- Only get price valid in May 2020
                    AND MIC.ORGANIZATION_ID = MSI1.ORGANIZATION_ID
                    AND (QPPR.PRICING_ATTRIBUTE_ID = C1_PRICING_ATTRIBUTE_ID
                        OR ( QPPR.PRICING_ATTRIBUTE_ID = C2_PRICING_ATTRIBUTE_ID
                             AND C3_LIST_LINE_ID IS NULL ))
--  --  AND REGEXP_EXTRACT( QPLL.PRODUCT_ID, '^[[:digit:]]+$')
            )
    )
WHERE
    1 = 1
    AND SEQN = 1
  """)
PRIOR_PRICE.createOrReplaceTempView("PRIOR_PRICE")
PRIOR_PRICE.persist(StorageLevel.DISK_ONLY)
PRIOR_PRICE.count()

# COMMAND ----------

UOM_CONVERSION_ITEM = spark.sql("""
  SELECT INVENTORY_ITEM_ID, UOM_CODE, CONVERSION_RATE FROM MTL_UOM_CONVERSIONS
    WHERE INVENTORY_ITEM_ID <> 0
    AND (DISABLE_DATE IS NULL OR DISABLE_DATE > CURRENT_DATE)
""")

UOM_CONVERSION_ITEM.createOrReplaceTempView("UOM_CONVERSION_ITEM")
UOM_CONVERSION_ITEM.cache()
UOM_CONVERSION_ITEM.count()


UOM_CONVERSION_DEFAULT = spark.sql("""
  SELECT UOM_CODE, CONVERSION_RATE FROM MTL_UOM_CONVERSIONS
    WHERE INVENTORY_ITEM_ID = 0
    AND (DISABLE_DATE IS NULL OR DISABLE_DATE > CURRENT_DATE)
""")

UOM_CONVERSION_DEFAULT.createOrReplaceTempView("UOM_CONVERSION_DEFAULT")
#UOM_CONVERSION_DEFAULT.cache()
UOM_CONVERSION_DEFAULT.persist(StorageLevel.DISK_ONLY)
UOM_CONVERSION_DEFAULT.count()


# COMMAND ----------

W_SALES_INVOICE_LINE_F = spark.sql("""
  SELECT  
	CONCAT('INVOICE-', COALESCE(STRING(INT(RCTLA.CUSTOMER_TRX_LINE_ID)), '')) INTEGRATION_ID,
	DATE_FORMAT(RCTLA.CREATION_DATE, "yyyy-MM-dd HH:mm:ss.SSS") CREATED_ON_DT,
	DATE_FORMAT(RCTLA.LAST_UPDATE_DATE, "yyyy-MM-dd HH:mm:ss.SSS") CHANGED_ON_DT,
	INT(RCTLA.CREATED_BY) CREATED_BY_ID,
	INT(RCTLA.LAST_UPDATED_BY) CHANGED_BY_ID,
	DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') INSERT_DT, 
	DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') UPDATE_DT,
	'EBS' DATASOURCE_NUM_ID,
	DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmssSSS') LOAD_BATCH_ID,
	BS.NAME BATCH_SOURCE_NAME,
	'N' DELETE_FLG,
	CASE 
		WHEN NVL(RCTLA.INTERFACE_LINE_ATTRIBUTE11, '0') <> '0' 
		THEN 'Y' 
		ELSE 'N' 
	END DISCOUNT_LINE_FLG,
	RCTA.INVOICE_CURRENCY_CODE DOC_CURR_CODE,
	'' FUND_NAME,
	'' FUND_NUMBER,
	'' FUND_STATUS,
	RCTLA.LINE_NUMBER INVOICE_ITEM,
	RCTA.TRX_NUMBER INVOICE_NUM,
	GSOB.CURRENCY_CODE LOC_CURR_CODE,
	CASE 
		WHEN RCTA.INVOICE_CURRENCY_CODE  = GSOB.CURRENCY_CODE THEN 1
		ELSE RCTA.EXCHANGE_RATE
	END LOC_EXCHANGE_RATE,
	'' OFFER_CODE,
	OOHA.CUST_PO_NUMBER PURCH_ORDER_NUM,
	CASE 
		WHEN 
			CASE 
				WHEN RCTLA.LINE_TYPE IN ('LINE', 'FREIGHT') 
				THEN RCTLA.INTERFACE_LINE_CONTEXT 
				ELSE RCTLA1.INTERFACE_LINE_CONTEXT
			END = 'ORDER ENTRY'
		THEN 
			CASE 
				WHEN RCTLA.LINE_TYPE IN ('LINE', 'FREIGHT')  
				THEN 
					CASE
						WHEN RCTLA.SALES_ORDER_LINE IS NOT NULL
						THEN RCTLA.SALES_ORDER_LINE
						ELSE LON.SALES_ORDER_LINE
					END 
				ELSE RCTLA1.SALES_ORDER_LINE
			END
		ELSE NULL 
	END SALES_ORDER_ITEM,
	CASE 
		WHEN 
			CASE 
				WHEN RCTLA.LINE_TYPE IN ('LINE', 'FREIGHT') 
				THEN RCTLA.INTERFACE_LINE_CONTEXT 
				ELSE RCTLA1.INTERFACE_LINE_CONTEXT
			END = 'ORDER ENTRY'
		THEN String(OOLA.SHIPMENT_NUMBER)
		ELSE NULL 
	END  SALES_ORDER_ITEM_DETAIL_NUM,
	CASE 
		WHEN
			CASE 
				WHEN RCTLA.LINE_TYPE IN ('LINE', 'FREIGHT') 
				THEN RCTLA.INTERFACE_LINE_CONTEXT 
				ELSE RCTLA1.INTERFACE_LINE_CONTEXT
			END IN ('ORDER ENTRY' , 'INTERCOMPANY') 
		THEN 
			CASE 
				WHEN RCTLA.LINE_TYPE IN ('LINE', 'FREIGHT')
			THEN   
				CASE 
					WHEN RCTLA.SALES_ORDER IS NOT NULL
					THEN RCTLA.SALES_ORDER
					ELSE LON.SALES_ORDER
				END
			ELSE RCTLA1.SALES_ORDER
		END
		ELSE NULL 
	END SALES_ORDER_NUM,
	CASE 
		WHEN 
			CASE WHEN RCTLA.LINE_TYPE IN ('LINE', 'FREIGHT') 
				THEN RCTLA.INTERFACE_LINE_CONTEXT 
				ELSE RCTLA1.INTERFACE_LINE_CONTEXT
			END ='ORDER ENTRY'
		THEN 
			CASE
				WHEN RCTLA.LINE_TYPE='LINE'
				THEN RCTLA.INTERFACE_LINE_ATTRIBUTE6
				ELSE
				RCTLA1.INTERFACE_LINE_ATTRIBUTE6
			END
		ELSE NULL
	END SALES_ORDLN_ID,
	RCTLA.UOM_CODE SALES_UOM_CODE,
	MSIB.ATTRIBUTE15 ANS_STD_UOM,
	'INVOICE' TRANSACTION_TYPE,
	'' UTILIZATION_TYPE,
	CASE
		WHEN RCTLA_VW.TOTAL_INVOICED_AMOUNT <> 0 
			AND (RCTLA.INTERFACE_LINE_CONTEXT = 'ORDER ENTRY'
			OR RCTLA.INTERFACE_LINE_CONTEXT IS NULL)
			AND ARAA_VW.EARNED_DISCOUNT_TAKEN <> 0
		THEN 
			ROUND(((((RCTLA.QUANTITY_INVOICED * 
			RCTLA.UNIT_SELLING_PRICE) / RCTLA_VW.TOTAL_INVOICED_AMOUNT)) * 
			ARAA_VW.EARNED_DISCOUNT_TAKEN),2)
		ELSE 0
	END EARNED_DISCOUNT,
	CASE 
		WHEN RCTLA.LINE_TYPE IN ('TAX','FREIGHT' )
		THEN 0
		WHEN RCTTA.TYPE IN ( 'DM') 
		THEN  0     
		WHEN RCTTA.TYPE IN ( 'CM') 
		THEN -1.0 * RCTLA.QUANTITY_CREDITED        
		WHEN NVL(RCTLA.INTERFACE_LINE_ATTRIBUTE11,0) <> '0' 
		THEN 0
		WHEN RCTLA.UNIT_SELLING_PRICE < 0 AND RCTLA.QUANTITY_INVOICED > 0 
		THEN -1.0 * RCTLA.QUANTITY_INVOICED
		ELSE RCTLA.QUANTITY_INVOICED
	END INVOICED_QTY,
	CASE 
		WHEN RCTLA.LINE_TYPE IN ('LINE', 'CB')
		THEN
			CASE 
				WHEN RCTTA.TYPE = 'CM'
				THEN -1 * RCTLA.EXTENDED_AMOUNT
				ELSE RCTLA.EXTENDED_AMOUNT
			END 
		ELSE 0 
	END NET_AMT,
	CASE
		WHEN RCTLA_VW.TOTAL_INVOICED_AMOUNT <> 0
			AND (RCTLA.INTERFACE_LINE_CONTEXT = 'ORDER ENTRY'
			OR RCTLA.INTERFACE_LINE_CONTEXT IS NULL)
			AND ARAA_VW.UNEARNED_DISCOUNT_TAKEN <> 0
		THEN ROUND(((((RCTLA.QUANTITY_INVOICED * RCTLA.UNIT_SELLING_PRICE) / RCTLA_VW.TOTAL_INVOICED_AMOUNT)) * ARAA_VW.UNEARNED_DISCOUNT_TAKEN),2)
		ELSE 0
	END	UNEARNED_DISCOUNT,
	FLOAT(NULL) VOL_DISCOUNT_ACCRUAL,
	FLOAT(NULL) VOL_DISCOUNT_ACCRUAL_LE_CURR,
	FLOAT(NULL) CONT_REBATE_ACCRUAL,
	FLOAT(NULL) CONT_REBATE_ACCRUAL_LE_CURR,
	FLOAT(NULL) BOGO_ACCRUAL,
	FLOAT(NULL) BOGO_ACCRUAL_LE_CURR,
	FLOAT(NULL) TPR_ACCRUAL,
	FLOAT(NULL) TPR_ACCRUAL_LE_CURR,
	FLOAT(NULL) COOP_ADV_FEE_ACCRUAL,
	FLOAT(NULL) COOP_ADV_FEE_ACCRUAL_LE_CURR,
	DATE_FORMAT(LON.ACTUAL_SHIPMENT_DATE, "yyyy-MM-dd") ACTUAL_SHIPMENT_DT,
	INT(RCTA.BILL_TO_CUSTOMER_ID) CUSTOMER_ACCOUNT_ID,
	INT(CUST_ACCT_SITE_ID) CUSTOMER_SHIP_TO_LOC_ID, 
	0 FUND_CATEGORY_ID,
	COALESCE(
		CASE 
			WHEN RCTLA. LINE_TYPE = 'LINE' THEN INT(RCTLA.WAREHOUSE_ID)
			ELSE INT(RCTLA1.WAREHOUSE_ID)
		END,
		OOLA.SHIP_FROM_ORG_ID
	) INVENTORY_ORG_ID,
	DATE_FORMAT(RCTA.TRX_DATE, "yyyy-MM-dd") INVOICED_ON_DT,
	INT(RCTLA.ORG_ID) OPERATING_UNIT_ORG_ID,
	CASE 
		WHEN OOLA.SOURCE_TYPE_CODE ='INTERNAL' 
		THEN 
			CASE 
				WHEN OOHA.ORDER_SOURCE_ID = 10
				THEN 
					CONCAT('SALES_ORDLNS' ,'-' , COALESCE (OOLA.LINE_CATEGORY_CODE, ''),
					'-', COALESCE (INT(OOHA.ORDER_TYPE_ID), ''),
					'-INTERNAL-SELF SHIP')
				ELSE 
					CONCAT('SALES_ORDLNS' , '-' , COALESCE (OOLA.LINE_CATEGORY_CODE, ''),
					'-' , COALESCE (INT(OOHA.ORDER_TYPE_ID), ''),
					'-EXTERNAL-SELF SHIP')
			END
		ELSE 
			CASE 
				WHEN OOHA.ORDER_SOURCE_ID = 10
				THEN
					CONCAT('SALES_ORDLNS' , '-' , COALESCE (OOLA.LINE_CATEGORY_CODE, ''),
					'-' , COALESCE (INT(OOHA.ORDER_TYPE_ID), ''),
					'-INTERNAL-DROP SHIP')
				ELSE
					CONCAT('SALES_ORDLNS' , '-' , COALESCE (OOLA.LINE_CATEGORY_CODE, ''),
					'-' , COALESCE (INT(OOHA.ORDER_TYPE_ID), ''),
					'-EXTERNAL-DROP SHIP')
			END 
	END ORDER_XACT_TYPE_ID,
	LON.SALES_ORDER_DATE ORDERED_ON_DT,
	CASE 
		WHEN RCTLA.LINE_TYPE = 'LINE' 
		THEN INT(RCTLA.INVENTORY_ITEM_ID)
		ELSE INT(RCTLA1.INVENTORY_ITEM_ID)
	END PRODUCT_ID,
	INT(RCTLA.ORG_ID) SALES_ORG_ID,
	DATE_FORMAT(LON.SCHEDULE_SHIP_DATE,"yyyy-MM-dd") SCHEDULED_SHIP_DT , 
	CONCAT(
		'SALES_IVCLNS' , 
		'-' , COALESCE(INT(RCTA.CUST_TRX_TYPE_ID), ''),
		'-' , COALESCE (INT(RCTLA.ORG_ID), ''),
		'-', COALESCE(
			(CASE 
				WHEN RCTLA.LINE_TYPE IN ('LINE', 'FREIGHT') 
				THEN RCTLA.INTERFACE_LINE_CONTEXT
				ELSE RCTLA1.INTERFACE_LINE_CONTEXT
			END),
			''
		)  
	) XACT_TYPE_ID,
	RCTA.EXCHANGE_DATE EXCHANGE_DATE,
    --OOLA.UNIT_LIST_PRICE CURRENT_UNIT_PRICE,
    RCTLA.UNIT_SELLING_PRICE CURRENT_UNIT_PRICE,
   --  PRIOR_PRICE.OLD_PRICE ,
    PRIOR_PRICE.OLD_PRICE * 
    --PRIOR_PRICE.PRODUCT_UOM_CODE    

             NVL(UOM_CONVERSION_ITEM_TO.CONVERSION_RATE,UOM_CONVERSION_DEFAULT_TO.CONVERSION_RATE) / 
             NVL(UOM_CONVERSION_ITEM_FROM.CONVERSION_RATE,UOM_CONVERSION_DEFAULT_FROM.CONVERSION_RATE) PRIOR_UNIT_PRICE
  FROM RA_CUSTOMER_TRX_ALL RCTA
  INNER JOIN RA_CUSTOMER_TRX_LINES_ALL_INC RCTLA ON RCTA.CUSTOMER_TRX_ID = RCTLA.CUSTOMER_TRX_ID
  LEFT JOIN RA_BATCH_SOURCES_ALL BS ON RCTA.BATCH_SOURCE_ID = BS.BATCH_SOURCE_ID AND RCTA.ORG_ID = BS.ORG_ID
  LEFT JOIN OE_ORDER_LINES_ALL OOLA ON INT(RCTLA.INTERFACE_LINE_ATTRIBUTE6) = OOLA.LINE_ID
  LEFT JOIN OE_ORDER_HEADERS_ALL OOHA ON OOLA.HEADER_ID = OOHA.HEADER_ID
  LEFT JOIN GL_SETS_OF_BOOKS GSOB ON RCTA.SET_OF_BOOKS_ID = GSOB.SET_OF_BOOKS_ID
  LEFT JOIN MTL_SYSTEM_ITEMS_B MSIB ON OOLA.INVENTORY_ITEM_ID = MSIB.Inventory_item_id AND OOLA.SHIP_FROM_ORG_ID = MSIB.Organization_id 
  INNER JOIN RA_CUST_TRX_TYPES_ALL RCTTA ON RCTA.CUST_TRX_TYPE_ID = RCTTA.CUST_TRX_TYPE_ID AND RCTA.ORG_ID = RCTTA.ORG_ID 
  LEFT JOIN RA_CUSTOMER_TRX_LINES_ALL_INC RCTLA1 ON RCTLA.LINK_TO_CUST_TRX_LINE_ID = RCTLA1.CUSTOMER_TRX_LINE_ID
  LEFT OUTER JOIN LINE_ORD_NO LON ON CONCAT(COALESCE (RCTLA.CUSTOMER_TRX_ID, ''),	'-' , COALESCE (RCTLA.LINE_NUMBER, '')) = LON.LINE_KEY
  LEFT OUTER JOIN AR_RECEIVABLE_APPS_ALL_VW ARAA_VW ON RCTLA.CUSTOMER_TRX_ID = ARAA_VW.APPLIED_CUSTOMER_TRX_ID
  LEFT OUTER JOIN RA_CUSTOMER_TRX_LINES_ALL_VW RCTLA_VW ON RCTLA.CUSTOMER_TRX_ID = RCTLA_VW.CUSTOMER_TRX_ID
  LEFT OUTER JOIN CUSTOMER_LOC_USE_D ON RCTA.SHIP_TO_SITE_USE_ID = CUSTOMER_LOC_USE_D.SITE_USE_ID
  LEFT JOIN PRIOR_PRICE
    ON OOLA.PRICE_LIST_ID = PRIOR_PRICE.LIST_HEADER_ID
    AND OOLA.INVENTORY_ITEM_ID = PRIOR_PRICE.INVENTORY_ITEM_ID
  LEFT JOIN UOM_CONVERSION_ITEM UOM_CONVERSION_ITEM_FROM
    ON OOLA.INVENTORY_ITEM_ID = UOM_CONVERSION_ITEM_FROM.INVENTORY_ITEM_ID
    AND PRIOR_PRICE.PRODUCT_UOM_CODE = UOM_CONVERSION_ITEM_FROM.UOM_CODE
  LEFT JOIN UOM_CONVERSION_DEFAULT UOM_CONVERSION_DEFAULT_FROM
    ON PRIOR_PRICE.PRODUCT_UOM_CODE = UOM_CONVERSION_DEFAULT_FROM.UOM_CODE
  LEFT JOIN UOM_CONVERSION_ITEM UOM_CONVERSION_ITEM_TO
    ON OOLA.INVENTORY_ITEM_ID   =  UOM_CONVERSION_ITEM_TO.INVENTORY_ITEM_ID
    AND OOLA.ORDER_QUANTITY_UOM = UOM_CONVERSION_ITEM_TO.UOM_CODE
  LEFT JOIN UOM_CONVERSION_DEFAULT UOM_CONVERSION_DEFAULT_TO
    ON OOLA.ORDER_QUANTITY_UOM = UOM_CONVERSION_DEFAULT_TO.UOM_CODE  
    
 WHERE	
	(RCTLA.INTERFACE_LINE_CONTEXT IN (
      'ORDER ENTRY', 'INTERCOMPANY','INVOICE_CONVERSION','CR_AR_ADJ_CONVERSION',
      'DR_AR_ADJ_CONVERSION','GLOBAL_PROCUREMENT', 'MICROFLEX_INVOICE_CONVERSION'
	)
	OR RCTLA.INTERFACE_LINE_CONTEXT IS NULL)
    --AND RCTLA.CUSTOMER_TRX_LINE_ID = 22830946
""")

W_SALES_INVOICE_LINE_F.createOrReplaceTempView("W_SALES_INVOICE_LINE_F")
#W_SALES_INVOICE_LINE_F.cache()
W_SALES_INVOICE_LINE_F.persist(StorageLevel.DISK_ONLY)
W_SALES_INVOICE_LINE_F.count()

# COMMAND ----------

count = W_SALES_INVOICE_LINE_F.select("INTEGRATION_ID").count()
countDistinct = W_SALES_INVOICE_LINE_F.select("INTEGRATION_ID").distinct().count()

print(count)
print(countDistinct)

if(count != countDistinct):
  message = 'Mismatch in count of total records ({0}) and distinct count of primary keys ({1}) in {2} '.format(count, countDistinct, tableName)
  raise Exception(message)

# COMMAND ----------

W_SALES_INVOICE_LINE_F.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

LINE_ORD_NO.unpersist()
AR_RECEIVABLE_APPS_ALL_VW.unpersist()
RA_CUSTOMER_TRX_LINES_ALL_VW.unpersist()
CUSTOMER_LOC_USE_D.unpersist()
QP_LIST_LINES_V.unpersist()
PRIOR_PRICE.unpersist()
UOM_CONVERSION_ITEM.unpersist()
UOM_CONVERSION_DEFAULT.unpersist()
W_SALES_INVOICE_LINE_F.unpersist()

# COMMAND ----------


