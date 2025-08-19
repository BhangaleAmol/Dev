# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_tm.pointofsales

# COMMAND ----------

# PARAMETERS
source_name = "EBS"

# COMMAND ----------

# LOAD DATASETS
file_name = 's_trademanagement.clean_territory_assignments.dlt'
file_path = get_file_path(temp_folder, file_name, target_container, target_storage) 
terr_df = spark.read.format('delta').load(file_path)

grainger = spark.table('amazusftp1.grainger_pos')

# COMMAND ----------

# SAMPLING
if sampling:
  terr_df = terr_df.limit(10)
  grainger = grainger.limit(10)

# COMMAND ----------

# CREATE VIEWS
terr_df.createOrReplaceTempView('terr_df')
grainger.createOrReplaceTempView('grainger')

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

OZF_CONV=spark.sql("""
SELECT
  CONV.CODE_CONVERSION_ID,
  CONV.LAST_UPDATE_DATE,
  CONV.LAST_UPDATED_BY,
  CONV.LAST_UPDATE_BY,
  CONV.CREATION_DATE,
  CONV.CREATED_BY,
  CONV.LAST_UPDATE_LOGIN,
  CONV.ORG_ID,
  CONV.PARTY_ID,
  CONV.CUST_ACCOUNT_ID,
  CONV.CODE_CONVERSION_TYPE,
  CONV.EXTERNAL_CODE,
  CONV.INTERNAL_CODE,
  CONV.DESCRIPTION,
  CONV.START_DATE_ACTIVE,
  CONV.END_DATE_ACTIVE,
  INT.PRODUCTCODE AS INTERNAL_CODE_ITEM,
  EXT.PRODUCTCODE AS EXTERNAL_CODE_ITEM
FROM
  EBS.OZF_CODE_CONVERSIONS_ALL CONV
  LEFT JOIN S_CORE.PRODUCT_EBS INT ON CONV.INTERNAL_CODE = INT.ITEMID
  LEFT JOIN S_CORE.PRODUCT_EBS EXT ON CONV.EXTERNAL_CODE = EXT.ITEMID
""")

OZF_CONV.createOrReplaceTempView('OZF_CONV')

# COMMAND ----------

PROD_CONV=spark.sql("""
SELECT distinct
  c50.external_code external_code,
  nvl(c5a.internal_code_item, c5b.internal_code_item) item
FROM
  (
    SELECT
      distinct external_code
    FROM
      OZF_CONV
    WHERE
      code_conversion_type = 'OZF_PRODUCT_CODES'
      AND org_id = '82'
      AND nvl(end_date_active, current_date) >= current_date
  ) c50
  LEFT JOIN (
    SELECT
      external_code,
      internal_code_item
    FROM
      OZF_CONV
    WHERE
      code_conversion_type = 'OZF_PRODUCT_CODES'
      AND org_id = '82'
      AND nvl(end_date_active, current_date) >= current_date --and external_code= '11250180W-GRA'
      AND party_id IS NULL
  ) c5b ON c50.external_code = c5b.external_code
  LEFT JOIN (
    SELECT
      external_code,
      internal_code_item
    FROM
      OZF_CONV
    WHERE
      code_conversion_type = 'OZF_PRODUCT_CODES'
      AND org_id = '82'
      AND nvl(end_date_active, current_date) >= current_date --and external_code= '11250180W-GRA'
      AND party_id = '15098'
  ) c5a ON c50.external_Code = c5a.external_code
    """)

PROD_CONV.createOrReplaceTempView('PROD_CONV')

# COMMAND ----------

ZIP3=spark.sql("""
select
  min(territoryid) territoryid,
  trim(zip3) zip3,
  vertical,
  min(salesregion) sales_region,
  min(userid) user_id
from
  terr_df
where
  zip3 is not null
  and TerritoryID not like '%HSE'
  and scenario = 'DEFAULT'
group by
  trim(zip3),
  vertical
""")

ZIP3.createOrReplaceTempView('ZIP3')

# COMMAND ----------

ZIP0=spark.sql("""
select
  territoryid,
  vertical,
  salesregion,
  min(userid) user_id
from
  terr_df
where
  zip3 is null
  and TerritoryID not like '%HSE'
  and scenario = 'DEFAULT'
group by
  territoryid,
  vertical,
  salesregion
""")

ZIP0.createOrReplaceTempView('ZIP0')

# COMMAND ----------

TERR_DEFAULT=spark.sql("""
select
  *
from
  terr_df
where
  scenario = 'DEFAULT'
""")

TERR_DEFAULT.createOrReplaceTempView('TERR_DEFAULT')

# COMMAND ----------

SECONDORG_ZIP3=spark.sql("""
select
  min(territoryid) territoryid,
  trim(zip3) zip3,
  vertical,
  min(salesregion) sales_region,
  min(userid) user_id
from
  terr_df
where
  zip3 is not null
  and TerritoryID not like '%HSE'
  and scenario = 'ORG2'
group by
  trim(zip3),
  vertical
""")

SECONDORG_ZIP3.createOrReplaceTempView('SECONDORG_ZIP3')

# COMMAND ----------

THIRDORG_ZIP3=spark.sql("""
select
  min(territoryid) territoryid,
  trim(zip3) zip3,
  vertical,
  min(salesregion) sales_region,
  min(userid) user_id
from
  terr_df
where
  zip3 is not null
  and TerritoryID not like '%HSE'
  and scenario = 'ORG3'
group by
  trim(zip3),
  vertical
""")

THIRDORG_ZIP3.createOrReplaceTempView('THIRDORG_ZIP3')

# COMMAND ----------

CUST_OVERRIDE=spark.sql("""
SELECT
  DISTINCT CustomerID CUSTID,
  MAPTOTERRITORY
FROM
  smartsheets.qvpos_customer_territory_override
WHERE
  MaptoTerritory IS NOT NULL
""")

CUST_OVERRIDE.createOrReplaceTempView('CUST_OVERRIDE')

# COMMAND ----------

SECONDORG_CUST_OVERRIDE=spark.sql("""
SELECT
  DISTINCT CustomerID CUSTID,
  MAPTOTERRITORY
FROM
  smartsheets.qvpos_customer_territory_override
WHERE
  MapTo2ndOrgTerritory IS NOT NULL
""")

SECONDORG_CUST_OVERRIDE.createOrReplaceTempView('SECONDORG_CUST_OVERRIDE')

# COMMAND ----------

THIRDORG_CUST_OVERRIDE=spark.sql("""
SELECT
  DISTINCT CustomerID CUSTID,
  MAPTOTERRITORY
FROM
  smartsheets.qvpos_customer_territory_override
WHERE
  MapToOneS_Territory IS NOT NULL
""")

THIRDORG_CUST_OVERRIDE.createOrReplaceTempView('THIRDORG_CUST_OVERRIDE')

# COMMAND ----------

PROD_OVERRIDE=spark.sql("""
SELECT
  *
FROM
  smartsheets.qvpos_product_territory_override
WHERE
  MaptoTerritory IS NOT NULL
""")

PROD_OVERRIDE.createOrReplaceTempView('PROD_OVERRIDE')

# COMMAND ----------

SECONDORG_PROD_OVERRIDE=spark.sql("""
SELECT
  *
FROM
  smartsheets.qvpos_product_territory_override
WHERE
  MapTo2ndOrgTerritory IS NOT NULL
""")

SECONDORG_PROD_OVERRIDE.createOrReplaceTempView('SECONDORG_PROD_OVERRIDE')

# COMMAND ----------

THIRDORG_PROD_OVERRIDE=spark.sql("""
SELECT
  *
FROM
  smartsheets.qvpos_product_territory_override
WHERE
  MapToOneS_Territory IS NOT NULL
""")

THIRDORG_PROD_OVERRIDE.createOrReplaceTempView('THIRDORG_PROD_OVERRIDE')

# COMMAND ----------

main_stage=spark.sql("""
SELECT
COMPANY,
VERTICAL,
DISTID,
DISTRIBUTOR,
CITY,
STATE,
SHIP_ZIP_CODE,
SELLING_PLANT_DESCRIPTION,
SUPPLIER_STOCK_NO,
BILL_DATE,
CY,
Month,
SUM(UNITS_SOLD) UNITS_SOLD,
UNITPRICE,
UOM_NAME,
SUM(COGS) COGS,
GIS_BRAND_LEVEL_1,
GIS_BRAND_LEVEL_2,
GIS_BRAND_LEVEL_3
FROM
grainger
GROUP BY
COMPANY,
VERTICAL,
DISTID,
DISTRIBUTOR,
CITY,
STATE,
SHIP_ZIP_CODE,
SELLING_PLANT_DESCRIPTION,
SUPPLIER_STOCK_NO,
BILL_DATE,
CY,
Month,
UNITPRICE,
UOM_NAME,
GIS_BRAND_LEVEL_1,
GIS_BRAND_LEVEL_2,
GIS_BRAND_LEVEL_3
""")

main_stage.createOrReplaceTempView('main_stage')

# COMMAND ----------

main = spark.sql("""
SELECT
  cast(NULL as string) AS createdBy,
  GRAINGER.BILL_DATE AS createdOn,
  cast(NULL AS STRING) AS modifiedBy,
  cast(NULL AS timestamp) AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  ACCOUNT.accountid accountId,
  NULL AS ansellAcquisitionCost,
  NULL AS ansellAgreementName,
  CAST(NVL(GRAINGER.UNITPRICE,0) AS decimal(22,7))  ansellContractPrice,
  NULL AS ansellCorrectedAgreementName,
  NULL AS approvalDate,
  NULL AS batchNumber,
  NULL AS batchStatusCode,
  NULL AS batchType,
  NULL AS claimComments,
  SUM(GRAINGER.UNITS_SOLD) AS claimedQuantity,
   sum( ((CASE 
WHEN UPPER(GRAINGER.UOM_NAME) = 'BD' THEN PROD.piecesInBundle
WHEN UPPER(GRAINGER.UOM_NAME) = 'BG' THEN PROD.piecesInBag
WHEN UPPER(GRAINGER.UOM_NAME) = 'BX' THEN PROD.piecesInBox
WHEN UPPER(GRAINGER.UOM_NAME) = 'CA' THEN PROD.piecesInCase
WHEN UPPER(GRAINGER.UOM_NAME) = 'CT' THEN PROD.piecesInCarton
WHEN UPPER(GRAINGER.UOM_NAME) = 'DP' THEN PROD.piecesInDisplayDispenser
WHEN UPPER(GRAINGER.UOM_NAME) = 'DZ' THEN PROD.piecesInDozen
WHEN UPPER(GRAINGER.UOM_NAME) = 'EA' THEN PROD.piecesInEach
WHEN UPPER(GRAINGER.UOM_NAME) = 'GR' THEN PROD.piecesInGross
WHEN UPPER(GRAINGER.UOM_NAME) = 'PC' THEN 1
WHEN UPPER(GRAINGER.UOM_NAME) = 'PK' THEN PROD.piecesInPack
WHEN UPPER(GRAINGER.UOM_NAME) = 'PR' THEN 2
WHEN UPPER(GRAINGER.UOM_NAME) = 'RL' THEN PROD.piecesInRoll
END ) * GRAINGER.UNITS_SOLD)

 / PROD.piecesInCase) claimedQuantityCase,
  sum( ((CASE 
WHEN UPPER(GRAINGER.UOM_NAME) = 'BD' THEN PROD.piecesInBundle
WHEN UPPER(GRAINGER.UOM_NAME) = 'BG' THEN PROD.piecesInBag
WHEN UPPER(GRAINGER.UOM_NAME) = 'BX' THEN PROD.piecesInBox
WHEN UPPER(GRAINGER.UOM_NAME) = 'CA' THEN PROD.piecesInCase
WHEN UPPER(GRAINGER.UOM_NAME) = 'CT' THEN PROD.piecesInCarton
WHEN UPPER(GRAINGER.UOM_NAME) = 'DP' THEN PROD.piecesInDisplayDispenser
WHEN UPPER(GRAINGER.UOM_NAME) = 'DZ' THEN PROD.piecesInDozen
WHEN UPPER(GRAINGER.UOM_NAME) = 'EA' THEN PROD.piecesInEach
WHEN UPPER(GRAINGER.UOM_NAME) = 'GR' THEN PROD.piecesInGross
WHEN UPPER(GRAINGER.UOM_NAME) = 'PC' THEN 1
WHEN UPPER(GRAINGER.UOM_NAME) = 'PK' THEN PROD.piecesInPack
WHEN UPPER(GRAINGER.UOM_NAME) = 'PR' THEN 2
WHEN UPPER(GRAINGER.UOM_NAME) = 'RL' THEN PROD.piecesInRoll
END ) * GRAINGER.UNITS_SOLD)

/

(CASE 
WHEN PROD.primarySellingUom IN ('BD','BUNDLE') THEN PROD.piecesInBundle
WHEN PROD.primarySellingUom IN ( 'BG','BAG') THEN PROD.piecesInBag
WHEN PROD.primarySellingUom IN ('BX','BOX') THEN PROD.piecesInBox
WHEN PROD.primarySellingUom  IN ('CA','CASE') THEN PROD.piecesInCase
WHEN PROD.primarySellingUom  IN ('CT','CARTON') THEN PROD.piecesInCarton
WHEN PROD.primarySellingUom  IN ('DP','DISPLAY/DISPENSER') THEN PROD.piecesInDisplayDispenser
WHEN PROD.primarySellingUom  IN ('DZ','DOZEN') THEN PROD.piecesInDozen
WHEN PROD.primarySellingUom  IN ('EA','EACH') THEN PROD.piecesInEach
WHEN PROD.primarySellingUom  IN ('GR','GROSS') THEN PROD.piecesInGross
WHEN PROD.primarySellingUom  IN ('PC','PIECE') THEN 1
WHEN PROD.primarySellingUom  IN ('PK','PACK') THEN PROD.piecesInPack
WHEN PROD.primarySellingUom  IN ('PR','PAIR') THEN 2
WHEN PROD.primarySellingUom  IN ('RL','ROLL') THEN PROD.piecesInRoll
END ) )AS claimedQuantityPrimary,
 GRAINGER.UOM_NAME AS claimedUom,
  NULL AS claimId,
  NULL AS claimNumber,
  NULL AS claimStatus,
  NULL AS contractEligible,
  'USD' AS currency,
  GRAINGER.BILL_DATE AS dateInvoiced,
  NULL AS disputeFollowUpAction,
  NULL AS disputeReasonCode,
  NULL AS disputeReasonDescription,
  NULL AS distributorClaimNumber,
  NULL AS distributorInvoiceNumber,
  NULL AS distributorOrderNumber,
  PARTY.partyId AS distributorPartyId,
  NULL AS distributorSubmittedAcquisitionCost,
  NULL AS distributorSubmittedEndUserActualSellingPrice,
  NULL AS distributorSubmittedEnduserAddress1,
  NULL AS distributorSubmittedEnduserAddress2,
  NULL AS distributorSubmittedEnduserAddress3,
  GRAINGER.CITY AS distributorSubmittedEnduserCity,
  NULL AS distributorSubmittedEndUserContractNumber,
  NULL AS distributorSubmittedEndUserContractPrice,
  NULL AS distributorSubmittedEnduserId,
  GRAINGER.SELLING_PLANT_DESCRIPTION AS distributorSubmittedEndUserName,
  GRAINGER.state AS distributorSubmittedEndUserState,
  GRAINGER.SHIP_ZIP_CODE AS distributorSubmittedEndUserZipCode,
  GRAINGER.SUPPLIER_STOCK_NO AS distributorSubmittedItemNumber,
  NULL AS distributorSubmittedMemoNumber,
  NULL AS distributorSubmittedTotalClaimedRebateAmount,
  NULL AS distributorSubmittedUnitRebateAmount,
  NULL AS endUserPartyId,
  0 AS ebsAcceptedAmount,
  NULL AS ebsEndUserSalesAmt,
  0 AS ebsTotalAllowedRebate,
  0 AS ebsTotalPaybackAmount,
  0 AS ebsUnitRebateAmount,
  NULL AS endUserPrmsNumber,
  NULL AS endUserProtectedPrice,
  NULL AS endUserUniqueGlobalLocatorNumber,
  1 AS exchangeRate,
  1 AS exchangeRateUsd,
  NULL AS gpoContractApprover,
  NULL AS gpoContractDescription,
  NULL AS gpoContractNumber,
  NULL AS gpoContractType,
  NULL AS gpoExporationDate,
  NULL AS gpoId,
  NULL AS gpoInitiator,
  NULL AS gpoStartDate,
  NULL AS gpoStatus,
  CASE WHEN PROD.ITEMID = 'Unknown' THEN GRAINGER.SUPPLIER_STOCK_NO ELSE PROD.ITEMID END AS itemId,
  GRAINGER.GIS_BRAND_LEVEL_1 as marketLevel1,
  GRAINGER.GIS_BRAND_LEVEL_2 as marketLevel2,
  GRAINGER.GIS_BRAND_LEVEL_3 as marketLevel3,
  ORG.organizationId AS owningBusinessUnitId,
  'POS' AS posFlag,
   'GRAINGER'   AS posSource,
  'PROCESSED' AS processingType,
  NULL rejectReason,
  GRAINGER.BILL_DATE AS reportDate,
  NULL AS resaleLineStatus,
  NULL AS reSaleTransferType,
  ZIP3.sales_region AS salesRegion,
  SECONDORG_ZIP3.sales_region AS salesRegionOrg2,
  THIRDORG_ZIP3.sales_region AS salesRegionOrg3,
  NULL AS settlementDocumentNumber,
  NULL AS settlementDocumentType,
  COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID
  ) AS territoryId,
  COALESCE(
    SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_ZIP3.TERRITORYID
  ) AS territoryIdOrg2,
  COALESCE(
    THIRDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    THIRDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    THIRDORG_ZIP3.TERRITORYID
  ) AS territoryIdOrg3,
  sum(GRAINGER.UNITS_SOLD * GRAINGER.UNITPRICE) AS transactionAmount,
  TERR_DEFAULT.userid AS userId,
  (
    CASE
      WHEN (
        SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE SECONDORG_ZIP3.USER_ID
    END
  ) AS userIdOrg2,
  (
    CASE
      WHEN (
        THIRDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR THIRDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE THIRDORG_ZIP3.USER_ID
    END
  ) AS userIdOrg3,
  GRAINGER.VERTICAL AS vertical
FROM
  main_stage GRAINGER
  LEFT JOIN S_CORE.ACCOUNT_AGG ACCOUNT ON ACCOUNT.registrationId = (
    case
      when GRAINGER.DISTID like '% - OTD' then rtrim(' - OTD', GRAINGER.DISTID)
      when GRAINGER.DISTID like '% - ITD' then rtrim(' - ITD', GRAINGER.DISTID)
      else GRAINGER.DISTID
    end
  )
  LEFT JOIN S_CORE.PARTY_AGG PARTY ON PARTY.partynumber = (
    case
      when GRAINGER.DISTID like '% - OTD' then rtrim(' - OTD', GRAINGER.DISTID)
      when GRAINGER.DISTID like '% - ITD' then rtrim(' - ITD', GRAINGER.DISTID)
      else GRAINGER.DISTID
    end
  )
  LEFT JOIN S_CORE.organization_agg ORG ON upper(ORG.name) = upper(GRAINGER.COMPANY)
  LEFT JOIN ZIP3 ON substr(GRAINGER.SHIP_ZIP_CODE, 1, 3) = ZIP3.zip3
  AND GRAINGER.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON GRAINGER.vertical = ZIP0.vertical
  LEFT JOIN SECONDORG_ZIP3 ON substr(GRAINGER.SHIP_ZIP_CODE, 1, 3) = SECONDORG_ZIP3.zip3
  AND GRAINGER.vertical = SECONDORG_ZIP3.vertical --LEFT JOIN SECONDORG_ZIP0 ON GRAINGER.vertical                   = SECONDORG_ZIP0.vertical
  LEFT JOIN PROD_CONV ON GRAINGER.SUPPLIER_STOCK_NO = PROD_CONV.external_code
  LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON PROD_CONV.item = PROD_OVERRIDE.PRODUCTID
  LEFT JOIN CUST_OVERRIDE ON GRAINGER.DISTID = CUST_OVERRIDE.custid
  LEFT JOIN S_CORE.PRODUCT_EBS PROD ON PROD_CONV.ITEM = PROD.PRODUCTCODE
  LEFT JOIN SECONDORG_PROD_OVERRIDE ON PROD_CONV.item = SECONDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN SECONDORG_CUST_OVERRIDE ON GRAINGER.DISTID = SECONDORG_CUST_OVERRIDE.custid
  LEFT JOIN TERR_DEFAULT ON GRAINGER.vertical = TERR_DEFAULT.vertical
  AND SUBSTR(GRAINGER.SHIP_ZIP_CODE, 1, 3) = TERR_DEFAULT.zip3
  LEFT JOIN THIRDORG_PROD_OVERRIDE ON PROD_CONV.item = THIRDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN THIRDORG_CUST_OVERRIDE ON GRAINGER.DISTID = THIRDORG_CUST_OVERRIDE.custid
  LEFT JOIN THIRDORG_ZIP3 ON substr(GRAINGER.SHIP_ZIP_CODE, 1, 3) = THIRDORG_ZIP3.zip3
  WHERE ORG.organizationType = 'OPERATING_UNIT'
GROUP BY
  ACCOUNT.accountid,
  GRAINGER.UNITPRICE,
  GRAINGER.UOM_NAME,
  GRAINGER.BILL_DATE,
  PARTY.partyId,
  GRAINGER.CITY,
  GRAINGER.SELLING_PLANT_DESCRIPTION,
  GRAINGER.state,
  GRAINGER.SHIP_ZIP_CODE,
  GRAINGER.SUPPLIER_STOCK_NO,
  CASE WHEN PROD.ITEMID = 'Unknown' THEN GRAINGER.SUPPLIER_STOCK_NO ELSE PROD.ITEMID END,
  GRAINGER.GIS_BRAND_LEVEL_1 ,
  GRAINGER.GIS_BRAND_LEVEL_2 ,
  GRAINGER.GIS_BRAND_LEVEL_3 ,
  ORG.organizationId,
  ZIP3.sales_region,
  SECONDORG_ZIP3.sales_region,
  THIRDORG_ZIP3.sales_region,
  COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID
  ),
  COALESCE(
    SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_ZIP3.TERRITORYID
  ),
  COALESCE(
    THIRDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    THIRDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    THIRDORG_ZIP3.TERRITORYID
  ),
  TERR_DEFAULT.userid,
  (
    CASE
      WHEN (
        SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE SECONDORG_ZIP3.USER_ID
    END
  ),
  (
    CASE
      WHEN (
        THIRDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR THIRDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE THIRDORG_ZIP3.USER_ID
    END
  ),
  GRAINGER.VERTICAL

""")

main.createOrReplaceTempView('main')

# COMMAND ----------

columns = list(schema.keys())

key_columns = ['processingType', 'batchType', 'claimId', 'claimNumber', 'territoryId', 'userid', 'vertical', 'distributorSubmittedEndUserZipCode', 'dateInvoiced', 'claimedUom', 'itemId',  'distributorSubmittedEndUserState', 'distributorSubmittedEnduserCity', 'distributorSubmittedEndUserName', 'distributorSubmittedItemNumber', 'marketLevel1', 'marketLevel2', 'marketLevel3', 'posSource', 'ansellContractPrice']

# COMMAND ----------

# TRANSFORM DATA
main_2 = (
  main
  .transform(parse_date(['dateInvoiced','reportDate'], expected_format = 'M/d/yyyy')) 
  .transform(parse_timestamp(['createdOn'], expected_format = 'M/d/yyyy')) 
  .transform(convert_null_to_unknown(key_columns))
)

# COMMAND ----------

# DROP DUPLICATES
main_3 = remove_duplicate_rows(main_2, key_columns, table_name, 
  source_name, NOTEBOOK_NAME, NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main_3  
  .transform(tg_default(source_name))
  .transform(tg_trade_management_point_of_sales())
  .transform(attach_dataset_column('GRAINGER'))
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys = spark.sql("""
SELECT 
CAST(NVL(GRAINGER.UNITPRICE,0) AS decimal(22,7))  AS ansellContractPrice,
 NULL AS batchType,
GRAINGER.UOM_NAME AS claimedUom,
NULL   AS claimId,
NULL   AS claimNumber,
GRAINGER.BILL_DATE AS dateInvoiced,
GRAINGER.CITY AS distributorSubmittedEnduserCity,
GRAINGER.SELLING_PLANT_DESCRIPTION AS distributorSubmittedEndUserName,
GRAINGER.state AS distributorSubmittedEndUserState,
GRAINGER.SHIP_ZIP_CODE AS distributorSubmittedEndUserZipCode,
GRAINGER.SUPPLIER_STOCK_NO  AS distributorSubmittedItemNumber,
CASE WHEN PROD.ITEMID = 'Unknown' THEN GRAINGER.SUPPLIER_STOCK_NO ELSE PROD.ITEMID END AS itemId,
GRAINGER.GIS_BRAND_LEVEL_1  as marketLevel1,
GRAINGER.GIS_BRAND_LEVEL_2  as marketLevel2,
GRAINGER.GIS_BRAND_LEVEL_3   as marketLevel3,
'PROCESSED' AS processingType,
 COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID
  ) AS territoryId,
TERR_DEFAULT.userid AS userId,
GRAINGER.VERTICAL AS vertical,
'GRAINGER'   AS posSource
 from 
amazusftp1.grainger_pos GRAINGER
 LEFT JOIN S_CORE.ACCOUNT_AGG ACCOUNT ON ACCOUNT.registrationId = (
    case
      when GRAINGER.DISTID like '% - OTD' then rtrim(' - OTD', GRAINGER.DISTID)
      when GRAINGER.DISTID like '% - ITD' then rtrim(' - ITD', GRAINGER.DISTID)
      else GRAINGER.DISTID
    end
  )
  LEFT JOIN S_CORE.PARTY_AGG PARTY ON PARTY.partynumber = (
    case
      when GRAINGER.DISTID like '% - OTD' then rtrim(' - OTD', GRAINGER.DISTID)
      when GRAINGER.DISTID like '% - ITD' then rtrim(' - ITD', GRAINGER.DISTID)
      else GRAINGER.DISTID
    end
  )
  LEFT JOIN S_CORE.organization_agg ORG ON upper(ORG.name) = upper(GRAINGER.COMPANY)
  LEFT JOIN ZIP3 ON substr(GRAINGER.SHIP_ZIP_CODE, 1, 3) = ZIP3.zip3
  AND GRAINGER.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON GRAINGER.vertical = ZIP0.vertical
  LEFT JOIN SECONDORG_ZIP3 ON substr(GRAINGER.SHIP_ZIP_CODE, 1, 3) = SECONDORG_ZIP3.zip3
  AND GRAINGER.vertical = SECONDORG_ZIP3.vertical 
  LEFT JOIN PROD_CONV ON GRAINGER.SUPPLIER_STOCK_NO = PROD_CONV.external_code
  LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON PROD_CONV.item = PROD_OVERRIDE.PRODUCTID
  LEFT JOIN CUST_OVERRIDE ON GRAINGER.DISTID = CUST_OVERRIDE.custid
  LEFT JOIN S_CORE.PRODUCT_EBS PROD ON PROD_CONV.ITEM = PROD.PRODUCTCODE
  LEFT JOIN SECONDORG_PROD_OVERRIDE ON PROD_CONV.item = SECONDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN SECONDORG_CUST_OVERRIDE ON GRAINGER.DISTID = SECONDORG_CUST_OVERRIDE.custid
  LEFT JOIN TERR_DEFAULT ON GRAINGER.vertical = TERR_DEFAULT.vertical
  AND SUBSTR(GRAINGER.SHIP_ZIP_CODE, 1, 3) = TERR_DEFAULT.zip3
  LEFT JOIN THIRDORG_PROD_OVERRIDE ON PROD_CONV.item = THIRDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN THIRDORG_CUST_OVERRIDE ON GRAINGER.DISTID = THIRDORG_CUST_OVERRIDE.custid
  LEFT JOIN THIRDORG_ZIP3 ON substr(GRAINGER.SHIP_ZIP_CODE, 1, 3) = THIRDORG_ZIP3.zip3
  WHERE ORG.organizationType = 'OPERATING_UNIT'
""")

full_keys_f = (
  full_keys
  .transform(parse_date(['dateInvoiced'], expected_format = 'M/dd/yyyy'))
  .transform(convert_null_to_unknown(key_columns))
  .transform(attach_source_column(source = source_name)) 
  .transform(attach_surrogate_key(columns = [*key_columns, '_SOURCE']))
  .select('_ID')
  .transform(add_unknown_ID())
)



filter_date = spark.sql("""select distinct Date_Format(to_Date(BILL_DATE,'M/d/yyyy'),'yyyy-MM-dd') TransactionDate from amazusftp1.grainger_pos """).rdd.flatMap(lambda x: x).collect()
apply_soft_delete(full_keys_f, table_name, key_columns = '_ID',date_field = 'dateInvoiced', date_value = filter_date)


# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_trademanagement
