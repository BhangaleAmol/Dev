# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_tm.pointofsales

# COMMAND ----------

# LOAD DATASETS
file_path = get_file_path(temp_folder, 's_trademanagement.clean_territory_assignments.dlt', target_container, target_storage)
terr_df = spark.read.format('delta').load(file_path)

main_inc = spark.table('ebs.ozf_resale_lines_all')
main_int_inc = spark.table('ebs.ozf_resale_lines_int_all')

# COMMAND ----------

# SAMPLING
if sampling:
  terr_df = terr_df.limit(10)
  main_inc = main_inc.limit(10)
  main_int_inc = main_int_inc.limit(10)

# COMMAND ----------

# VIEWS
terr_df.createOrReplaceTempView("terr_df")
main_inc.createOrReplaceTempView('ozf_resale_lines_all')
main_int_inc.createOrReplaceTempView('ozf_resale_lines_int_all')

# COMMAND ----------

OZF_RESALE_ADJUSTMENTS_ALL=spark.sql("""
SELECT
  a.CORRECTED_AGREEMENT_ID CORRECTED_AGREEMENT_ID,
  a.orig_system_agreement_name orig_system_agreement_name,
  a.corrected_agreement_name corrected_agreement_name,
  a.orig_system_agreement_price orig_system_agreement_price,
  a.agreement_name agreement_name,
  a.claimed_amount claimed_amount,
  a.total_claimed_amount total_claimed_amount,
  a.CALCULATED_PRICE CALCULATED_PRICE,
  a.allowed_amount allowed_amount,
  a.total_allowed_amount total_allowed_amount,
  a.RESALE_BATCH_ID RESALE_BATCH_ID,
  A.RESALE_LINE_ID RESALE_LINE_ID,
  a.LINE_AGREEMENT_FLAG LINE_AGREEMENT_FLAG,
  max(a.CREATION_DATE) CREATION_DATE,
  accepted_amount accepted_amount,
  a.END_USER_UNIT_PRICE
FROM
  EBS.OZF_RESALE_ADJUSTMENTS_ALL A
where
  A.LINE_AGREEMENT_FLAG = 'T'
GROUP BY
  A.CORRECTED_AGREEMENT_ID,
  A.ORIG_SYSTEM_AGREEMENT_NAME,
  A.CORRECTED_AGREEMENT_NAME,
  A.ORIG_SYSTEM_AGREEMENT_PRICE,
  A.AGREEMENT_NAME,
  A.CLAIMED_AMOUNT,
  A.TOTAL_CLAIMED_AMOUNT,
  A.CALCULATED_PRICE,
  A.ALLOWED_AMOUNT,
  A.TOTAL_ALLOWED_AMOUNT,
  A.RESALE_BATCH_ID,
  A.RESALE_LINE_ID,
  A.LINE_AGREEMENT_FLAG,
  accepted_amount,
  a.END_USER_UNIT_PRICE
""")

OZF_RESALE_ADJUSTMENTS_ALL.createOrReplaceTempView('OZF_RESALE_ADJUSTMENTS_ALL')

# COMMAND ----------

OZF_SETTLEMENT_DOCS_ALL=spark.sql("""
SELECT
  DISTINCT CLAIM_ID,
  SETTLEMENT_TYPE,
  SETTLEMENT_NUMBER
FROM
  EBS.OZF_SETTLEMENT_DOCS_ALL
""")

OZF_SETTLEMENT_DOCS_ALL.createOrReplaceTempView('OZF_SETTLEMENT_DOCS_ALL')

# COMMAND ----------

DIS=spark.sql("""
SELECT
  LV.LOOKUP_CODE,
  LV.MEANING
from
  ebs.FND_LOOKUP_VALUES LV
WHERE
  LV.LANGUAGE = 'US'
  and LV.VIEW_APPLICATION_ID = 682
  and LV.SECURITY_GROUP_ID = 0
  and lookup_type = 'OZF_RESALE_DISPUTE_CODE'
  AND lookup_code IN (
    SELECT
      distinct dispute_code
    FROM
      ebs.ozf_resale_lines_int_all
  )
""")

DIS.createOrReplaceTempView('DIS')     

# COMMAND ----------

OZF_LOOKUPS=spark.sql("""
SELECT
  LV.LOOKUP_TYPE,
  LV.LOOKUP_CODE,
  LV.LAST_UPDATE_DATE,
  LV.LAST_UPDATED_BY,
  LV.LAST_UPDATE_LOGIN,
  LV.CREATION_DATE,
  LV.CREATED_BY,
  LV.MEANING,
  LV.ENABLED_FLAG,
  LV.START_DATE_ACTIVE,
  LV.END_DATE_ACTIVE,
  LV.DESCRIPTION,
  LV.ATTRIBUTE_CATEGORY,
  LV.ATTRIBUTE1,
  LV.ATTRIBUTE2,
  LV.ATTRIBUTE3,
  LV.ATTRIBUTE4,
  LV.ATTRIBUTE5,
  LV.ATTRIBUTE6,
  LV.ATTRIBUTE7,
  LV.ATTRIBUTE8,
  LV.ATTRIBUTE9,
  LV.ATTRIBUTE10,
  LV.ATTRIBUTE11,
  LV.ATTRIBUTE12,
  LV.ATTRIBUTE13,
  LV.ATTRIBUTE14,
  LV.ATTRIBUTE15,
  LV.TAG
from
  EBS.FND_LOOKUP_VALUES LV
WHERE
  LV.LANGUAGE = 'US'
  and LV.VIEW_APPLICATION_ID = 682
  and LV.SECURITY_GROUP_ID = 0
  AND LV.LOOKUP_TYPE = 'OZF_FOLLOWUP_ACTION_CODE'
 """)
        
OZF_LOOKUPS.createOrReplaceTempView('OZF_LOOKUPS')     

# COMMAND ----------

MTL_SYSTEM_ITEMS_B=spark.sql("""
SELECT *
from
  EBS.MTL_SYSTEM_ITEMS_B 
WHERE
  ORGANIZATION_ID = 124 
 """)
        
MTL_SYSTEM_ITEMS_B.createOrReplaceTempView('MTL_SYSTEM_ITEMS_B')     

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
  DISTINCT case
      when CustomerID like '% - OTD' then rtrim(' - OTD', CustomerID)
      when CustomerID like '% - ITD' then rtrim(' - ITD', CustomerID)
      else CustomerID
    end CUSTID,
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
  DISTINCT case
      when CustomerID like '% - OTD' then rtrim(' - OTD', CustomerID)
      when CustomerID like '% - ITD' then rtrim(' - ITD', CustomerID)
      else CustomerID
    end CUSTID,
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
  DISTINCT case
      when CustomerID like '% - OTD' then rtrim(' - OTD', CustomerID)
      when CustomerID like '% - ITD' then rtrim(' - ITD', CustomerID)
      else CustomerID
    end CUSTID,
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

PROCON_CONTRACTS =spark.sql("""
SELECT DISTINCT GPO_ID,
CONTRACT_NUMBER,
DESCRIPTION,
CONTRACT_TYPE,
to_date(START_DATE,'dd-MMM-yy') START_DATE,
to_date(EXPIRATION_DATE,'dd-MMM-yy') EXPIRATION_DATE,
STATUS,
INITIATOR,
CONTRACT_APPROVER
FROM amazusftp1.WC_PROCON_ALL_CONTRACTS_A
where contract_number not like 'New Contract' and status not in  ('Rejected','Deleted')
""")

PROCON_CONTRACTS .createOrReplaceTempView('PROCON_CONTRACTS')

# COMMAND ----------

PROCON_PAM_CONTRACTS =spark.sql("""
SELECT
CONTRACTID,
MAX(GPO) GPO,
MAX(ENDUSER) ENDUSER,
MAX(CONTRACTTYPE) CONTRACTTYPE,
MAX(STARTDATE) STARTDATE,
MAX(ENDDATE) ENDDATE,
MAX(CONTRACTSTATUS) CONTRACTSTATUS,
MAX(OWNER) OWNER,
MAX(LASTAPPROVER) LASTAPPROVER
FROM (
SELECT distinct 
A.GPO,
A.CONTRACTID,
A.ENDUSER,
A.CONTRACTTYPE,
to_date(A.STARTDATE,'MM/dd/yyyy') STARTDATE,
to_date(A.ENDDATE,'MM/dd/yyyy') ENDDATE,
A.CONTRACTSTATUS,
A.OWNER,
A.LASTAPPROVER
FROM
  PAM.DBO_VW_REPORTS_ALL_CONTRACTS_SUMMARY_MED A,
  (
    SELECT
      CONTRACTID,
      MIN(STATUS_SELECTION) STATUS
    FROM
      (
        SELECT
          CONTRACTID,
          CONTRACTSTATUS,
          (
            CASE
              WHEN CONTRACTSTATUS = 'Expired- Not Renewed' THEN 1
              WHEN CONTRACTSTATUS = 'Contract Will Not Be Renewed' THEN 2
              WHEN CONTRACTSTATUS = 'Contract Renewed' THEN 3
              WHEN CONTRACTSTATUS = 'Contract Issued' THEN 4
            END
          ) STATUS_SELECTION
        FROM
          PAM.DBO_VW_REPORTS_ALL_CONTRACTS_SUMMARY_MED
      )
    GROUP BY
      CONTRACTID
  ) B
WHERE
  A.CONTRACTID = B.CONTRACTID
  AND (
    CASE
      WHEN A.CONTRACTSTATUS = 'Expired- Not Renewed' THEN 1
      WHEN A.CONTRACTSTATUS = 'Contract Will Not Be Renewed' THEN 2
      WHEN A.CONTRACTSTATUS = 'Contract Renewed' THEN 3
      WHEN A.CONTRACTSTATUS = 'Contract Issued' THEN 4
    END
  ) = B.STATUS
  )
  GROUP BY CONTRACTID
  
  """)

PROCON_PAM_CONTRACTS .createOrReplaceTempView('PROCON_PAM_CONTRACTS')

# COMMAND ----------

EXCLUSION_LIST = spark.sql("""
SELECT DISTINCT PARTYID , REPLACE(
    STRING(INT(HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID)),
    ",",
    ""
  ) AS ACCOUNTID,ITDOTD  FROM amazusftp1.EXCLUSION_LIST  
LEFT JOIN EBS.HZ_CUST_ACCOUNTS ON HZ_CUST_ACCOUNTS.ACCOUNT_NUMBER = amazusftp1.EXCLUSION_LIST.ACCOUNTNUMBER
""")

EXCLUSION_LIST .createOrReplaceTempView('EXCLUSION_LIST')

# COMMAND ----------

REJECT_REASON = spark.sql("""
SELECT distinct lookup_code,meaning from EBS.FND_LOOKUP_VALUES where lookup_type = 'OZF_REJECT_REASON_CODE' AND LANGUAGE = 'US'
""")

REJECT_REASON.createOrReplaceTempView('REJECT_REASON')

# COMMAND ----------

main_chargeback = spark.sql("""
--chargeback
SELECT
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_ALL.CREATED_BY)),
    ",",
    ""
  ) AS createdBy,
  OZF_RESALE_LINES_ALL.CREATION_DATE AS createdOn,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_ALL.LAST_UPDATED_BY)),
    ",",
    ""
  ) AS modifiedBy,
  OZF_RESALE_LINES_ALL.LAST_UPDATE_DATE AS modifiedOn,
  CURRENT_TIMESTAMP AS insertedOn,
  CURRENT_TIMESTAMP AS updatedOn,
  REPLACE(
    STRING(INT (OZF_CLAIMS_ALL.CUST_ACCOUNT_ID)),
    ",",
    ""
  )  AS accountId,
  OZF_RESALE_LINES_ALL.PURCHASE_PRICE AS ansellAcquisitionCost,
  OZF_RESALE_ADJUSTMENTS_ALL.AGREEMENT_NAME AS ansellAgreementName,
  OZF_RESALE_ADJUSTMENTS_ALL.CALCULATED_PRICE AS ansellContractPrice,
  OZF_RESALE_ADJUSTMENTS_ALL.CORRECTED_AGREEMENT_NAME AS ansellCorrectedAgreementName,
  OZF_CLAIMS_ALL.LAST_UPDATE_DATE AS approvalDate,
  OZF_RESALE_BATCHES_ALL.BATCH_NUMBER AS batchNumber,
  OZF_RESALE_BATCHES_ALL.STATUS_CODE AS batchStatusCode,
  OZF_RESALE_BATCHES_ALL.BATCH_TYPE AS batchType,
  OZF_RESALE_LINES_ALL.LINE_ATTRIBUTE10 AS claimComments,
  OZF_RESALE_LINES_ALL.QUANTITY AS claimedQuantity,
   ((CASE 
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BD' THEN PROD.piecesInBundle
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BG' THEN PROD.piecesInBag
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BX' THEN PROD.piecesInBox
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'CA' THEN PROD.piecesInCase
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'CT' THEN PROD.piecesInCarton
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'DP' THEN PROD.piecesInDisplayDispenser
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'DZ' THEN PROD.piecesInDozen
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'EA' THEN PROD.piecesInEach
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'GR' THEN PROD.piecesInGross
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PC' THEN 1
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PK' THEN PROD.piecesInPack
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PR' THEN 2
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'RL' THEN PROD.piecesInRoll
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'KT' THEN PROD.piecesInKit     
END ) * OZF_RESALE_LINES_ALL.QUANTITY)

/ PROD.piecesInCase claimedQuantityCase,
   ((CASE 
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BD' THEN PROD.piecesInBundle
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BG' THEN PROD.piecesInBag
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BX' THEN PROD.piecesInBox
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'CA' THEN PROD.piecesInCase
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'CT' THEN PROD.piecesInCarton
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'DP' THEN PROD.piecesInDisplayDispenser
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'DZ' THEN PROD.piecesInDozen
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'EA' THEN PROD.piecesInEach
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'GR' THEN PROD.piecesInGross
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PC' THEN 1
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PK' THEN PROD.piecesInPack
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PR' THEN 2
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'RL' THEN PROD.piecesInRoll
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'KT' THEN PROD.piecesInKit
END ) * OZF_RESALE_LINES_ALL.QUANTITY)

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
WHEN PROD.primarySellingUom  IN ('KT','KIT') THEN PROD.piecesInKit
END ) AS claimedQuantityPrimary,
  OZF_RESALE_LINES_ALL.UOM_CODE AS claimedUom,
  BIGINT(OZF_RESALE_LINES_ALL.RESALE_LINE_ID) AS claimId,
  OZF_CLAIMS_ALL.CLAIM_NUMBER AS claimNumber,
  OZF_CLAIMS_ALL.STATUS_CODE AS claimStatus,
  'Y' AS contractEligible,
  OZF_RESALE_BATCHES_ALL.CURRENCY_CODE AS currency,
  CAST(NULL AS STRING) AS disputeFollowUpAction,
  CAST(NULL AS STRING) AS disputeReasonCode,
  CAST(NULL AS STRING) AS disputeReasonDescription,
  OZF_RESALE_BATCHES_ALL.partner_claim_number AS distributorClaimNumber,
  OZF_RESALE_LINES_ALL.Invoice_number AS distributorInvoiceNumber,
  OZF_RESALE_LINES_ALL.order_number AS distributorOrderNumber,
  REPLACE(
    STRING(INT (HZ_PARTIES.PARTY_ID)),
    ",",
    ""
  ) AS distributorPartyId,
  OZF_RESALE_LINES_ALL.orig_system_purchase_price AS distributorSubmittedAcquisitionCost,
  OZF_RESALE_ADJUSTMENTS_ALL.orig_system_agreement_price AS distributorSubmittedEndUserActualSellingPrice,
  OZF_RESALE_LINES_ALL.bill_to_address AS distributorSubmittedEnduserAddress1,
  OZF_RESALE_LINES_ALL.line_attribute3 AS distributorSubmittedEnduserAddress2,
  OZF_RESALE_LINES_ALL.line_attribute4 AS distributorSubmittedEnduserAddress3,
  OZF_RESALE_LINES_ALL.bill_to_city AS distributorSubmittedEnduserCity,
  OZF_RESALE_ADJUSTMENTS_ALL.orig_system_agreement_name AS distributorSubmittedEndUserContractNumber,
  OZF_RESALE_LINES_ALL.ORIG_SYSTEM_SELLING_PRICE AS distributorSubmittedEndUserContractPrice,
  OZF_RESALE_LINES_ALL.line_attribute11 AS distributorSubmittedEnduserId,
  OZF_RESALE_LINES_ALL.line_attribute7 AS distributorSubmittedEndUserName,
  OZF_RESALE_LINES_ALL.bill_to_state AS distributorSubmittedEndUserState,
  OZF_RESALE_LINES_ALL.bill_to_postal_code AS distributorSubmittedEndUserZipCode,
  
 case 
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 2 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 3 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 4 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 5 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 6 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 7 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 8 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 2 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 3 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 4 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 5 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 6 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 7 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 8 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   else trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)  end   AS distributorSubmittedEndUserZipCodeUpdated,
  
  OZF_RESALE_LINES_ALL.orig_system_item_number AS distributorSubmittedItemNumber,
  OZF_RESALE_LINES_ALL.line_attribute5 AS distributorSubmittedMemoNumber,
  OZF_RESALE_ADJUSTMENTS_ALL.total_claimed_amount AS distributorSubmittedTotalClaimedRebateAmount,
  OZF_RESALE_ADJUSTMENTS_ALL.claimed_amount AS distributorSubmittedUnitRebateAmount,
  REPLACE(STRING(INT (OZF_RESALE_LINES_ALL.BILL_TO_PARTY_ID)),
    ",",
    ""
  ) as endUserPartyId,
  OZF_RESALE_ADJUSTMENTS_ALL.END_USER_UNIT_PRICE * OZF_RESALE_LINES_ALL.quantity AS ebsEndUserSalesAmt,
  EU_HP.orig_system_reference  AS endUserPrmsNumber,
  OZF_RESALE_LINES_ALL.LINE_ATTRIBUTE14 AS endUserProtectedPrice,
  EU_HPS.global_location_number AS endUserUniqueGlobalLocatorNumber,
  NULL AS exchangeRate,
  NULL AS exchangeRateUsd,
  OZF_RESALE_LINES_ALL.date_ordered AS dateInvoiced,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID)),
    ",",
    ""
  ) AS itemId,
  REPLACE(
    STRING(INT (OZF_RESALE_BATCHES_ALL.ORG_ID)),
    ",",
    ""
  ) AS owningBusinessUnitId,
  'PROCESSED' AS processingType,
  OZF_RESALE_BATCHES_ALL.report_date AS reportDate,
  OZF_RESALE_LINES_ALL.status_code AS resaleLineStatus,
  OZF_RESALE_LINES_ALL.resale_transfer_type AS reSaleTransferType,
  NULL salesRegion,
  OZF_SETTLEMENT_DOCS_ALL.SETTLEMENT_NUMBER settlementDocumentNumber,
  OZF_SETTLEMENT_DOCS_ALL.SETTLEMENT_TYPE settlementDocumentType,
  NULL territoryId,
  CASE 
    WHEN OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED', 'PENDING_PAYMENT')
        and NVL(OZF_RESALE_ADJUSTMENTS_ALL.END_USER_UNIT_PRICE,0) = 0
          then OZF_RESALE_LINES_ALL.QUANTITY * OZF_RESALE_LINES_ALL.ORIG_SYSTEM_SELLING_PRICE 
    WHEN OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED', 'CLOSED', 'REJECTED', 'PENDING_PAYMENT')
          then OZF_RESALE_LINES_ALL.QUANTITY * OZF_RESALE_ADJUSTMENTS_ALL.END_USER_UNIT_PRICE 
  END transactionAmount,
  NULL userId,
  MTL_SYSTEM_ITEMS_B.SEGMENT1 productCode,
  HZ_CUST_ACCOUNTS.ATTRIBUTE11 vertical,
  NVL (OZF_RESALE_ADJUSTMENTS_ALL.allowed_amount, 0) as ebsUnitRebateAmount,
  NVL (OZF_RESALE_ADJUSTMENTS_ALL.total_allowed_amount, 0) as ebsTotalAllowedRebate,
  NVL ((OZF_RESALE_ADJUSTMENTS_ALL.total_claimed_amount - OZF_RESALE_ADJUSTMENTS_ALL.total_allowed_amount), 0) as ebsTotalPaybackAmount,
  OZF_RESALE_ADJUSTMENTS_ALL.ACCEPTED_AMOUNT as ebsAcceptedAmount,
  NULL rejectReason
FROM
  EBS.OZF_RESALE_LINES_ALL
  INNER JOIN OZF_RESALE_ADJUSTMENTS_ALL ON OZF_RESALE_LINES_ALL.RESALE_LINE_ID = OZF_RESALE_ADJUSTMENTS_ALL.RESALE_LINE_ID
  INNER JOIN EBS.OZF_RESALE_BATCHES_ALL ON OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID = OZF_RESALE_ADJUSTMENTS_ALL.RESALE_BATCH_ID
  LEFT JOIN EBS.MTL_SYSTEM_ITEMS_B ON MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID = OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID
  LEFT JOIN EBS.OZF_CLAIMS_ALL ON OZF_CLAIMS_ALL.SOURCE_OBJECT_ID = OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID
  LEFT JOIN EBS.HZ_CUST_ACCOUNTS ON OZF_CLAIMS_ALL.CUST_ACCOUNT_ID = HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID
  LEFT JOIN EBS.HZ_PARTIES ON HZ_CUST_ACCOUNTS.PARTY_ID = HZ_PARTIES.PARTY_ID
  LEFT JOIN EBS.HZ_PARTY_SITES ON HZ_PARTIES.PARTY_ID = HZ_PARTY_SITES.PARTY_ID
  LEFT JOIN OZF_SETTLEMENT_DOCS_ALL ON OZF_CLAIMS_ALL.CLAIM_ID = OZF_SETTLEMENT_DOCS_ALL.CLAIM_ID
  LEFT JOIN  EBS.HZ_PARTIES EU_HP ON OZF_RESALE_LINES_ALL.BILL_TO_PARTY_ID      = EU_HP.PARTY_ID
  LEFT JOIN   EBS.HZ_PARTY_SITES EU_HPS ON OZF_RESALE_LINES_ALL.BILL_TO_PARTY_SITE_ID = EU_HPS.PARTY_SITE_ID
  LEFT JOIN S_CORE.PRODUCT_EBS PROD ON OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID = PROD.ITEMID
WHERE
  1 = 1
  AND OZF_RESALE_BATCHES_ALL.STATUS_CODE IN ('CLOSED')
  AND OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'CHARGEBACK'
  AND MTL_SYSTEM_ITEMS_B.organization_id = 124
  AND OZF_RESALE_ADJUSTMENTS_ALL.LINE_AGREEMENT_FLAG = 'T'
  AND HZ_PARTY_SITES.IDENTIFYING_ADDRESS_FLAG = 'Y'
  AND OZF_CLAIMS_ALL.STATUS_CODE <> 'CANCELLED' --and OZF_RESALE_LINES_ALL.resale_line_id = 8953622
""")

main_chargeback = main_chargeback.transform(apply_schema(schema))
main_chargeback.createOrReplaceTempView('main_chargeback')

# COMMAND ----------

main_disputed = spark.sql("""
--Disputed
SELECT
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_INT_ALL.CREATED_BY)),
    ",",
    ""
  ) AS createdBy,
  OZF_RESALE_LINES_INT_ALL.CREATION_DATE AS createdOn,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_INT_ALL.LAST_UPDATED_BY)),
    ",",
    ""
  ) AS modifiedBy,
  OZF_RESALE_LINES_INT_ALL.LAST_UPDATE_DATE AS modifiedOn,
  CURRENT_TIMESTAMP AS insertedOn,
  CURRENT_TIMESTAMP AS updatedOn,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_INT_ALL.SOLD_FROM_CUST_ACCOUNT_ID)),
    ",",
    ""
  )  AS accountId,
  OZF_RESALE_LINES_INT_ALL.PURCHASE_PRICE AS ansellAcquisitionCost,
  OZF_RESALE_LINES_INT_ALL.AGREEMENT_NAME AS ansellAgreementName,
  OZF_RESALE_LINES_INT_ALL.CALCULATED_PRICE AS ansellContractPrice,
  OZF_RESALE_LINES_INT_ALL.CORRECTED_AGREEMENT_NAME AS ansellCorrectedAgreementName,
  NULL AS approvalDate,
  OZF_RESALE_BATCHES_ALL.BATCH_NUMBER AS batchNumber,
  OZF_RESALE_BATCHES_ALL.STATUS_CODE AS batchStatusCode,
  OZF_RESALE_BATCHES_ALL.BATCH_TYPE AS batchType,
  OZF_RESALE_LINES_INT_ALL.LINE_ATTRIBUTE10 AS claimComments,
  OZF_RESALE_LINES_INT_ALL.QUANTITY AS claimedQuantity,
    ((CASE 
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'BD' THEN PROD.piecesInBundle
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'BG' THEN PROD.piecesInBag
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'BX' THEN PROD.piecesInBox
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'CA' THEN PROD.piecesInCase
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'CT' THEN PROD.piecesInCarton
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'DP' THEN PROD.piecesInDisplayDispenser
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'DZ' THEN PROD.piecesInDozen
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'EA' THEN PROD.piecesInEach
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'GR' THEN PROD.piecesInGross
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'PC' THEN 1
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'PK' THEN PROD.piecesInPack
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'PR' THEN 2
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'RL' THEN PROD.piecesInRoll
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'KT' THEN PROD.piecesInKit
END ) * OZF_RESALE_LINES_INT_ALL.QUANTITY)

/ PROD.piecesInCase claimedQuantityCase,
  ((CASE 
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'BD' THEN PROD.piecesInBundle
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'BG' THEN PROD.piecesInBag
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'BX' THEN PROD.piecesInBox
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'CA' THEN PROD.piecesInCase
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'CT' THEN PROD.piecesInCarton
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'DP' THEN PROD.piecesInDisplayDispenser
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'DZ' THEN PROD.piecesInDozen
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'EA' THEN PROD.piecesInEach
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'GR' THEN PROD.piecesInGross
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'PC' THEN 1
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'PK' THEN PROD.piecesInPack
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'PR' THEN 2
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'RL' THEN PROD.piecesInRoll
WHEN OZF_RESALE_LINES_INT_ALL.UOM_CODE = 'KT' THEN PROD.piecesInKit
END ) * OZF_RESALE_LINES_INT_ALL.QUANTITY)

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
WHEN PROD.primarySellingUom  IN ('KT','KIT') THEN PROD.piecesInKit
END )   AS claimedQuantityPrimary,
  OZF_RESALE_LINES_INT_ALL.UOM_CODE AS claimedUom,
  BIGINT(OZF_RESALE_LINES_INT_ALL.RESALE_LINE_INT_ID) AS claimId,
  '-1' AS claimNumber,
  NULL AS claimStatus,
  OZF_RESALE_LINES_INT_ALL.CONTRACT_ELIGIBLE AS contractEligible,
  OZF_RESALE_BATCHES_ALL.CURRENCY_CODE AS currency,
  OZF_LOOKUPS.MEANING AS disputeFollowUpAction,
  OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE AS disputeReasonCode,
  DIS.MEANING AS disputeReasonDescription,
  OZF_RESALE_BATCHES_ALL.partner_claim_number AS distributorClaimNumber,
  OZF_RESALE_LINES_INT_ALL.Invoice_number AS distributorInvoiceNumber,
  OZF_RESALE_LINES_INT_ALL.order_number AS distributorOrderNumber,
  REPLACE(
    STRING(INT (HZ_PARTIES.PARTY_ID)),
    ",",
    ""
  ) AS distributorPartyId,
  OZF_RESALE_LINES_INT_ALL.orig_system_purchase_price AS distributorSubmittedAcquisitionCost,
  OZF_RESALE_LINES_INT_ALL.orig_system_agreement_price AS distributorSubmittedEndUserActualSellingPrice,
  OZF_RESALE_LINES_INT_ALL.bill_to_address AS distributorSubmittedEnduserAddress1,
  OZF_RESALE_LINES_INT_ALL.line_attribute3 AS distributorSubmittedEnduserAddress2,
  OZF_RESALE_LINES_INT_ALL.line_attribute4 AS distributorSubmittedEnduserAddress3,
  OZF_RESALE_LINES_INT_ALL.bill_to_city AS distributorSubmittedEnduserCity,
  OZF_RESALE_LINES_INT_ALL.orig_system_agreement_name AS distributorSubmittedEndUserContractNumber,
  OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_SELLING_PRICE AS distributorSubmittedEndUserContractPrice,
  OZF_RESALE_LINES_INT_ALL.line_attribute11 AS distributorSubmittedEnduserId,
  OZF_RESALE_LINES_INT_ALL.line_attribute7 AS distributorSubmittedEndUserName,
  OZF_RESALE_LINES_INT_ALL.bill_to_state AS distributorSubmittedEndUserState,
  OZF_RESALE_LINES_INT_ALL.bill_to_postal_code AS distributorSubmittedEndUserZipCode,
  
     case 
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 2 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 3 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 4 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 5 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' ||  substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 6 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 7 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 8 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 2 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 3 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 4 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 5 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 6 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 7 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 8 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   else trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)  end  AS distributorSubmittedEndUserZipCodeUpdated,
  
  
  OZF_RESALE_LINES_INT_ALL.orig_system_item_number AS distributorSubmittedItemNumber,
  OZF_RESALE_LINES_INT_ALL.line_attribute5 AS distributorSubmittedMemoNumber,
  OZF_RESALE_LINES_INT_ALL.total_claimed_amount AS distributorSubmittedTotalClaimedRebateAmount,
  OZF_RESALE_LINES_INT_ALL.claimed_amount AS distributorSubmittedUnitRebateAmount,
  REPLACE(STRING(INT (OZF_RESALE_LINES_INT_ALL.BILL_TO_PARTY_ID)),
    ",",
    ""
  ) as endUserPartyId,
  NULL AS ebsEndUserSalesAmt,
  HP2.orig_system_reference AS endUserPrmsNumber,
  OZF_RESALE_LINES_INT_ALL.LINE_ATTRIBUTE14 AS endUserProtectedPrice,
  END_USER_HPS.global_location_number AS endUserUniqueGlobalLocatorNumber,
  NULL AS exchangeRate,
  NULL AS exchangeRateUsd,
  OZF_RESALE_LINES_INT_ALL.date_ordered AS dateInvoiced,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_INT_ALL.INVENTORY_ITEM_ID)),
    ",",
    ""
  )AS itemId,
  REPLACE(
    STRING(INT (OZF_RESALE_BATCHES_ALL.ORG_ID)),
    ",",
    ""
  )AS owningBusinessUnitId,
  'DISPUTED' AS processingType,
  OZF_RESALE_BATCHES_ALL.report_date AS reportDate,
  OZF_RESALE_LINES_INT_ALL.status_code AS resaleLineStatus,
  OZF_RESALE_LINES_INT_ALL.resale_transfer_type AS reSaleTransferType,
  NULL salesRegion,
  NULL settlementDocumentNumber,
  NULL settlementDocumentType,
  NULL territoryId,
   case
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'CHARGEBACK' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED', 'PENDING_PAYMENT') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null and NVL( OZF_RESALE_LINES_INT_ALL.CALCULATED_PRICE,0) = 0 then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_SELLING_PRICE 
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'CHARGEBACK' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.CALCULATED_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'CHARGEBACK' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED', 'PENDING_PAYMENT') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.CALCULATED_PRICE


when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'CHARGEBACK' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null and NVL (OZF_RESALE_LINES_INT_ALL.CALCULATED_PRICE,0) =0 then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_SELLING_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'CHARGEBACK' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is NOT null then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_SELLING_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'CHARGEBACK' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED', 'PENDING_PAYMENT') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is NOT null then 0

when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null and NVL(OZF_RESALE_LINES_INT_ALL.PURCHASE_PRICE,0) = 0 then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_PURCHASE_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null and NVL(OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_PURCHASE_PRICE,0) = 0 then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_SELLING_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.PURCHASE_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is NOT null then 0
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.PURCHASE_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null and NVL (OZF_RESALE_LINES_INT_ALL.PURCHASE_PRICE,0) =0 then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_PURCHASE_PRICE 
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is null and NVL (OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_PURCHASE_PRICE,0) = 0  then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_SELLING_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is NOT null then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_PURCHASE_PRICE
when OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING' and OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED') and OZF_RESALE_LINES_INT_ALL.DISPUTE_CODE is NOT null and NVL(OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_PURCHASE_PRICE,0) = 0 then OZF_RESALE_LINES_INT_ALL.QUANTITY * OZF_RESALE_LINES_INT_ALL.ORIG_SYSTEM_SELLING_PRICE
else 99999999
end   transactionAmount,
  NULL userId,
  MTL_SYSTEM_ITEMS_B.SEGMENT1 productCode,
  HZ_CUST_ACCOUNTS.ATTRIBUTE11 vertical,
  NVL (OZF_RESALE_LINES_INT_ALL.allowed_amount, 0) as ebsUnitRebateAmount,
  NVL (OZF_RESALE_LINES_INT_ALL.total_allowed_amount, 0) as ebsTotalAllowedRebate,
  NVL ((OZF_RESALE_LINES_INT_ALL.total_claimed_amount - OZF_RESALE_LINES_INT_ALL.total_allowed_amount), 0) as ebsTotalPaybackAmount,
  OZF_RESALE_LINES_INT_ALL.ACCEPTED_AMOUNT as ebsAcceptedAmount,
  REJECT_REASON.meaning rejectReason
FROM
  OZF_RESALE_LINES_INT_ALL --INNER JOIN   OZF_RESALE_ADJUSTMENTS_ALL ON   OZF_RESALE_LINES_ALL.RESALE_LINE_ID  = OZF_RESALE_ADJUSTMENTS_ALL.RESALE_LINE_ID
  INNER JOIN EBS.OZF_RESALE_BATCHES_ALL ON OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID = OZF_RESALE_LINES_INT_ALL.RESALE_BATCH_ID
  LEFT JOIN OZF_LOOKUPS ON OZF_RESALE_LINES_INT_ALL.FOLLOWUP_ACTION_CODE = OZF_LOOKUPS.LOOKUP_CODE
  LEFT JOIN DIS ON DIS.lookup_code = OZF_RESALE_LINES_INT_ALL.dispute_code
  LEFT JOIN MTL_SYSTEM_ITEMS_B ON MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID = OZF_RESALE_LINES_INT_ALL.INVENTORY_ITEM_ID --LEFT JOIN EBS.OZF_CLAIMS_ALL ON  OZF_CLAIMS_ALL.SOURCE_OBJECT_ID   = OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID
  LEFT JOIN EBS.HZ_CUST_ACCOUNTS ON OZF_RESALE_LINES_INT_ALL.sold_from_cust_account_id = HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID
  LEFT JOIN EBS.HZ_PARTIES ON HZ_CUST_ACCOUNTS.PARTY_ID = HZ_PARTIES.PARTY_ID
  LEFT JOIN EBS.HZ_PARTIES HP2 ON HP2.party_id = OZF_RESALE_LINES_INT_ALL.bill_to_party_id
   LEFT JOIN EBS.HZ_PARTY_SITES END_USER_HPS ON   END_USER_HPS.PARTY_SITE_ID =  OZF_RESALE_LINES_INT_ALL.BILL_TO_PARTY_SITE_ID
   LEFT JOIN S_CORE.PRODUCT_EBS PROD ON OZF_RESALE_LINES_INT_ALL.INVENTORY_ITEM_ID = PROD.ITEMID
   left join REJECT_REASON on OZF_RESALE_LINES_INT_ALL.reject_reason_code  = REJECT_REASON.lookup_code
WHERE
  1 = 1
  AND OZF_RESALE_BATCHES_ALL.STATUS_CODE <> ('CLOSED')
  AND OZF_RESALE_BATCHES_ALL.purge_flag is null
  AND OZF_RESALE_BATCHES_ALL.BATCH_TYPE in ('TRACING', 'CHARGEBACK')
  --AND MTL_SYSTEM_ITEMS_B.organization_id = 124 --AND OZF_RESALE_ADJUSTMENTS_ALL.LINE_AGREEMENT_FLAG   = 'T'
  --AND HZ_PARTY_SITES.IDENTIFYING_ADDRESS_FLAG = 'Y' --AND ozf_lookups.lookup_TYPE = 'OZF_FOLLOWUP_ACTION_CODE'
  --AND OZF_CLAIMS_ALL.STATUS_CODE <> 'CANCELLED'
  --and OZF_RESALE_LINES_INT_ALL.resale_line_int_id = '9019819'

""")

main_disputed = main_disputed.transform(apply_schema(schema))
main_disputed.createOrReplaceTempView('main_disputed')

# COMMAND ----------

main_tracing = spark.sql("""
 --tracing
SELECT
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_ALL.CREATED_BY)),
    ",",
    ""
  ) AS createdBy,
  OZF_RESALE_LINES_ALL.CREATION_DATE AS createdOn,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_ALL.LAST_UPDATED_BY)),
    ",",
    ""
  ) AS modifiedBy,
  OZF_RESALE_LINES_ALL.LAST_UPDATE_DATE AS modifiedOn,
  CURRENT_TIMESTAMP AS insertedOn,
  CURRENT_TIMESTAMP AS updatedOn,
  REPLACE(
    STRING(INT (OZF_RESALE_BATCHES_ALL.PARTNER_CUST_ACCOUNT_ID)),
    ",",
    ""
  )  AS accountId,
  OZF_RESALE_LINES_ALL.PURCHASE_PRICE AS ansellAcquisitionCost,
  NULL AS ansellAgreementName,
  NULL AS ansellContractPrice,
  NULL AS ansellCorrectedAgreementName,
  NULL AS approvalDate,
  OZF_RESALE_BATCHES_ALL.BATCH_NUMBER AS batchNumber,
  OZF_RESALE_BATCHES_ALL.STATUS_CODE AS batchStatusCode,
  OZF_RESALE_BATCHES_ALL.BATCH_TYPE AS batchType,
  OZF_RESALE_LINES_ALL.LINE_ATTRIBUTE10 AS claimComments,
  OZF_RESALE_LINES_ALL.QUANTITY AS claimedQuantity,
    ((CASE 
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BD' THEN PROD.piecesInBundle
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BG' THEN PROD.piecesInBag
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BX' THEN PROD.piecesInBox
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'CA' THEN PROD.piecesInCase
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'CT' THEN PROD.piecesInCarton
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'DP' THEN PROD.piecesInDisplayDispenser
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'DZ' THEN PROD.piecesInDozen
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'EA' THEN PROD.piecesInEach
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'GR' THEN PROD.piecesInGross
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PC' THEN 1
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PK' THEN PROD.piecesInPack
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PR' THEN 2
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'RL' THEN PROD.piecesInRoll
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'KT' THEN PROD.piecesInKit
END ) * OZF_RESALE_LINES_ALL.QUANTITY)

/ PROD.piecesInCase claimedQuantityCase,
  ((CASE 
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BD' THEN PROD.piecesInBundle
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BG' THEN PROD.piecesInBag
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'BX' THEN PROD.piecesInBox
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'CA' THEN PROD.piecesInCase
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'CT' THEN PROD.piecesInCarton
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'DP' THEN PROD.piecesInDisplayDispenser
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'DZ' THEN PROD.piecesInDozen
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'EA' THEN PROD.piecesInEach
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'GR' THEN PROD.piecesInGross
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PC' THEN 1
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PK' THEN PROD.piecesInPack
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'PR' THEN 2
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'RL' THEN PROD.piecesInRoll
WHEN OZF_RESALE_LINES_ALL.UOM_CODE = 'KT' THEN PROD.piecesInKit
END ) * OZF_RESALE_LINES_ALL.QUANTITY)

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
WHEN PROD.primarySellingUom  IN ('KT','KIT') THEN PROD.piecesInKit
END )   AS claimedQuantityPrimary,
  OZF_RESALE_LINES_ALL.UOM_CODE AS claimedUom,
  BIGINT(OZF_RESALE_LINES_ALL.RESALE_LINE_ID) AS claimId,
  '-1' AS claimNumber,
  NULL AS claimStatus,
  'N' AS contractEligible,
  OZF_RESALE_BATCHES_ALL.CURRENCY_CODE AS currency,
  CAST(NULL AS STRING) AS disputeFollowUpAction,
  CAST(NULL AS STRING) AS disputeReasonCode,
  CAST(NULL AS STRING) AS disputeReasonDescription,
  OZF_RESALE_BATCHES_ALL.partner_claim_number AS distributorClaimNumber,
  OZF_RESALE_LINES_ALL.Invoice_number AS distributorInvoiceNumber,
  OZF_RESALE_LINES_ALL.order_number AS distributorOrderNumber,
  REPLACE(
    STRING(INT (HZ_PARTIES.PARTY_ID)),
    ",",
    ""
  ) AS distributorPartyId,
  OZF_RESALE_LINES_ALL.orig_system_purchase_price AS distributorSubmittedAcquisitionCost,
  NULL AS distributorSubmittedEndUserActualSellingPrice,
  OZF_RESALE_LINES_ALL.bill_to_address AS distributorSubmittedEnduserAddress1,
  OZF_RESALE_LINES_ALL.line_attribute3 AS distributorSubmittedEnduserAddress2,
  OZF_RESALE_LINES_ALL.line_attribute4 AS distributorSubmittedEnduserAddress3,
  OZF_RESALE_LINES_ALL.bill_to_city AS distributorSubmittedEnduserCity,
  NULL AS distributorSubmittedEndUserContractNumber,
  OZF_RESALE_LINES_ALL.ORIG_SYSTEM_SELLING_PRICE AS distributorSubmittedEndUserContractPrice,
  OZF_RESALE_LINES_ALL.line_attribute11 AS distributorSubmittedEnduserId,
  OZF_RESALE_LINES_ALL.line_attribute7 AS distributorSubmittedEndUserName,
  OZF_RESALE_LINES_ALL.bill_to_state AS distributorSubmittedEndUserState,
  OZF_RESALE_LINES_ALL.bill_to_postal_code AS distributorSubmittedEndUserZipCode,
  
  case 
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 2 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 3 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 4 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 5 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 6 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 7 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 8 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 2 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 3 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 4 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 5 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 6 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 7 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 8 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   else trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)  end   AS distributorSubmittedEndUserZipCodeUpdated,
  
  
  OZF_RESALE_LINES_ALL.orig_system_item_number AS distributorSubmittedItemNumber,
  OZF_RESALE_LINES_ALL.line_attribute5 AS distributorSubmittedMemoNumber,
  NULL AS distributorSubmittedTotalClaimedRebateAmount,
  NULL AS distributorSubmittedUnitRebateAmount,
  REPLACE(STRING(INT (OZF_RESALE_LINES_ALL.BILL_TO_PARTY_ID)),
    ",",
    ""
  ) as endUserPartyId,
  
  (
    OZF_RESALE_LINES_ALL.END_USER_UNIT_PRICE * OZF_RESALE_LINES_ALL.quantity
  ) AS ebsEndUserSalesAmt,
  HP1.orig_system_reference AS endUserPrmsNumber,
  OZF_RESALE_LINES_ALL.LINE_ATTRIBUTE14 AS endUserProtectedPrice,
  HPS1.global_location_number AS endUserUniqueGlobalLocatorNumber,
  NULL AS exchangeRate,
  NULL AS exchangeRateUsd,
  OZF_RESALE_LINES_ALL.date_ordered AS dateInvoiced,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID)),
    ",",
    ""
  ) AS itemId,
  REPLACE(
    STRING(INT (OZF_RESALE_BATCHES_ALL.ORG_ID)),
    ",",
    ""
  ) AS owningBusinessUnitId,
  'TRACING' AS processingType,
  OZF_RESALE_BATCHES_ALL.report_date AS reportDate,
  OZF_RESALE_LINES_ALL.status_code AS resaleLineStatus,
  OZF_RESALE_LINES_ALL.resale_transfer_type AS reSaleTransferType,
  NULL salesRegion,
  NULL settlementDocumentNumber,
  NULL settlementDocumentType,
  NULL territoryId,
  CASE
    WHEN OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED')
      and NVL(OZF_RESALE_LINES_ALL.PURCHASE_PRICE,0) = 0
       then OZF_RESALE_LINES_ALL.QUANTITY * OZF_RESALE_LINES_ALL.ORIG_SYSTEM_PURCHASE_PRICE
    WHEN OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED')
      and NVL(OZF_RESALE_LINES_ALL.ORIG_SYSTEM_PURCHASE_PRICE,0) = 0
       then OZF_RESALE_LINES_ALL.QUANTITY * OZF_RESALE_LINES_ALL.ORIG_SYSTEM_SELLING_PRICE
    WHEN OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('CLOSED', 'REJECTED')
       then OZF_RESALE_LINES_ALL.QUANTITY * OZF_RESALE_LINES_ALL.PURCHASE_PRICE
    WHEN OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED')
      and nvl(OZF_RESALE_LINES_ALL.PURCHASE_PRICE,0) = 0 
        then OZF_RESALE_LINES_ALL.QUANTITY * OZF_RESALE_LINES_ALL.ORIG_SYSTEM_PURCHASE_PRICE
    WHEN OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED')
      and nvl(OZF_RESALE_LINES_ALL.ORIG_SYSTEM_PURCHASE_PRICE,0) = 0 
        then OZF_RESALE_LINES_ALL.QUANTITY * OZF_RESALE_LINES_ALL.ORIG_SYSTEM_SELLING_PRICE
    WHEN OZF_RESALE_BATCHES_ALL.STATUS_CODE in ('OPEN', 'DISPUTED', 'PROCESSED')    
        then OZF_RESALE_LINES_ALL.QUANTITY * OZF_RESALE_LINES_ALL.PURCHASE_PRICE
  END transactionAmount,
  NULL userId,
  MTL_SYSTEM_ITEMS_B.SEGMENT1 productCode,
  HZ_CUST_ACCOUNTS.ATTRIBUTE11 vertical,
  0 as ebsUnitRebateAmount,
  0 as ebsTotalAllowedRebate,
  0 as ebsTotalPaybackAmount,
  OZF_RESALE_BATCHES_ALL.ACCEPTED_AMOUNT as ebsAcceptedAmount,
  NULL rejectReason
FROM
  OZF_RESALE_LINES_ALL
  INNER JOIN EBS.OZF_RESALE_BATCH_LINE_MAPS_ALL ON OZF_RESALE_LINES_ALL.RESALE_LINE_ID = OZF_RESALE_BATCH_LINE_MAPS_ALL.RESALE_LINE_ID
  INNER JOIN EBS.OZF_RESALE_BATCHES_ALL ON OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID = OZF_RESALE_BATCH_LINE_MAPS_ALL.RESALE_BATCH_ID AND 
   OZF_RESALE_BATCHES_ALL.partner_cust_account_id     = OZF_RESALE_LINES_ALL.sold_from_cust_account_id  
    
    
    LEFT JOIN EBS.HZ_CUST_ACCOUNTS ON OZF_RESALE_BATCHES_ALL.PARTNER_CUST_ACCOUNT_ID = HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID AND OZF_RESALE_LINES_ALL.sold_from_cust_account_id   = HZ_CUST_ACCOUNTS.cust_account_id
    LEFT JOIN EBS.HZ_CUST_ACCT_SITES_ALL ON  HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID             = HZ_CUST_ACCT_SITES_ALL.CUST_ACCOUNT_ID 
    LEFT JOIN   EBS.HZ_CUST_SITE_USES_ALL ON  HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID          = HZ_CUST_SITE_USES_ALL.CUST_ACCT_SITE_ID AND OZF_RESALE_BATCHES_ALL.org_id = HZ_CUST_SITE_USES_ALL.org_id
      LEFT JOIN EBS.HZ_PARTIES ON HZ_CUST_ACCOUNTS.PARTY_ID = HZ_PARTIES.PARTY_ID 
      AND (
  CASE
    WHEN OZF_RESALE_BATCHES_ALL.partner_party_id != HZ_PARTIES.party_id
    THEN OZF_RESALE_BATCHES_ALL.partner_party_id
    ELSE HZ_PARTIES.party_id
  END)                 = OZF_RESALE_BATCHES_ALL.partner_party_id
       LEFT JOIN EBS.HZ_PARTY_SITES ON  HZ_PARTIES.PARTY_ID =  HZ_PARTY_SITES.PARTY_ID 
  LEFT JOIN EBS.MTL_SYSTEM_ITEMS_B ON MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID = OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID 
  LEFT JOIN EBS.HZ_PARTIES HP1 ON  HP1.PARTY_ID = OZF_RESALE_LINES_ALL.BILL_TO_PARTY_ID
   LEFT JOIN EBS.HZ_PARTY_SITES HPS1 ON  HPS1.PARTY_SITE_ID = OZF_RESALE_LINES_ALL.BILL_TO_PARTY_SITE_ID
   LEFT JOIN S_CORE.PRODUCT_EBS PROD ON OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID = PROD.ITEMID
WHERE
  1 = 1
  AND OZF_RESALE_BATCHES_ALL.STATUS_CODE IN ('CLOSED')
  AND OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING'
  AND MTL_SYSTEM_ITEMS_B.organization_id = 124 
  AND  HZ_CUST_ACCT_SITES_ALL.STATUS                     = 'A'
  AND HZ_CUST_SITE_USES_ALL.SITE_USE_CODE = 'BILL_TO'
   AND HZ_CUST_ACCT_SITES_ALL.BILL_TO_FLAG  = 'P'
  --AND OZF_RESALE_ADJUSTMENTS_ALL.LINE_AGREEMENT_FLAG   = 'T'
  AND HZ_PARTY_SITES.IDENTIFYING_ADDRESS_FLAG = 'Y'
  --AND OZF_CLAIMS_ALL.STATUS_CODE <> 'CANCELLED'
  --and OZF_RESALE_LINES_ALL.resale_line_id = 781542.0 


""")

main_tracing = main_tracing.transform(apply_schema(schema))
main_tracing.createOrReplaceTempView('main_tracing')

# COMMAND ----------

main_stage = spark.sql("""
  SELECT * FROM main_chargeback --where claimid = '39094699'
  UNION ALL
  SELECT * FROM main_disputed --where claimid = '39094699'
  UNION ALL
  SELECT * FROM main_tracing --where claimid = '39094699'
""")

main_stage.createOrReplaceTempView('main_stage')

# COMMAND ----------

main = spark.sql("""
select
  MAIN.createdBy,
  MAIN.createdOn,
  MAIN.modifiedBy,
  MAIN.modifiedOn,
  MAIN.insertedOn,
  MAIN.updatedOn,
  MAIN.accountId,
  MAIN.ansellAcquisitionCost,
  MAIN.ansellAgreementName,
  CAST(COALESCE(MAIN.ansellContractPrice,0) AS decimal(22,7)) as ansellContractPrice,
  MAIN.ansellCorrectedAgreementName,
  MAIN.approvalDate,
  MAIN.batchNumber,
  MAIN.batchStatusCode,
  COALESCE(MAIN.batchType, 'unknown') AS batchType,
  MAIN.claimComments,
  MAIN.claimedQuantity,
  MAIN.claimedQuantityCase,
  MAIN.claimedQuantityPrimary,
  COALESCE(MAIN.claimedUom, 'unknown') AS claimedUom,
  COALESCE(MAIN.claimId, 'unknown') AS claimId,
  COALESCE(MAIN.claimNumber, 'unknown') AS claimNumber,
  MAIN.claimStatus,
  MAIN.contractEligible,
  MAIN.currency,
  Date_format(CASE 
		WHEN YEAR(MAIN.dateInvoiced) < 2000
			THEN CAST(CAST(YEAR(MAIN.dateInvoiced) + 2000 AS STRING) || '-' || DATE_FORMAT(MAIN.dateInvoiced, 'MM-dd') AS DATE)
		ELSE MAIN.dateInvoiced
	END,'yyyy-MM-dd') AS dateInvoiced,
  MAIN.disputeFollowUpAction,
  MAIN.disputeReasonCode,
  MAIN.disputeReasonDescription,
  MAIN.distributorClaimNumber,
  MAIN.distributorInvoiceNumber,
  MAIN.distributorOrderNumber,
  MAIN.distributorPartyId,
  MAIN.distributorSubmittedAcquisitionCost,
  MAIN.distributorSubmittedEndUserActualSellingPrice,
  MAIN.distributorSubmittedEnduserAddress1,
  MAIN.distributorSubmittedEnduserAddress2,
  MAIN.distributorSubmittedEnduserAddress3,
  COALESCE(MAIN.distributorSubmittedEnduserCity, 'unknown') AS distributorSubmittedEnduserCity,
  MAIN.distributorSubmittedEndUserContractNumber,
  MAIN.distributorSubmittedEndUserContractPrice,
  MAIN.distributorSubmittedEnduserId,
  COALESCE(MAIN.distributorSubmittedEndUserName, 'unknown') AS distributorSubmittedEndUserName,
  COALESCE(MAIN.distributorSubmittedEndUserState, 'unknown') AS distributorSubmittedEndUserState,
  --distributorSubmittedEndUserZipCode,
  COALESCE(
    MAIN.distributorSubmittedEndUserZipCodeUpdated,
    'unknown'
  ) AS distributorSubmittedEndUserZipCode,
  COALESCE(MAIN.distributorSubmittedItemNumber, 'unknown') AS distributorSubmittedItemNumber,
  MAIN.distributorSubmittedMemoNumber,
  MAIN.distributorSubmittedTotalClaimedRebateAmount,
  MAIN.distributorSubmittedUnitRebateAmount,
  MAIN.endUserPartyId,
  MAIN.ebsAcceptedAmount,
  MAIN.ebsEndUserSalesAmt,
  MAIN.ebsTotalAllowedRebate,
  MAIN.ebsTotalPaybackAmount,
  MAIN.ebsUnitRebateAmount,
  MAIN.endUserPrmsNumber,
  MAIN.endUserProtectedPrice,
  MAIN.endUserUniqueGlobalLocatorNumber,
  MAIN.exchangeRate,
  MAIN.exchangeRateUsd,
  COALESCE(
    PROCON_PAM_CONTRACTS.LASTAPPROVER,
    PROCON_CONTRACTS.CONTRACT_APPROVER
  ) AS gpoContractApprover,
  COALESCE(
    PROCON_PAM_CONTRACTS.ENDUSER,
    PROCON_CONTRACTS.DESCRIPTION
  ) AS gpoContractDescription,
  COALESCE(
    PROCON_PAM_CONTRACTS.CONTRACTID,
    PROCON_CONTRACTS.CONTRACT_NUMBER
  ) AS gpoContractNumber,
  COALESCE(
    PROCON_PAM_CONTRACTS.CONTRACTTYPE,
    PROCON_CONTRACTS.CONTRACT_TYPE
  ) AS gpoContractType,
  COALESCE(
    PROCON_PAM_CONTRACTS.ENDDATE,
    PROCON_CONTRACTS.EXPIRATION_DATE
  ) AS gpoExporationDate,
  COALESCE(
    PROCON_PAM_CONTRACTS.GPO,
    PROCON_CONTRACTS.GPO_ID
  ) AS gpoId,
  COALESCE(
    PROCON_PAM_CONTRACTS.OWNER,
    PROCON_CONTRACTS.INITIATOR
  ) AS gpoInitiator,
  COALESCE(
    PROCON_PAM_CONTRACTS.STARTDATE,
    PROCON_CONTRACTS.START_DATE
  ) AS gpoStartDate,
  COALESCE(
    PROCON_PAM_CONTRACTS.CONTRACTSTATUS,
    PROCON_CONTRACTS.STATUS
  ) AS gpoStatus,
  COALESCE(MAIN.itemId, 'unknown') AS itemId,
  'unknown' marketLevel1,
  'unknown' marketLevel2,
  'unknown' marketLevel3,
  MAIN.owningBusinessUnitId,
  COALESCE(EXCLUSION_LIST.ITDOTD,'Exclude') AS posFlag, 
  'TM' AS posSource,
  MAIN.processingType,
  MAIN.rejectReason,
  Date_format(CASE 
		WHEN YEAR(MAIN.reportDate) < 2000
			THEN CAST(CAST(YEAR(MAIN.reportDate) + 2000 AS STRING) || '-' || DATE_FORMAT(MAIN.reportDate, 'MM-dd') AS DATE)
		ELSE MAIN.reportDate
	END,'yyyy-MM-dd') AS reportDate,
  MAIN.resaleLineStatus,
  MAIN.reSaleTransferType,
  ZIP3.sales_region AS salesRegion,
  SECONDORG_ZIP3.sales_region AS salesRegionOrg2,
  THIRDORG_ZIP3.sales_region AS salesRegionOrg3,
  MAIN.settlementDocumentNumber,
  MAIN.settlementDocumentType,
  COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID,
    'unknown'
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
  case
when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice >= 500  and MAIN.disputeReasonCode is null     then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice <= -500 and MAIN.disputeReasonCode is null     then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice >= 500  and MAIN.disputeReasonCode is not null then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice <= -500 and MAIN.disputeReasonCode is not null then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING'    and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice >= 500  and MAIN.disputeReasonCode is null     then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING'    and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice <= -500 and MAIN.disputeReasonCode is null     then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING'    and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice >= 500  and MAIN.disputeReasonCode is not null then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING'    and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice <= -500 and MAIN.disputeReasonCode is not null then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice

when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice = 3  and MAIN.disputeReasonCode is null     then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice = 3  and MAIN.disputeReasonCode is not null then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING'    and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice = 3 and MAIN.disputeReasonCode is not null then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING'    and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT', 'OPEN', 'DISPUTED', 'PROCESSED', 'PROCESSING') and MAIN.ansellContractPrice = 3  and MAIN.disputeReasonCode is not null then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice

when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT') and MAIN.disputeReasonCode is null and NVL( MAIN.ansellContractPrice,0) = 0 then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice 
when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT') and MAIN.disputeReasonCode is null then MAIN.claimedQuantity * MAIN.ansellContractPrice

when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('OPEN', 'DISPUTED', 'PROCESSED') and MAIN.disputeReasonCode is null and NVL (MAIN.ansellContractPrice,0) =0 then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('OPEN', 'DISPUTED', 'PROCESSED') and MAIN.disputeReasonCode is NOT null then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice

when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('OPEN', 'DISPUTED', 'PROCESSED') and MAIN.disputeReasonCode is null then MAIN.claimedQuantity * MAIN.ansellContractPrice
when MAIN.batchType = 'CHARGEBACK' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED', 'PENDING_PAYMENT') and MAIN.disputeReasonCode is NOT null then 0
when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED') and MAIN.disputeReasonCode is null and NVL(MAIN.ansellAcquisitionCost,0) = 0 then MAIN.claimedQuantity * MAIN.distributorSubmittedAcquisitionCost
when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED') and MAIN.disputeReasonCode is null and NVL(MAIN.distributorSubmittedAcquisitionCost,0) = 0 then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED') and MAIN.disputeReasonCode is null then MAIN.claimedQuantity * MAIN.ansellAcquisitionCost
when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('PENDING_CLOSE','CLOSED', 'REJECTED') and MAIN.disputeReasonCode is NOT null then 0

when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('OPEN', 'DISPUTED', 'PROCESSED') and MAIN.disputeReasonCode is null and NVL (MAIN.ansellAcquisitionCost,0) =0 then MAIN.claimedQuantity * MAIN.distributorSubmittedAcquisitionCost 
when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('OPEN', 'DISPUTED', 'PROCESSED') and MAIN.disputeReasonCode is null and NVL (MAIN.distributorSubmittedAcquisitionCost,0) = 0 then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('OPEN', 'DISPUTED', 'PROCESSED') and MAIN.disputeReasonCode is null then MAIN.claimedQuantity * MAIN.ansellAcquisitionCost

when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('OPEN', 'DISPUTED', 'PROCESSED') and MAIN.disputeReasonCode is NOT null then MAIN.claimedQuantity * MAIN.distributorSubmittedAcquisitionCost
when MAIN.batchType = 'TRACING' and MAIN.batchStatusCode in ('OPEN', 'DISPUTED', 'PROCESSED') and MAIN.disputeReasonCode is NOT null and NVL(MAIN.distributorSubmittedAcquisitionCost,0) = 0 then MAIN.claimedQuantity * MAIN.distributorSubmittedEndUserContractPrice
when MAIN.batchType = 'TRACING'    and MAIN.batchStatusCode in ('PENDING_PAYMENT')            then 0
else 99999999 
end AS  transactionAmount,
  COALESCE(TERR_DEFAULT.userid, 'unknown') AS userId,
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
  COALESCE(main.VERTICAL, 'unknown') AS vertical
from
  main_stage main --LEFT JOIN S_CORE.organization_agg ORG ON upper(ORG.name) = upper(main.COMPANY)
  LEFT JOIN ZIP3 ON substr(
    main.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = ZIP3.zip3
  AND main.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON MAIN.vertical = ZIP0.vertical
  LEFT JOIN SECONDORG_ZIP3 ON substr(
    main.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = SECONDORG_ZIP3.zip3
  AND main.vertical = SECONDORG_ZIP3.vertical
  LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON main.PRODUCTCODE = PROD_OVERRIDE.PRODUCTID
  LEFT JOIN ebs.HZ_CUST_ACCOUNTS HZ_ACC ON MAIN.ACCOUNTID = HZ_ACC.CUST_ACCOUNT_ID
  LEFT JOIN EBS.HZ_PARTIES PARTY ON HZ_ACC.PARTY_ID = PARTY.PARTY_ID
  LEFT JOIN CUST_OVERRIDE ON PARTY.PARTY_NUMBER = CUST_OVERRIDE.custid
  LEFT JOIN SECONDORG_PROD_OVERRIDE ON MAIN.PRODUCTCODE = SECONDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN SECONDORG_CUST_OVERRIDE ON PARTY.PARTY_NUMBER = SECONDORG_CUST_OVERRIDE.custid
  LEFT JOIN TERR_DEFAULT ON MAIN.vertical = TERR_DEFAULT.vertical
  AND SUBSTR(
    MAIN.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = TERR_DEFAULT.zip3
  LEFT JOIN THIRDORG_PROD_OVERRIDE ON main.PRODUCTCODE = THIRDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN THIRDORG_CUST_OVERRIDE ON PARTY.PARTY_NUMBER = THIRDORG_CUST_OVERRIDE.custid
  LEFT JOIN THIRDORG_ZIP3 ON substr(
    MAIN.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = THIRDORG_ZIP3.zip3
  LEFT JOIN PROCON_CONTRACTS ON MAIN.ansellCorrectedAgreementName = PROCON_CONTRACTS.CONTRACT_NUMBER
  LEFT JOIN PROCON_PAM_CONTRACTS ON MAIN.ansellCorrectedAgreementName = PROCON_PAM_CONTRACTS.CONTRACTID
  LEFT JOIN EXCLUSION_LIST ON MAIN.distributorPartyId = EXCLUSION_LIST.PARTYID AND MAIN.ACCOUNTID = EXCLUSION_LIST.ACCOUNTID
  

""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# PERSIST TRANSFORMATIONS
file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name, target_container, target_storage)
main.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
main_2 = spark.read.format('delta').load(target_file)

# COMMAND ----------

# TRANSFORM DATA
main_3 = main_2.filter('dateInvoiced IS NOT NULL')

# COMMAND ----------

columns = list(schema.keys())

key_columns = ['processingType', 'batchType', 'claimId', 'claimNumber', 'territoryId', 'userid', 'vertical', 'distributorSubmittedEndUserZipCode', 'dateInvoiced', 'claimedUom', 'itemId', 'distributorSubmittedEndUserState', 'distributorSubmittedEnduserCity', 'distributorSubmittedEndUserName', 'distributorSubmittedItemNumber', 'marketLevel1', 'marketLevel2', 'marketLevel3', 'posSource', 'ansellContractPrice']

# COMMAND ----------

# DROP DUPLICATES
main_4 = remove_duplicate_rows(main_3, key_columns, table_name, 
  source_name, NOTEBOOK_NAME, NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main_4
  .transform(tg_default(source_name)) 
  .transform(tg_trade_management_point_of_sales())
  .transform(attach_dataset_column('TM'))
  .transform(parse_timestamp(['dateInvoiced','reportDate'], expected_format = 'yyyy-MM-dd'))
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

handle_delete1 = spark.sql("""
SELECT DISTINCT
MAIN.ansellContractPrice,
MAIN.batchType,
MAIN.claimedUom,
MAIN.claimId,
MAIN.claimNumber,
MAIN.dateInvoiced,
MAIN.distributorSubmittedEnduserCity,
MAIN.distributorSubmittedEndUserName,
MAIN.distributorSubmittedEndUserState,
MAIN.distributorSubmittedEndUserZipCodeUpdated,
MAIN.distributorSubmittedItemNumber,
MAIN.itemId,
MAIN.marketLevel1,
MAIN.marketLevel2,
MAIN.marketLevel3,
MAIN.processingType,
  COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID,
    'unknown'
  ) AS territoryId,
 COALESCE(TERR_DEFAULT.userid, 'unknown') AS userId,
MAIN.vertical,
'TM' posSource
FROM 
(

SELECT
OZF_RESALE_ADJUSTMENTS_ALL.CALCULATED_PRICE AS ansellContractPrice,
OZF_RESALE_BATCHES_ALL.BATCH_TYPE AS batchType,
OZF_RESALE_LINES_ALL.UOM_CODE AS claimedUom,
BIGINT(OZF_RESALE_LINES_ALL.RESALE_LINE_ID) claimId,
OZF_CLAIMS_ALL.CLAIM_NUMBER AS  claimNumber,
OZF_RESALE_LINES_ALL.date_ordered AS  dateInvoiced,
  OZF_RESALE_LINES_ALL.bill_to_city AS distributorSubmittedEnduserCity,
  OZF_RESALE_LINES_ALL.line_attribute7 AS distributorSubmittedEndUserName,
  OZF_RESALE_LINES_ALL.bill_to_state AS distributorSubmittedEndUserState,
  OZF_RESALE_LINES_ALL.bill_to_postal_code AS distributorSubmittedEndUserZipCode,
  OZF_RESALE_LINES_ALL.orig_system_item_number AS distributorSubmittedItemNumber,
REPLACE(
    STRING(INT (OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID)),
    ",",
    ""
  ) AS itemId,
'unknown' as marketLevel1,
'unknown' as marketLevel2,
'unknown' as marketLevel3,
'PROCESSED' as processingType,
NULL AS  territoryId,
NULL AS userid,
HZ_CUST_ACCOUNTS.ATTRIBUTE11 vertical,
 case 
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 2 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 3 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 4 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 5 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 6 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 7 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 8 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 2 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 3 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 4 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 5 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 6 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 7 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 8 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   else trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)  end   AS distributorSubmittedEndUserZipCodeUpdated,
  REPLACE(
    STRING(INT (OZF_CLAIMS_ALL.CUST_ACCOUNT_ID)),
    ",",
    ""
  )  AS accountId,
  MTL_SYSTEM_ITEMS_B.SEGMENT1 productCode
FROM   EBS.OZF_RESALE_LINES_ALL
  INNER JOIN OZF_RESALE_ADJUSTMENTS_ALL ON OZF_RESALE_LINES_ALL.RESALE_LINE_ID = OZF_RESALE_ADJUSTMENTS_ALL.RESALE_LINE_ID
  INNER JOIN EBS.OZF_RESALE_BATCHES_ALL ON OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID = OZF_RESALE_ADJUSTMENTS_ALL.RESALE_BATCH_ID
  LEFT JOIN EBS.MTL_SYSTEM_ITEMS_B ON MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID = OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID
  LEFT JOIN EBS.OZF_CLAIMS_ALL ON OZF_CLAIMS_ALL.SOURCE_OBJECT_ID = OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID
  LEFT JOIN EBS.HZ_CUST_ACCOUNTS ON OZF_CLAIMS_ALL.CUST_ACCOUNT_ID = HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID
  LEFT JOIN EBS.HZ_PARTIES ON HZ_CUST_ACCOUNTS.PARTY_ID = HZ_PARTIES.PARTY_ID
  LEFT JOIN EBS.HZ_PARTY_SITES ON HZ_PARTIES.PARTY_ID = HZ_PARTY_SITES.PARTY_ID
  LEFT JOIN OZF_SETTLEMENT_DOCS_ALL ON OZF_CLAIMS_ALL.CLAIM_ID = OZF_SETTLEMENT_DOCS_ALL.CLAIM_ID
  LEFT JOIN  EBS.HZ_PARTIES EU_HP ON OZF_RESALE_LINES_ALL.BILL_TO_PARTY_ID      = EU_HP.PARTY_ID
  LEFT JOIN   EBS.HZ_PARTY_SITES EU_HPS ON OZF_RESALE_LINES_ALL.BILL_TO_PARTY_SITE_ID = EU_HPS.PARTY_SITE_ID
  LEFT JOIN S_CORE.PRODUCT_EBS PROD ON OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID = PROD.ITEMID
WHERE
  1 = 1
  AND OZF_RESALE_BATCHES_ALL.STATUS_CODE IN ('CLOSED')
  AND OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'CHARGEBACK'
  AND MTL_SYSTEM_ITEMS_B.organization_id = 124
  AND OZF_RESALE_ADJUSTMENTS_ALL.LINE_AGREEMENT_FLAG = 'T'
  AND HZ_PARTY_SITES.IDENTIFYING_ADDRESS_FLAG = 'Y'
  AND OZF_CLAIMS_ALL.STATUS_CODE <> 'CANCELLED'
  --AND OZF_RESALE_LINES_ALL.RESALE_LINE_ID='8120542'

)MAIN
 LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON main.PRODUCTCODE = PROD_OVERRIDE.PRODUCTID
 LEFT JOIN ebs.HZ_CUST_ACCOUNTS HZ_ACC ON MAIN.ACCOUNTID = HZ_ACC.CUST_ACCOUNT_ID
  LEFT JOIN EBS.HZ_PARTIES PARTY ON HZ_ACC.PARTY_ID = PARTY.PARTY_ID
  LEFT JOIN CUST_OVERRIDE ON PARTY.PARTY_NUMBER = CUST_OVERRIDE.custid
 LEFT JOIN ZIP3 ON substr(
    main.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = ZIP3.zip3
  AND main.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON MAIN.vertical = ZIP0.vertical
 LEFT JOIN TERR_DEFAULT ON MAIN.vertical = TERR_DEFAULT.vertical
  AND SUBSTR(
    MAIN.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = TERR_DEFAULT.zip3
  """)

handle_delete1.createOrReplaceTempView('handle_delete1')

# COMMAND ----------

handle_delete2 = spark.sql("""
SELECT DISTINCT
MAIN.ansellContractPrice,
MAIN.batchType,
MAIN.claimedUom,
MAIN.claimId,
MAIN.claimNumber,
MAIN.dateInvoiced,
MAIN.distributorSubmittedEnduserCity,
MAIN.distributorSubmittedEndUserName,
MAIN.distributorSubmittedEndUserState,
MAIN.distributorSubmittedEndUserZipCodeUpdated,
MAIN.distributorSubmittedItemNumber,
MAIN.itemId,
MAIN.marketLevel1,
MAIN.marketLevel2,
MAIN.marketLevel3,
MAIN.processingType,
  COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID,
    'unknown'
  ) AS territoryId,
 COALESCE(TERR_DEFAULT.userid, 'unknown') AS userId,
MAIN.vertical,
'TM' posSource
FROM 
(
SELECT
OZF_RESALE_LINES_INT_ALL.CALCULATED_PRICE AS ansellContractPrice,
OZF_RESALE_BATCHES_ALL.BATCH_TYPE AS batchType,
  OZF_RESALE_LINES_INT_ALL.UOM_CODE AS claimedUom,
  BIGINT(OZF_RESALE_LINES_INT_ALL.RESALE_LINE_INT_ID) AS claimId,
  '-1' AS claimNumber,
  OZF_RESALE_LINES_INT_ALL.date_ordered AS dateInvoiced,
  OZF_RESALE_LINES_INT_ALL.bill_to_city AS distributorSubmittedEnduserCity,
  OZF_RESALE_LINES_INT_ALL.line_attribute7 AS distributorSubmittedEndUserName,
  OZF_RESALE_LINES_INT_ALL.bill_to_state AS distributorSubmittedEndUserState,
  OZF_RESALE_LINES_INT_ALL.bill_to_postal_code AS distributorSubmittedEndUserZipCode,
  OZF_RESALE_LINES_INT_ALL.orig_system_item_number AS distributorSubmittedItemNumber,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_INT_ALL.INVENTORY_ITEM_ID)),
    ",",
    ""
  )AS itemId,
'unknown' as marketLevel1,
'unknown' as marketLevel2,
'unknown' as marketLevel3,
'DISPUTED' as processingType,
NULL AS  territoryId,
NULL AS userid,
HZ_CUST_ACCOUNTS.ATTRIBUTE11 vertical,
  case 
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 2 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 3 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 4 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 5 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' ||  substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 6 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 7 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1))) = 8 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 2 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 3 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 4 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 5 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 6 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 7 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)) = 8 and OZF_RESALE_LINES_INT_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)
   else trim(OZF_RESALE_LINES_INT_ALL.bill_to_postal_code)  end  AS distributorSubmittedEndUserZipCodeUpdated,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_INT_ALL.SOLD_FROM_CUST_ACCOUNT_ID)),
    ",",
    ""
  )  AS accountId,
  MTL_SYSTEM_ITEMS_B.SEGMENT1 productCode
FROM
  EBS.OZF_RESALE_LINES_INT_ALL 
  INNER JOIN EBS.OZF_RESALE_BATCHES_ALL ON OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID = OZF_RESALE_LINES_INT_ALL.RESALE_BATCH_ID
  --LEFT JOIN OZF_LOOKUPS ON OZF_RESALE_LINES_INT_ALL.FOLLOWUP_ACTION_CODE = OZF_LOOKUPS.LOOKUP_CODE
  --LEFT JOIN DIS ON DIS.lookup_code = OZF_RESALE_LINES_INT_ALL.dispute_code
  LEFT JOIN MTL_SYSTEM_ITEMS_B ON MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID = OZF_RESALE_LINES_INT_ALL.INVENTORY_ITEM_ID 
  LEFT JOIN EBS.HZ_CUST_ACCOUNTS ON OZF_RESALE_LINES_INT_ALL.sold_from_cust_account_id = HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID
  LEFT JOIN EBS.HZ_PARTIES ON HZ_CUST_ACCOUNTS.PARTY_ID = HZ_PARTIES.PARTY_ID
  LEFT JOIN EBS.HZ_PARTIES HP2 ON HP2.party_id = OZF_RESALE_LINES_INT_ALL.bill_to_party_id
   LEFT JOIN EBS.HZ_PARTY_SITES END_USER_HPS ON   END_USER_HPS.PARTY_SITE_ID =  OZF_RESALE_LINES_INT_ALL.BILL_TO_PARTY_SITE_ID
   LEFT JOIN S_CORE.PRODUCT_EBS PROD ON OZF_RESALE_LINES_INT_ALL.INVENTORY_ITEM_ID = PROD.ITEMID
WHERE
  1 = 1
  AND OZF_RESALE_BATCHES_ALL.STATUS_CODE <> ('CLOSED')
  AND OZF_RESALE_BATCHES_ALL.purge_flag is null
  AND OZF_RESALE_BATCHES_ALL.BATCH_TYPE in ('TRACING', 'CHARGEBACK')
  --AND OZF_RESALE_LINES_INT_ALL.RESALE_LINE_INT_ID='8120542'

)MAIN
 LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON main.PRODUCTCODE = PROD_OVERRIDE.PRODUCTID
 LEFT JOIN ebs.HZ_CUST_ACCOUNTS HZ_ACC ON MAIN.ACCOUNTID = HZ_ACC.CUST_ACCOUNT_ID
  LEFT JOIN EBS.HZ_PARTIES PARTY ON HZ_ACC.PARTY_ID = PARTY.PARTY_ID
  LEFT JOIN CUST_OVERRIDE ON PARTY.PARTY_NUMBER = CUST_OVERRIDE.custid
 LEFT JOIN ZIP3 ON substr(
    main.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = ZIP3.zip3
  AND main.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON MAIN.vertical = ZIP0.vertical
 LEFT JOIN TERR_DEFAULT ON MAIN.vertical = TERR_DEFAULT.vertical
  AND SUBSTR(
    MAIN.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = TERR_DEFAULT.zip3
  """)

handle_delete2.createOrReplaceTempView('handle_delete2')

# COMMAND ----------

handle_delete3 = spark.sql("""
SELECT DISTINCT
MAIN.ansellContractPrice,
MAIN.batchType,
MAIN.claimedUom,
MAIN.claimId,
MAIN.claimNumber,
MAIN.dateInvoiced,
MAIN.distributorSubmittedEnduserCity,
MAIN.distributorSubmittedEndUserName,
MAIN.distributorSubmittedEndUserState,
MAIN.distributorSubmittedEndUserZipCodeUpdated,
MAIN.distributorSubmittedItemNumber,
MAIN.itemId,
MAIN.marketLevel1,
MAIN.marketLevel2,
MAIN.marketLevel3,
MAIN.processingType,
  COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID,
    'unknown'
  ) AS territoryId,
 COALESCE(TERR_DEFAULT.userid, 'unknown') AS userId,
MAIN.vertical,
'TM' posSource
FROM 
(

SELECT
  NULL AS ansellContractPrice,
  OZF_RESALE_BATCHES_ALL.BATCH_TYPE AS batchType,
   OZF_RESALE_LINES_ALL.UOM_CODE AS claimedUom,
  BIGINT(OZF_RESALE_LINES_ALL.RESALE_LINE_ID) AS claimId,
  '-1' AS claimNumber,
  OZF_RESALE_LINES_ALL.date_ordered AS dateInvoiced,
 OZF_RESALE_LINES_ALL.bill_to_city AS distributorSubmittedEnduserCity,
  OZF_RESALE_LINES_ALL.line_attribute7 AS distributorSubmittedEndUserName,
  OZF_RESALE_LINES_ALL.bill_to_state AS distributorSubmittedEndUserState,
  OZF_RESALE_LINES_ALL.bill_to_postal_code AS distributorSubmittedEndUserZipCode,
  OZF_RESALE_LINES_ALL.orig_system_item_number AS distributorSubmittedItemNumber,
  REPLACE(
    STRING(INT (OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID)),
    ",",
    ""
  ) AS itemId,
'unknown' as marketLevel1,
'unknown' as marketLevel2,
'unknown' as marketLevel3,
'TRACING'  as processingType,
NULL AS  territoryId,
NULL AS userid,
  HZ_CUST_ACCOUNTS.ATTRIBUTE11 vertical,
 case 
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 2 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 3 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 4 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 5 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 6 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 7 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') > 0 and length(trim(substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1))) = 8 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' ||  substr(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code),1, instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-')-1)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 2 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 3 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 4 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 5 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 6 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 7 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '00' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   when instr(OZF_RESALE_LINES_ALL.bill_to_postal_code, '-') = 0 and length(trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)) = 8 and OZF_RESALE_LINES_ALL.bill_to_postal_code < 'A' then '0' || trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)
   else trim(OZF_RESALE_LINES_ALL.bill_to_postal_code)  end   AS distributorSubmittedEndUserZipCodeUpdated,
  REPLACE(
    STRING(INT (OZF_RESALE_BATCHES_ALL.PARTNER_CUST_ACCOUNT_ID)),
    ",",
    ""
  )  AS accountId,
  MTL_SYSTEM_ITEMS_B.SEGMENT1 productCode

FROM
  EBS.OZF_RESALE_LINES_ALL
  INNER JOIN EBS.OZF_RESALE_BATCH_LINE_MAPS_ALL ON OZF_RESALE_LINES_ALL.RESALE_LINE_ID = OZF_RESALE_BATCH_LINE_MAPS_ALL.RESALE_LINE_ID
  INNER JOIN EBS.OZF_RESALE_BATCHES_ALL ON OZF_RESALE_BATCHES_ALL.RESALE_BATCH_ID = OZF_RESALE_BATCH_LINE_MAPS_ALL.RESALE_BATCH_ID AND 
   OZF_RESALE_BATCHES_ALL.partner_cust_account_id     = OZF_RESALE_LINES_ALL.sold_from_cust_account_id  
    
    
    LEFT JOIN EBS.HZ_CUST_ACCOUNTS ON OZF_RESALE_BATCHES_ALL.PARTNER_CUST_ACCOUNT_ID = HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID AND OZF_RESALE_LINES_ALL.sold_from_cust_account_id   = HZ_CUST_ACCOUNTS.cust_account_id
    LEFT JOIN EBS.HZ_CUST_ACCT_SITES_ALL ON  HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID             = HZ_CUST_ACCT_SITES_ALL.CUST_ACCOUNT_ID 
    LEFT JOIN   EBS.HZ_CUST_SITE_USES_ALL ON  HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID          = HZ_CUST_SITE_USES_ALL.CUST_ACCT_SITE_ID AND OZF_RESALE_BATCHES_ALL.org_id = HZ_CUST_SITE_USES_ALL.org_id
      LEFT JOIN EBS.HZ_PARTIES ON HZ_CUST_ACCOUNTS.PARTY_ID = HZ_PARTIES.PARTY_ID 
      AND (
  CASE
    WHEN OZF_RESALE_BATCHES_ALL.partner_party_id != HZ_PARTIES.party_id
    THEN OZF_RESALE_BATCHES_ALL.partner_party_id
    ELSE HZ_PARTIES.party_id
  END)                 = OZF_RESALE_BATCHES_ALL.partner_party_id
       LEFT JOIN EBS.HZ_PARTY_SITES ON  HZ_PARTIES.PARTY_ID =  HZ_PARTY_SITES.PARTY_ID 
  LEFT JOIN EBS.MTL_SYSTEM_ITEMS_B ON MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID = OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID 
  LEFT JOIN EBS.HZ_PARTIES HP1 ON  HP1.PARTY_ID = OZF_RESALE_LINES_ALL.BILL_TO_PARTY_ID
   LEFT JOIN EBS.HZ_PARTY_SITES HPS1 ON  HPS1.PARTY_SITE_ID = OZF_RESALE_LINES_ALL.BILL_TO_PARTY_SITE_ID
   LEFT JOIN S_CORE.PRODUCT_EBS PROD ON OZF_RESALE_LINES_ALL.INVENTORY_ITEM_ID = PROD.ITEMID
WHERE
  1 = 1
  AND OZF_RESALE_BATCHES_ALL.STATUS_CODE IN ('CLOSED')
  AND OZF_RESALE_BATCHES_ALL.BATCH_TYPE = 'TRACING'
  AND MTL_SYSTEM_ITEMS_B.organization_id = 124 
  AND  HZ_CUST_ACCT_SITES_ALL.STATUS                     = 'A'
  AND HZ_CUST_SITE_USES_ALL.SITE_USE_CODE = 'BILL_TO'
   AND HZ_CUST_ACCT_SITES_ALL.BILL_TO_FLAG  = 'P'
  AND HZ_PARTY_SITES.IDENTIFYING_ADDRESS_FLAG = 'Y'
  --AND OZF_RESALE_LINES_ALL.RESALE_LINE_ID='8120542'

)MAIN
 LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON main.PRODUCTCODE = PROD_OVERRIDE.PRODUCTID
 LEFT JOIN ebs.HZ_CUST_ACCOUNTS HZ_ACC ON MAIN.ACCOUNTID = HZ_ACC.CUST_ACCOUNT_ID
  LEFT JOIN EBS.HZ_PARTIES PARTY ON HZ_ACC.PARTY_ID = PARTY.PARTY_ID
  LEFT JOIN CUST_OVERRIDE ON PARTY.PARTY_NUMBER = CUST_OVERRIDE.custid
 LEFT JOIN ZIP3 ON substr(
    main.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = ZIP3.zip3
  AND main.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON MAIN.vertical = ZIP0.vertical
 LEFT JOIN TERR_DEFAULT ON MAIN.vertical = TERR_DEFAULT.vertical
  AND SUBSTR(
    MAIN.distributorSubmittedEndUserZipCodeUpdated,
    1,
    3
  ) = TERR_DEFAULT.zip3
  """)

handle_delete3.createOrReplaceTempView('handle_delete3')

# COMMAND ----------

handle_delete = spark.sql("""
select 
ansellContractPrice AS ansellContractPrice,
COALESCE(batchType, 'unknown') AS batchType,
COALESCE(claimedUom, 'unknown') AS claimedUom,
COALESCE(claimId, 'unknown') AS claimId,
COALESCE(claimNumber, 'unknown') AS claimNumber,
COALESCE(dateInvoiced,to_date('1900-01-01','yyyy-MM-dd')) dateInvoiced,
COALESCE(distributorSubmittedEnduserCity, 'unknown') AS distributorSubmittedEnduserCity,
COALESCE(distributorSubmittedEndUserName, 'unknown') AS distributorSubmittedEndUserName,
COALESCE(distributorSubmittedEndUserState, 'unknown') AS distributorSubmittedEndUserState,
COALESCE(distributorSubmittedEndUserZipCodeUpdated,    'unknown'  ) AS distributorSubmittedEndUserZipCode,
COALESCE(distributorSubmittedItemNumber, 'unknown') AS distributorSubmittedItemNumber,
COALESCE(itemId, 'unknown') AS itemId,
'unknown' marketLevel1,
'unknown' marketLevel2,
'unknown' marketLevel3,
processingType,
territoryId,
userid,
COALESCE(VERTICAL, 'unknown') AS vertical,
posSource from handle_delete1
union
select 
ansellContractPrice AS ansellContractPrice,
COALESCE(batchType, 'unknown') AS batchType,
COALESCE(claimedUom, 'unknown') AS claimedUom,
COALESCE(claimId, 'unknown') AS claimId,
COALESCE(claimNumber, 'unknown') AS claimNumber,
COALESCE(dateInvoiced,to_date('1900-01-01','yyyy-MM-dd')) dateInvoiced,
COALESCE(distributorSubmittedEnduserCity, 'unknown') AS distributorSubmittedEnduserCity,
COALESCE(distributorSubmittedEndUserName, 'unknown') AS distributorSubmittedEndUserName,
COALESCE(distributorSubmittedEndUserState, 'unknown') AS distributorSubmittedEndUserState,
COALESCE(distributorSubmittedEndUserZipCodeUpdated,    'unknown'  ) AS distributorSubmittedEndUserZipCode,
COALESCE(distributorSubmittedItemNumber, 'unknown') AS distributorSubmittedItemNumber,
COALESCE(itemId, 'unknown') AS itemId,
'unknown' marketLevel1,
'unknown' marketLevel2,
'unknown' marketLevel3,
processingType,
territoryId,
userid,
COALESCE(VERTICAL, 'unknown') AS vertical,
posSource
from handle_delete2
union
select 
ansellContractPrice AS ansellContractPrice,
COALESCE(batchType, 'unknown') AS batchType,
COALESCE(claimedUom, 'unknown') AS claimedUom,
COALESCE(claimId, 'unknown') AS claimId,
COALESCE(claimNumber, 'unknown') AS claimNumber,
COALESCE(dateInvoiced,to_date('1900-01-01','yyyy-MM-dd')) dateInvoiced,
COALESCE(distributorSubmittedEnduserCity, 'unknown') AS distributorSubmittedEnduserCity,
COALESCE(distributorSubmittedEndUserName, 'unknown') AS distributorSubmittedEndUserName,
COALESCE(distributorSubmittedEndUserState, 'unknown') AS distributorSubmittedEndUserState,
COALESCE(distributorSubmittedEndUserZipCodeUpdated,    'unknown'  ) AS distributorSubmittedEndUserZipCode,
COALESCE(distributorSubmittedItemNumber, 'unknown') AS distributorSubmittedItemNumber,
COALESCE(itemId, 'unknown') AS itemId,
'unknown' marketLevel1,
'unknown' marketLevel2,
'unknown' marketLevel3,
processingType,
territoryId,
userid,
COALESCE(VERTICAL, 'unknown') AS vertical,
posSource
from handle_delete3
""")

handle_delete.createOrReplaceTempView('handle_delete')

# COMMAND ----------

# HANDLE DELETE
full_keys = spark.sql("""
 SELECT 
 CAST(COALESCE(ansellContractPrice,0) AS decimal(22,7))  AS ansellContractPrice,
 COALESCE(batchType, 'unknown') AS batchType,
COALESCE(claimedUom, 'unknown') AS claimedUom,
COALESCE(claimId, 'unknown') AS claimId,
COALESCE(claimNumber, 'unknown') AS claimNumber,
Date_format(CASE 
		WHEN YEAR(dateInvoiced) < 2000
			THEN CAST(CAST(YEAR(dateInvoiced) + 2000 AS STRING) || '-' || DATE_FORMAT(dateInvoiced, 'MM-dd') AS DATE)
		ELSE dateInvoiced
	END,'yyyy-MM-dd') AS dateInvoiced,
COALESCE(distributorSubmittedEnduserCity, 'unknown') AS distributorSubmittedEnduserCity,
COALESCE(distributorSubmittedEndUserName, 'unknown') AS distributorSubmittedEndUserName,
COALESCE(distributorSubmittedEndUserState, 'unknown') AS distributorSubmittedEndUserState,
COALESCE(distributorSubmittedEndUserZipCode,    'unknown'  ) AS distributorSubmittedEndUserZipCode,
COALESCE(distributorSubmittedItemNumber, 'unknown') AS distributorSubmittedItemNumber,
COALESCE(itemId, 'unknown') AS itemId,
'unknown' marketLevel1,
'unknown' marketLevel2,
'unknown' marketLevel3,
processingType,
territoryId,
userid,
COALESCE(VERTICAL, 'unknown') AS vertical,
posSource
 from handle_delete 
""")

full_keys_f = (
   full_keys
  .filter('dateInvoiced IS NOT NULL')
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns =  [*key_columns, '_SOURCE']))  
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  
  cutoff_value = get_incr_col_max_value(main_inc, 'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'ebs.ozf_resale_lines_all')
  update_run_datetime(run_datetime, table_name, 'ebs.ozf_resale_lines_all')

 # cutoff_value = get_incr_col_max_value(main_int_inc,  'LAST_UPDATE_DATE')
 # update_cutoff_value(cutoff_value, table_name, 'ebs.ozf_resale_lines_int_all')
 # update_run_datetime(run_datetime, table_name, 'ebs.ozf_resale_lines_int_all')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_trademanagement
