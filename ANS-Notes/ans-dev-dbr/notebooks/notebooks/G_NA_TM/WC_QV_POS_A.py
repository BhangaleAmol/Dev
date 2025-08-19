# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.wc_qv_pos_a

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_pos_a')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/na_tm/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT POINT_OF_SALES
source_table = 's_trademanagement.point_of_sales_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  point_of_sales = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  point_of_sales = load_full_dataset(source_table)
  
point_of_sales.createOrReplaceTempView('point_of_sales_agg')
point_of_sales.display()

# COMMAND ----------

# EXTRACT ACCRUALS
source_table = 's_trademanagement.trade_promotion_accruals_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  trade_promotion_accruals = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  trade_promotion_accruals = load_full_dataset(source_table)
  
trade_promotion_accruals.createOrReplaceTempView('trade_promotion_accruals_agg')
trade_promotion_accruals.display()

# COMMAND ----------

# EXTRACT INVOICES
source_table = 's_supplychain.sales_invoice_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_invoice_lines = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_invoice_lines = load_full_dataset(source_table)
  
sales_invoice_lines.createOrReplaceTempView('sales_invoice_lines_agg')
sales_invoice_lines.display()

# COMMAND ----------

# SAMPLING
if sampling:
  point_of_sales = point_of_sales.limit(10)
  point_of_sales.createOrReplaceTempView('point_of_sales_agg')
  
  trade_promotion_accruals = trade_promotion_accruals.limit(10)
  trade_promotion_accruals.createOrReplaceTempView('trade_promotion_accruals_agg')
  
  sales_invoice_lines = sales_invoice_lines.limit(10)
  sales_invoice_lines.createOrReplaceTempView('sales_invoice_lines_agg')

# COMMAND ----------

ZIP3=spark.sql("""
select
  min(territoryid) territoryid,
  trim(zip3) zip3,
  vertical,
  min(salesregion) sales_region,
  min(userid) user_id
from
  s_core.territory_assignments_agg
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
  s_core.territory_assignments_agg
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
  vertical,
  zip3,
  min(userid) userid
from
  s_core.territory_assignments_agg
where
  scenario = 'DEFAULT'
  group by zip3,vertical
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
  s_core.territory_assignments_agg
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
  s_core.territory_assignments_agg
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

FUND_CATEGORY=spark.sql("""
SELECT c.CATEGORY_ID, 
d.CATEGORY_NAME
FROM
ebs.AMS_CATEGORIES_B c, 
ebs.AMS_CATEGORIES_TL d
WHERE 
 c.category_id = d.category_id AND
c.enabled_flag = 'Y' AND
d.language = 'US'
""")
FUND_CATEGORY.createOrReplaceTempView('FUND_CATEGORY')

# COMMAND ----------

EXCLUSION_LIST_ITD=spark.sql("""
select distinct AccountNumber from amazusftp1.exclusion_list WHERE ITDOTD LIKE 'ITD%'
""")
EXCLUSION_LIST_ITD.createOrReplaceTempView('EXCLUSION_LIST_ITD')

# COMMAND ----------

EXCLUSION_LIST_OTD=spark.sql("""
select distinct RegistrationID  from amazusftp1.exclusion_list WHERE ITDOTD LIKE 'OTD%'
""")
EXCLUSION_LIST_OTD.createOrReplaceTempView('EXCLUSION_LIST_OTD')

# COMMAND ----------

main_point_of_sales = spark.sql("""

select 
  point_of_sales_agg._ID as _ID,
  org.name COMPANY,
  account.customerDivision DIVISION,
  point_of_sales_agg.salesRegion SALESREGION,
  party_agg.partyCountry SUBREGION,
  point_of_sales_agg.territoryId TERRITORY,
  point_of_sales_agg.userId USERID,
  account.vertical VERTICAL,
  account.accountType CUSTOMER_TYPE,
  party_agg.partyNumber || ' - OTD' DISTRIBUTORID ,
  party_agg.partyName DISTRIBUTORNAME,
  point_of_sales_agg.distributorSubmittedEnduserCity END_USER_CITY,
  point_of_sales_agg.distributorSubmittedEndUserState STATE,
  point_of_sales_agg.distributorSubmittedEndUserZipCode POSTALCODE , 
  point_of_sales_agg.distributorSubmittedEndUserName ENDUSER,
  product_agg.GBU GBU,
  product_agg.productCategory PRODCATEGORY,  
  product_agg.productSubCategory PRODSUBCATEGORY, 
  COALESCE(product_agg.productMaterialType,'Unspecified')  MATERIALTYPE,
  COALESCE(product_agg.productMaterialSubType,'Unspecified')  MATERIALSUBTYPE,
  product_agg.productBrandType BRANDTYPE, 
  'Ansell' BRANDOWNER,
  product_agg.productBrand BRAND, 
  product_agg.productDivision PRODUCT_DIVISION,
  product_agg.productM4Category M4CATEGORY,
  product_agg.productStyle STYLE,
  product_agg.productSubBrand SUBBRAND, 
  product_agg.productCode SKU,
  product_agg.name PRODUCT_NAME, 
  point_of_sales_agg.dateInvoiced TRANDATE,  
  point_of_sales_agg.batchNumber BATCH_NUM,
 SUM( point_of_sales_agg.claimedQuantitycase)  QTY,
  SUM(point_of_sales_agg.transactionAmount) as  TRANAMT,
  point_of_sales_agg.batchStatusCode STATUS_CODE,
  point_of_sales_agg.modifiedOn W_UPDATE_DT,
  point_of_sales_agg.ansellAgreementName DIST_END_USER_CON,
  point_of_sales_agg.ansellCorrectedAgreementName ANS_COR_END_USER_CON,
  point_of_sales_agg.gpoId GPO_ID,
  point_of_sales_agg.gpoContractNumber CONTRACT_NUMBER,
  point_of_sales_agg.gpoContractDescription DESCRIPTION,
  point_of_sales_agg.gpoContractType CONTRACT_TYPE,
  point_of_sales_agg.gpoStartDate START_DATE, 
  point_of_sales_agg.gpoExporationDate EXPIRATION_DATE, 
  point_of_sales_agg.gpoStatus STATUS,  
  point_of_sales_agg.gpoInitiator INITIATOR,
  point_of_sales_agg.gpoContractApprover CONTRACT_APPROVER,
  '' OS_SALESREGION,
  '' OS_TERRITORY, 
  '' OS_USERID,
  point_of_sales_agg.territoryIdOrg2 SECONDORG_TERRITORY,
  point_of_sales_agg.salesRegionOrg2 SECONDORG_SALESREGION,
  point_of_sales_agg.userIdOrg2 SECONDORG_USERID,
  '' ORD_PRICE_LIST_NAME,
  '' ORD_PRICE_LIST_ID,
  account.customerTier CUSTOMER_TIER,
  account.customerSegmentation CUSTOMER_SEGMENTATION,
  '' PARTY_SITE_NUMBER,
  '' MDM_ID,
  point_of_sales_agg.distributorClaimNumber DISTRIBUTORCLAIMNUMBER,
  point_of_sales_agg.resaleLineStatus RESALELINESTATUS,
  point_of_sales_agg.account_id ACCOUNT_ID
from 
  s_trademanagement.point_of_sales_agg
  join s_core.organization_agg  org on point_of_sales_agg.owningbusinessunit_id = org._ID 
  join s_core.account_agg account on point_of_sales_agg.account_ID = account._ID
  join s_core.party_agg on point_of_sales_agg.distributorParty_ID = party_agg._ID
  left join s_core.product_ebs product_agg on point_of_sales_agg.item_ID = product_agg._ID
  join exclusion_list_otd on party_agg.partyNumber = exclusion_list_otd.RegistrationID
  
  where point_of_sales_agg._source = 'EBS'
  and point_of_sales_agg._deleted = 'false'
  --and party_agg.partyNumber = '6740'
  --AND batchNumber = '69877'
  --and point_of_sales_agg.salesRegion is not null--= 'INDNC'
  and posSource =  'TM' 
  and date_format(point_of_sales_agg.dateInvoiced,'yyyyMM') >= 
  '201801'
  --(select date_format(TO_DATE(CVALUE,'yyyy/MM/dd'), 'yyyyMM') from smartsheets.edm_control_table where table_id = 'TM_START_DATE') 
  and  point_of_sales_agg.batchtype  || ' ' || party_agg.partyNumber not in (select distinct batchtype_regid from amazusftp1.wc_qv_pos_excluded_customer_h)
  GROUP BY 
  point_of_sales_agg._ID,
  org.name ,
  account.customerDivision ,
  point_of_sales_agg.salesRegion ,
  party_agg.partyCountry ,
  point_of_sales_agg.territoryId ,
  point_of_sales_agg.userId ,
  account.vertical ,
  account.accountType ,
  party_agg.partyNumber || ' - OTD'  ,
  party_agg.partyName ,
  point_of_sales_agg.distributorSubmittedEnduserCity,
  point_of_sales_agg.distributorSubmittedEndUserState ,
  point_of_sales_agg.distributorSubmittedEndUserZipCode , 
  point_of_sales_agg.distributorSubmittedEndUserName,
  product_agg.GBU ,
  product_agg.productCategory ,  
  product_agg.productSubCategory , 
  COALESCE(product_agg.productMaterialType,'Unspecified')  ,
  COALESCE(product_agg.productMaterialSubType,'Unspecified')  ,
  product_agg.productBrandType, 
  product_agg.productBrand , 
  product_agg.productDivision ,
  product_agg.productM4Category ,
  product_agg.productStyle ,
  product_agg.productSubBrand , 
  product_agg.productCode ,
  product_agg.name , 
  point_of_sales_agg.dateInvoiced ,  
  point_of_sales_agg.batchNumber ,
  point_of_sales_agg.batchStatusCode ,
  point_of_sales_agg.modifiedOn,
  point_of_sales_agg.ansellAgreementName,
  point_of_sales_agg.ansellCorrectedAgreementName ,
  point_of_sales_agg.gpoId ,
  point_of_sales_agg.gpoContractNumber,
  point_of_sales_agg.gpoContractDescription ,
  point_of_sales_agg.gpoContractType ,
  point_of_sales_agg.gpoStartDate , 
  point_of_sales_agg.gpoExporationDate , 
  point_of_sales_agg.gpoStatus ,  
  point_of_sales_agg.gpoInitiator ,
  point_of_sales_agg.gpoContractApprover ,
  point_of_sales_agg.territoryIdOrg2 ,
  point_of_sales_agg.salesRegionOrg2 ,
  point_of_sales_agg.userIdOrg2 ,
  account.customerTier ,
  account.customerSegmentation,
  point_of_sales_agg.resaleLineStatus,
  point_of_sales_agg.distributorClaimNumber,
  point_of_sales_agg.account_id

""")
main_point_of_sales.createOrReplaceTempView('main_point_of_sales')

# COMMAND ----------

main_invoices = spark.sql("""
select 
  sales_invoice_lines._ID as _ID,
  org.name COMPANY,
  account.customerDivision DIVISION,
   ZIP3.sales_region SALESREGION,
  shipTo.country SUBREGION,
    COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID,
    'unknown'
  ) AS  TERRITORY,
  TERR_DEFAULT.userId USERID,
  account.vertical VERTICAL,
  account.accountType CUSTOMER_TYPE,
  account.accountNumber || ' - ITD' DISTRIBUTORID ,
  account.name DISTRIBUTORNAME,
  nvl(shipTo.city, billTo.city) END_USER_CITY,
  nvl(shipTo.state, billTo.state) STATE,
  nvl(shipTo.postalCode, billTo.postalCode) POSTALCODE , 
  account.name ENDUSER,
  product.GBU GBU,
  product.productCategory PRODCATEGORY,  
  product.productSubCategory PRODSUBCATEGORY, 
  COALESCE(product.productMaterialType,'Unspecified') MATERIALTYPE,
  COALESCE(product.productMaterialSubType,'Unspecified') MATERIALSUBTYPE,
  product.productBrandType BRANDTYPE, 
  'Ansell' BRANDOWNER,
  product.productBrand BRAND, 
  product.productDivision PRODUCT_DIVISION,
  product.productM4Category M4CATEGORY,
  product.productStyle STYLE,
  product.productSubBrand SUBBRAND, 
  product.productCode SKU,
  product.name PRODUCT_NAME, 
  sales_invoice_headers.dateInvoiced TRANDATE,     
  0 BATCH_NUM,
  sum(sales_invoice_lines.quantityInvoiced * sales_invoice_lines.caseUomConv) QTY,
  sum(sales_invoice_lines.baseAmount * sales_invoice_headers.exchangeRate) - sum(nvl(settlementDiscountsCalculated,0)) TRANAMT,
  '' STATUS_CODE,
  sales_invoice_headers.dateInvoiced W_UPDATE_DT,
  '' DIST_END_USER_CON,
  '' ANS_COR_END_USER_CON,
  '' GPO_ID,
  '' CONTRACT_NUMBER,
  '' DESCRIPTION,
  '' CONTRACT_TYPE,
  '' START_DATE, 
  '' EXPIRATION_DATE, 
  '' STATUS,  
  '' INITIATOR,
  '' CONTRACT_APPROVER,
  '' OS_SALESREGION,
  '' OS_TERRITORY, 
  '' OS_USERID,
  COALESCE(
    SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_ZIP3.TERRITORYID
  ) SECONDORG_TERRITORY,
 SECONDORG_ZIP3.sales_region SECONDORG_SALESREGION,
   CASE
      WHEN (
        SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE SECONDORG_ZIP3.USER_ID
    END SECONDORG_USERID,
  '' ORD_PRICE_LIST_NAME,
  '' ORD_PRICE_LIST_ID,
  account.customerTier CUSTOMER_TIER,
  account.customerSegmentation CUSTOMER_SEGMENTATION,
  shipTo.partySiteNumber PARTY_SITE_NUMBER,
  shipTo.mdmId MDM_ID,
  NULL AS DISTRIBUTORCLAIMNUMBER,
  NULL AS RESALELINESTATUS,
  sales_invoice_headers.customer_ID ACCOUNT_ID
from 
  s_supplychain.sales_invoice_lines_agg sales_invoice_lines
  join s_supplychain.sales_invoice_headers_agg  sales_invoice_headers on sales_invoice_lines.invoice_ID = sales_invoice_headers._ID
  join s_core.organization_agg org on sales_invoice_headers.owningbusinessunit_id = org._ID 
  join s_core.account_agg account on sales_invoice_headers.customer_ID = account._ID
 -- join s_core.party_agg on point_of_sales_agg.distributorParty_ID = party_agg._ID
  join s_core.product_agg product on sales_invoice_lines.item_ID = product._ID
  join s_core.customer_location_agg shipTo on sales_invoice_headers.shipToAddress_ID = shipto._ID
  join s_core.customer_location_agg billTo on sales_invoice_headers.billtoAddress_ID = billTo._ID
    JOIN EXCLUSION_LIST_ITD ON account.accountNumber = EXCLUSION_LIST_ITD.AccountNumber 
   LEFT JOIN ZIP3 ON substr(nvl(shipTo.postalCode, billTo.postalCode), 1, 3) = ZIP3.zip3
  AND account.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON account.vertical = ZIP0.vertical
  LEFT JOIN SECONDORG_ZIP3 ON substr(nvl(shipTo.postalCode, billTo.postalCode), 1, 3) = SECONDORG_ZIP3.zip3
  AND account.vertical = SECONDORG_ZIP3.vertical 
  LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON product.productCode = PROD_OVERRIDE.PRODUCTID
  LEFT JOIN CUST_OVERRIDE ON account.accountNumber = CUST_OVERRIDE.custid
  LEFT JOIN SECONDORG_PROD_OVERRIDE ON product.productCode = SECONDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN SECONDORG_CUST_OVERRIDE ON account.accountNumber = SECONDORG_CUST_OVERRIDE.custid
  LEFT JOIN TERR_DEFAULT ON account.vertical = TERR_DEFAULT.vertical
  AND substr(nvl(shipTo.postalCode, billTo.postalCode), 1, 3) = TERR_DEFAULT.zip3
  LEFT JOIN THIRDORG_PROD_OVERRIDE ON product.productCode = THIRDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN THIRDORG_CUST_OVERRIDE ON account.accountNumber = THIRDORG_CUST_OVERRIDE.custid
  LEFT JOIN THIRDORG_ZIP3 ON substr(nvl(shipTo.postalCode, billTo.postalCode), 1, 3) = THIRDORG_ZIP3.zip3
  
    
where
  sales_invoice_headers._SOURCE = 'EBS'
  and sales_invoice_lines._SOURCE = 'EBS'
  and account.customerType = 'External'
   and org.name  <> 'Ansell  Mexico'
  --and account.accountNumber = 10916
  and date_format(sales_invoice_headers.dateInvoiced,'yyyyMM') >= 
  '201801'
  --(select date_format(TO_DATE(CVALUE,'yyyy/MM/dd'), 'yyyyMM') from smartsheets.edm_control_table where table_id = 'TM_START_DATE') 
and nvl(product.productDivision,'A') not like 'SH%' -- exclude sw
and (nvl(product.productDivision,'A') not like 'SH%' or account.accountNumber in  ('11500', '11077'))-- exclude sw, but include Wallmart 11500
and (nvl(account.customerDivision,'A') not like  'SH%' or account.accountNumber in ('11500', '11077'))
--and sales_invoice_lines.itemId is not null
and CASE WHEN shipTo.siteCategory = 'LA' THEN 'LA' ELSE 'NA' END ='NA'
and shipTo._deleted = 'false'
and billto._deleted = 'false'
GROUP BY
 sales_invoice_lines._ID ,
  org.name ,
  account.customerDivision ,
   ZIP3.sales_region ,
  shipTo.country ,
    COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID,
    'unknown'
  ) ,
  TERR_DEFAULT.userId ,
  account.vertical ,
  account.accountType ,
  account.accountNumber || ' - ITD' ,
  account.name ,
  nvl(shipTo.city, billTo.city) ,
  nvl(shipTo.state, billTo.state) ,
  nvl(shipTo.postalCode, billTo.postalCode) , 
  account.name ,
  product.GBU ,
  product.productCategory ,
  product.productSubCategory ,
  COALESCE(product.productMaterialType,'Unspecified') ,
  COALESCE(product.productMaterialSubType,'Unspecified') ,
  product.productBrandType ,
  product.productBrand ,
  product.productDivision ,
  product.productM4Category ,
  product.productStyle ,
  product.productSubBrand ,
  product.productCode ,
  product.name ,
  sales_invoice_headers.dateInvoiced ,
  sales_invoice_headers.dateInvoiced ,
  COALESCE(
    SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_ZIP3.TERRITORYID
  ) ,
 SECONDORG_ZIP3.sales_region ,
   CASE
      WHEN (
        SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE SECONDORG_ZIP3.USER_ID
    END ,
  account.customerTier ,
  account.customerSegmentation, 
  shipTo.partySiteNumber ,
  shipTo.mdmId ,
  sales_invoice_headers.customer_ID
 
""")
main_invoices.createOrReplaceTempView('main_invoices')

# COMMAND ----------

main_accruals = spark.sql("""
select 
  accruals._ID as _ID,
  org.name COMPANY,
  account.customerDivision DIVISION,
   ZIP3.sales_region SALESREGION,
  shipTo.country SUBREGION,
    COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID,
    'unknown'
  ) AS  TERRITORY,
  TERR_DEFAULT.userId USERID,
  account.vertical VERTICAL,
  account.accountType CUSTOMER_TYPE,
  account.accountNumber || ' - ITD' DISTRIBUTORID ,
  account.name DISTRIBUTORNAME,
  nvl(shipTo.city, billTo.city) END_USER_CITY,
  nvl(shipTo.state, billTo.state) STATE,
  nvl(shipTo.postalCode, billTo.postalCode) POSTALCODE , 
  account.name ENDUSER,
  product.GBU GBU,
  product.productCategory PRODCATEGORY,  
  product.productSubCategory PRODSUBCATEGORY, 
  COALESCE(product.productMaterialType,'Unspecified') MATERIALTYPE,
  COALESCE(product.productMaterialSubType,'Unspecified') MATERIALSUBTYPE,
  product.productBrandType BRANDTYPE, 
  'Ansell' BRANDOWNER,
  product.productBrand BRAND, 
  product.productDivision PRODUCT_DIVISION,
  product.productM4Category M4CATEGORY,
  product.productStyle STYLE,
  product.productSubBrand SUBBRAND, 
  product.productCode SKU,
  product.name PRODUCT_NAME, 
  accruals.glDate TRANDATE,     
  0 BATCH_NUM,
  0 QTY,
   SUM(CASE
    WHEN category.CATEGORY_NAME in  ('Volume Discount', 'Contract Rebates', 'BOGO', 'TPR', 'Slotting Fee', 'Coop Adv Fee')
    THEN case when org.name = 'Ansell Canada Inc' then -(accruals.amountActual+accruals.amountRemaining)  else -((accruals.amountActual+accruals.amountRemaining)) end
    ELSE 0
  END) AS  TRANAMT,
  '' STATUS_CODE,
  accruals.glDate W_UPDATE_DT,
  '' DIST_END_USER_CON,
  '' ANS_COR_END_USER_CON,
  '' GPO_ID,
  '' CONTRACT_NUMBER,
  '' DESCRIPTION,
  '' CONTRACT_TYPE,
  '' START_DATE, 
  '' EXPIRATION_DATE, 
  '' STATUS,  
  '' INITIATOR,
  '' CONTRACT_APPROVER,
  '' OS_SALESREGION,
  '' OS_TERRITORY, 
  '' OS_USERID,
  COALESCE(
    SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_ZIP3.TERRITORYID
  ) SECONDORG_TERRITORY,
 SECONDORG_ZIP3.sales_region SECONDORG_SALESREGION,
   CASE
      WHEN (
        SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE SECONDORG_ZIP3.USER_ID
    END SECONDORG_USERID,
  '' ORD_PRICE_LIST_NAME,
  '' ORD_PRICE_LIST_ID,
  account.customerTier CUSTOMER_TIER,
  account.customerSegmentation CUSTOMER_SEGMENTATION,
  '' PARTY_SITE_NUMBER,
  '' MDM_ID,
  NULL AS DISTRIBUTORCLAIMNUMBER,
  NULL AS RESALELINESTATUS,
  accruals.account_ID ACCOUNTID
from 
  s_trademanagement.trade_promotion_accruals_agg accruals
  join s_core.organization_agg org on accruals.owningbusinessunit_id = org._ID 
  join s_core.account_agg account on accruals.account_ID = account._ID
  left join s_core.product_agg product on accruals.item_ID = product._ID
  left join s_core.customer_location_agg shipTo on accruals.shipToAddress_ID = shipto._ID
  left join s_core.customer_location_agg billTo on accruals.billtoAddress_ID = billTo._ID
  JOIN EXCLUSION_LIST_ITD ON account.accountNumber = EXCLUSION_LIST_ITD.AccountNumber
    
   LEFT JOIN ZIP3 ON substr(nvl(shipTo.postalCode, billTo.postalCode), 1, 3) = ZIP3.zip3
  AND account.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON account.vertical = ZIP0.vertical
  LEFT JOIN SECONDORG_ZIP3 ON substr(nvl(shipTo.postalCode, billTo.postalCode), 1, 3) = SECONDORG_ZIP3.zip3
  AND account.vertical = SECONDORG_ZIP3.vertical 
  LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON product.productCode = PROD_OVERRIDE.PRODUCTID
  LEFT JOIN CUST_OVERRIDE ON account.accountNumber = CUST_OVERRIDE.custid
  LEFT JOIN SECONDORG_PROD_OVERRIDE ON product.productCode = SECONDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN SECONDORG_CUST_OVERRIDE ON account.accountNumber = SECONDORG_CUST_OVERRIDE.custid
  LEFT JOIN TERR_DEFAULT ON account.vertical = TERR_DEFAULT.vertical
  AND substr(nvl(shipTo.postalCode, billTo.postalCode), 1, 3) = TERR_DEFAULT.zip3
  LEFT JOIN THIRDORG_PROD_OVERRIDE ON product.productCode = THIRDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN THIRDORG_CUST_OVERRIDE ON account.accountNumber = THIRDORG_CUST_OVERRIDE.custid
  LEFT JOIN THIRDORG_ZIP3 ON substr(nvl(shipTo.postalCode, billTo.postalCode), 1, 3) = THIRDORG_ZIP3.zip3
  LEFT JOIN FUND_CATEGORY category ON accruals.categoryid = category.category_id
  where  
accruals._SOURCE = 'EBS'
  and accruals._deleted = 'false'
  and account.customerType = 'External'
 -- and account.accountnumber = '10005'
  and account.accountNumber not in ('11500', '11077')
  and org.name  <> 'Ansell  Mexico'
 -- and  date_format(gldate, 'yyyyMM') > '201501'
   and date_format(accruals.gldate,'yyyyMM') >= 
   '201801'
   --(select date_format(TO_DATE(CVALUE,'yyyy/MM/dd'), 'yyyyMM')  from smartsheets.edm_control_table where table_id = 'TM_START_DATE')
     and product.productDivision not like 'SH%' -- exclude sw
and (product.productDivision not like 'SH%' or account.accountNumber in  ('11500', '11077'))-- exclude sw, but include Wallmart 11500
and (account.customerDivision not like  'SH%' or account.accountNumber in ('11500', '11077'))
and CASE WHEN shipTo.siteCategory = 'LA' THEN 'LA' ELSE 'NA' END  ='NA'
and shipTo._deleted = 'false'
and billto._deleted = 'false'
GROUP BY
  accruals._ID ,
  org.name ,
  account.customerDivision ,
   ZIP3.sales_region ,
  shipTo.country ,
    COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID,
    'unknown'
  ) ,
  TERR_DEFAULT.userId ,
  account.vertical ,
  account.accountType ,
  account.accountNumber || ' - ITD' ,
  account.name ,
  nvl(shipTo.city, billTo.city) ,
  nvl(shipTo.state, billTo.state) ,
  nvl(shipTo.postalCode, billTo.postalCode) , 
  account.name ,
  product.GBU ,
  product.productCategory ,
  product.productSubCategory ,
  COALESCE(product.productMaterialType,'Unspecified') ,
  COALESCE(product.productMaterialSubType,'Unspecified') ,
  product.productBrandType ,
  product.productBrand ,
  product.productDivision ,
  product.productM4Category ,
  product.productStyle ,
  product.productSubBrand ,
  product.productCode ,
  product.name ,
  accruals.glDate ,
  accruals.glDate ,
  COALESCE(
    SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_ZIP3.TERRITORYID
  ) ,
 SECONDORG_ZIP3.sales_region ,
   CASE
      WHEN (
        SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE SECONDORG_ZIP3.USER_ID
    END ,
  account.customerTier ,
  account.customerSegmentation,
  accruals.account_ID

""")


main_accruals.createOrReplaceTempView('main_accruals')

# COMMAND ----------

# %sql select count(*) from main_point_of_sales

# COMMAND ----------

# %sql select count(*) from main_accruals

# COMMAND ----------

# %sql select count(*) from main_invoices 

# COMMAND ----------

main_point_of_sales_f = main_point_of_sales.transform(attach_subset_column('POS'))
main_invoices_f = main_invoices.transform(attach_subset_column('INV'))
main_accruals_f = main_accruals.transform(attach_subset_column('ACC'))

# COMMAND ----------

main_df = (
  main_point_of_sales_f
  .union(main_invoices_f)
  .union(main_accruals_f)
)
main_df.cache()

# COMMAND ----------

# main_df = spark.sql("""
#   SELECT * FROM main_point_of_sales
#   UNION
#   SELECT * FROM main_invoices
#   UNION
#   SELECT * FROM main_accruals
# """)

# main_df.cache()
# main_df.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("TRANDATE"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# VALIDATE DATA
if incremental:
  check_distinct_count(main_f, '_ID')

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
full_pos_keys = spark.sql("""
  SELECT _id from s_trademanagement.point_of_sales_agg 
  where point_of_sales_agg._source = 'EBS'
  and point_of_sales_agg._deleted = 'false'
  and posSource =  'TM' 
  and date_format(point_of_sales_agg.dateInvoiced,'yyyyMM') >= 
  '201801'
  
""")

full_inv_keys = spark.sql("""
  SELECT sales_invoice_lines._id from s_supplychain.sales_invoice_lines_agg sales_invoice_lines
  join s_supplychain.sales_invoice_headers_agg  sales_invoice_headers on sales_invoice_lines.invoice_ID = sales_invoice_headers._ID
  where sales_invoice_headers._SOURCE = 'EBS'
  and sales_invoice_lines._SOURCE = 'EBS'
  and date_format(sales_invoice_headers.dateInvoiced,'yyyyMM') >= 
  '201801'

""")

full_acc_keys = spark.sql("""
  SELECT _id from s_trademanagement.trade_promotion_accruals_agg accruals
  where accruals._SOURCE = 'EBS'
  and accruals._deleted = 'false'
  and date_format(accruals.gldate,'yyyyMM') >= 
   '201801'

""")

full_keys_f = (
  full_pos_keys
  .union(full_inv_keys)
  .union(full_acc_keys)
  .select('_ID')
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(point_of_sales, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_trademanagement.point_of_sales_agg')
  update_run_datetime(run_datetime, target_table, 's_trademanagement.point_of_sales_agg')

  cutoff_value = get_max_value(trade_promotion_accruals, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_trademanagement.trade_promotion_accruals_agg')
  update_run_datetime(run_datetime, target_table, 's_trademanagement.trade_promotion_accruals_agg')

  cutoff_value = get_max_value(sales_invoice_lines, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_lines_agg')
