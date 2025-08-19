# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.wc_qv_pos_dis_a

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = False
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_pos_dis_a')
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

# EXTRACT
source_table = 's_trademanagement.point_of_sales_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  point_of_sales_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  point_of_sales_agg = load_full_dataset(source_table)
  
point_of_sales_agg.createOrReplaceTempView('point_of_sales_agg')
point_of_sales_agg.display()

# COMMAND ----------

# SAMPLING
if sampling:
  point_of_sales_agg = point_of_sales_agg.limit(10)
  point_of_sales_agg.createOrReplaceTempView('point_of_sales_agg')

# COMMAND ----------

main_df = spark.sql("""
select
  point_of_sales_agg._ID as _ID,
  org.name COMPANY,
  account.customerDivision DIVISION,
  point_of_sales_agg.salesRegion SALESREGION,
  party_agg.partyCountry SUBREGION,
  point_of_sales_agg.territoryId TERRITORY,
  point_of_sales_agg.userId USERID,
  point_of_sales_agg.vertical VERTICAL,
  account.accountType CUSTOMER_TYPE,
  party_agg.partyNumber || ' - OTD' DISTRIBUTORID,
  party_agg.partyName DISTRIBUTORNAME,
  point_of_sales_agg.distributorSubmittedEnduserCity END_USER_CITY,
  point_of_sales_agg.distributorSubmittedEndUserState STATE,
  point_of_sales_agg.distributorSubmittedEndUserZipCode POSTALCODE,
  point_of_sales_agg.distributorSubmittedEndUserName ENDUSER,
  product_agg.GBU GBU,
  product_agg.productCategory PRODCATEGORY,
  product_agg.productSubCategory PRODSUBCATEGORY,
  COALESCE(product_agg.productMaterialType, 'Unspecified') MATERIALTYPE,
  COALESCE(product_agg.productMaterialSubType, 'Unspecified') MATERIALSUBTYPE,
  product_agg.productBrandType BRANDTYPE,
  'Ansell' BRANDOWNER,
  product_agg.productBrand BRAND,
  product_agg.productDivision PRODUCT_DIVISION,
  product_agg.productM4Category M4CATEGORY,
  product_agg.productStyle STYLE,
  product_agg.productSubBrand SUBBRAND,
  nvl(product_agg.productCode, point_of_sales_agg.distributorSubmittedItemNumber) SKU,
  product_agg.name PRODUCT_NAME,
  point_of_sales_agg.dateInvoiced TRANDATE,
  point_of_sales_agg.batchNumber BATCH_NUM,
  SUM(point_of_sales_agg.claimedQuantityCase) QTY,
  SUM(point_of_sales_agg.transactionAmount) as TRANAMT,
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
  point_of_sales_agg.posSource POSSOURCE,
  point_of_sales_agg.marketLevel1 MARKET_LEVEL1,
  point_of_sales_agg.marketLevel2 MARKET_LEVEL2,
  point_of_sales_agg.marketLevel3 MARKET_LEVEL3,
  point_of_sales_agg.account_ID ACCOUNT_ID
from
  s_trademanagement.point_of_sales_agg
  join s_core.organization_agg org on point_of_sales_agg.owningbusinessunit_id = org._ID
  join s_core.account_agg account on point_of_sales_agg.account_ID = account._ID
  join s_core.party_agg on point_of_sales_agg.distributorParty_ID = party_agg._ID
  left join s_core.product_ebs product_agg on point_of_sales_agg.item_ID = product_agg._ID
  --join exclusion_list_otd on party_agg.partyNumber = exclusion_list_otd.RegistrationID
where
  point_of_sales_agg._source = 'EBS'
  and point_of_sales_agg._deleted = 'false'
  and posSource <> 'TM'
  and posSource <> 'MANADJ'
  and date_format(point_of_sales_agg.dateInvoiced, 'yyyyMM') >= '201801' --(select date_format(TO_DATE(CVALUE,'yyyy/MM/dd'), 'yyyyMM') from smartsheets.edm_control_table where table_id = 'TM_START_DATE')
  and point_of_sales_agg.batchtype || ' ' || party_agg.partyNumber not in (
    select
      distinct batchtype_regid
    from
      amazusftp1.wc_qv_pos_excluded_customer_h
  )
GROUP BY
  point_of_sales_agg._ID,
  org.name,
  account.customerDivision,
  point_of_sales_agg.salesRegion,
  party_agg.partyCountry,
  point_of_sales_agg.territoryId,
  point_of_sales_agg.userId,
  point_of_sales_agg.vertical,
  account.accountType,
  party_agg.partyNumber || ' - OTD',
  party_agg.partyName,
  point_of_sales_agg.distributorSubmittedEnduserCity,
  point_of_sales_agg.distributorSubmittedEndUserState,
  point_of_sales_agg.distributorSubmittedEndUserZipCode,
  point_of_sales_agg.distributorSubmittedEndUserName,
  product_agg.GBU,
  product_agg.productCategory,
  product_agg.productSubCategory,
  COALESCE(product_agg.productMaterialType, 'Unspecified'),
  COALESCE(product_agg.productMaterialSubType, 'Unspecified'),
  product_agg.productBrandType,
  product_agg.productBrand,
  product_agg.productDivision,
  product_agg.productM4Category,
  product_agg.productStyle,
  product_agg.productSubBrand,
  nvl(product_agg.productCode, point_of_sales_agg.distributorSubmittedItemNumber),
  product_agg.name,
  point_of_sales_agg.dateInvoiced,
  point_of_sales_agg.batchNumber,
  point_of_sales_agg.batchStatusCode,
  point_of_sales_agg.modifiedOn,
  point_of_sales_agg.ansellAgreementName,
  point_of_sales_agg.ansellCorrectedAgreementName,
  point_of_sales_agg.gpoId,
  point_of_sales_agg.gpoContractNumber,
  point_of_sales_agg.gpoContractDescription,
  point_of_sales_agg.gpoContractType,
  point_of_sales_agg.gpoStartDate,
  point_of_sales_agg.gpoExporationDate,
  point_of_sales_agg.gpoStatus,
  point_of_sales_agg.gpoInitiator,
  point_of_sales_agg.gpoContractApprover,
  point_of_sales_agg.territoryIdOrg2,
  point_of_sales_agg.salesRegionOrg2,
  point_of_sales_agg.userIdOrg2,
  account.customerTier,
  account.customerSegmentation,
  point_of_sales_agg.posSource ,
  point_of_sales_agg.marketLevel1 ,
  point_of_sales_agg.marketLevel2 ,
  point_of_sales_agg.marketLevel3 ,
  point_of_sales_agg.account_ID
""")

main_df.cache()
main_df.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("TRANDATE"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)
main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
