# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.wc_pbi_tm_a

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_pbi_tm_a')
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
  point_of_sales = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  point_of_sales = load_full_dataset(source_table)
  
point_of_sales.createOrReplaceTempView('point_of_sales_agg')
point_of_sales.display()

# COMMAND ----------

# SAMPLING
if sampling:
  point_of_sales = point_of_sales.limit(10)
  point_of_sales.createOrReplaceTempView('point_of_sales_agg')

# COMMAND ----------

main_df = spark.sql("""
  select
    point_of_sales_agg._ID _ID,
    point_of_sales_agg.claimId RESALE_LINE_NUMBER,
    point_of_sales_agg.disputeReasonCode DISPUTE_REASON_CODE,
    point_of_sales_agg.disputeReasonDescription DISPUTE_REASON_DESC,
    party_agg.partyName  DISTRIBUTOR_NAME,
    point_of_sales_agg.batchType BATCH_TYPE,
    point_of_sales_agg.batchNumber BATCH_NUMBER,
    point_of_sales_agg.batchStatusCode BATCH_STATUS,
    party_agg.partyNumber DISTRIBUTOR_PARTY_NUMBER,
    concat(date_format(point_of_sales_agg.dateInvoiced, 'yyyy'),  ' / ' , date_format(point_of_sales_agg.dateInvoiced, 'MM')) INVOICE_MONTH,
    point_of_sales_agg.distributorSubmittedEndUserName DIST_SUBMITTED_END_USER_NAME,
    point_of_sales_agg.distributorSubmittedEnduserAddress1 DIST_SUBMITTED_END_USER_ADD1,
    point_of_sales_agg.distributorSubmittedEnduserAddress2 DIST_SUBMITTED_END_USER_ADD2,
    point_of_sales_agg.distributorSubmittedEnduserAddress3 DIST_SUBMITTED_END_USER_ADD3,
    point_of_sales_agg.distributorSubmittedEnduserCity DIST_SUBMITTED_END_USER_CITY,
    point_of_sales_agg.distributorSubmittedEndUserState DIST_SUBMITTED_END_USER_STATE,
    point_of_sales_agg.distributorSubmittedEndUserZipCode DIST_SUBMITTED_END_USER_ZIP,
    endUserPartyId END_USER_PARTY_ID,
   -- To Be added END_USER_PARTY_NUMBER,
    point_of_sales_agg.endUserPrmsNumber END_USER_PRMS_NUMBER,
    point_of_sales_agg.ansellCorrectedAgreementName EBS_CORRECTED_END_USER_CONT_NR,
    point_of_sales_agg.ansellAgreementName EBS_END_USER_CONTRACT_NUMBER,
    point_of_sales_agg.distributorSubmittedEndUserContractNumber DIST_SUB_END_USER_CONT_NR,
    product_agg.productBrand PRODUCT_BRAND,
    product_agg.productSubBrand PRODUCT_SUBBRAND,
    product_agg.productCode PRODUCT_NUMBER,
    product_agg.name PRODUCT_NAME,
    point_of_sales_agg.claimedQuantity CLAIMED_QTY,
    point_of_sales_agg.claimedUom CLAIMED_UOM,
    point_of_sales_agg.distributorSubmittedAcquisitionCost DISTRIBUTOR_ACQ_COST,
    point_of_sales_agg.distributorSubmittedEndUserContractPrice DISTRIBUTOR_CONTRACT_PRICE,
    point_of_sales_agg.ansellAcquisitionCost EBS_DISTRIBUTOR_ACQ_COST,
    point_of_sales_agg.ansellContractPrice EBS_CONTRACT_PRICE,
    point_of_sales_agg.ebsEndUserSalesAmt EBS_END_USER_SALE_AMOUNT,
    org.name OPERATING_UNIT,
    point_of_sales_agg.gpoId GPO_ID,
    point_of_sales_agg.gpoContractDescription DESCRIPTION,
    point_of_sales_agg.gpoContractType CONTRACT_TYPE,
    point_of_sales_agg.gpoStartDate CONTRACT_START_DATE,
    point_of_sales_agg.gpoExporationDate CONTRACT_EXPIRATION_DATE,
    point_of_sales_agg.gpoInitiator CONTRACT_INITIATOR,
    point_of_sales_agg.transactionAmount TRANSACTION_AMOUNT,
    account_agg.customerDivision  CUSTOMER_DIVISION,
    current_date W_INSERT_DT,
    point_of_sales_agg.modifiedOn CHANGED_ON_DT,
    point_of_sales_agg.salesRegion SALESREGION,
    point_of_sales_agg.territoryId TERRITORY,
    point_of_sales_agg.userId USERID,
    account_agg.vertical  VERTICAL,
    point_of_sales_agg.distributorSubmittedEnduserId DIST_END_USER_ID,
    point_of_sales_agg.territoryIdOrg2  SECONDORG_TERRITORY,
    point_of_sales_agg.salesRegionOrg2 SECONDORG_SALESREGION,
    point_of_sales_agg.userIdOrg2 SECONDORG_USERID,
    point_of_sales_agg.approvalDate CLAIM_APPROVAL_DATE
from
  point_of_sales_agg
  join s_core.account_agg on point_of_sales_agg.account_ID = account_agg._ID
  join s_core.party_agg on point_of_sales_agg.distributorParty_ID = party_agg._ID
  join s_core.organization_agg org on point_of_sales_agg.owningBusinessUnit_ID = org._ID
  LEFT join s_core.product_agg on point_of_sales_agg.item_ID = product_agg._ID
  join (select distinct RegistrationID  from amazusftp1.exclusion_list WHERE ITDOTD LIKE 'OTD%') exclusion_list_otd on party_agg.partyNumber = exclusion_list_otd.RegistrationID
where
  date_format(point_of_sales_agg.dateInvoiced, 'yyyyMM') >= '201801'
  --and org.name  not like '%Canada%'
  --and account_agg.gbu  in ('MEDICAL')
  and possource = 'TM'
 -- and point_of_sales_agg.posFlag = 'OTD'
   and point_of_sales_agg._deleted = 'false'
""")

main_df.cache()
main_df.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("CHANGED_ON_DT"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)
main_f.display()

# COMMAND ----------

# VALIDATE DATA
check_distinct_count(main_f, '_ID')

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
   spark.table('s_trademanagement.point_of_sales_agg')
  .select('_ID')
  .filter("_DELETED IS FALSE")
  .filter("_SOURCE = 'EBS'")
  )

apply_soft_delete(full_keys_f, table_name, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_trademanagement.point_of_sales_agg')
  update_run_datetime(run_datetime, target_table, 's_trademanagement.point_of_sales_agg')
