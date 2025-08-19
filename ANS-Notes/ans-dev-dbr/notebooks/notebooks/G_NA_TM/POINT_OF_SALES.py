# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.point_of_sales

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'point_of_sales')
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
  SELECT 
    point_of_sales_agg._ID,
    acc.accountnumber,
    ansellAcquisitionCost,
    ansellAgreementName,
    ansellContractPrice,
    ansellCorrectedAgreementName,
    prod.ansStdUom,
    prod.ansStduomConv,
    party_agg.address1Line0 ansellEndUserAddress1,
    party_agg.address1Line1 ansellEndUserAddress2,
    party_agg.address1Line2 ansellEndUserAddress3,
    party_agg.partyCity ansellEndUserCity,
    party_agg.partyName ansellEndUserName,
    party_agg.partyPostalCode ansellEndUserPostalCode,
    party_agg.partyState ansellEndUserState,
    date_format(approvalDate, 'yyyy-MM') approvalMonth,
    date_format(approvalDate, 'yyyy') approvalYear,
    batchNumber,
    batchStatusCode,
    batchType,
    prod.primaryuomconv / prod.piecesincase as caseUomConv,
    claimComments,
    claimedQuantity,
    claimedQuantityCase,
    claimedQuantityPrimary,
    claimedUom,
    claimId,
    contractEligible,
    dateInvoiced,
    disputeFollowUpAction,
    disputeReasonCode,
    disputeReasonDescription,
    distributorClaimNumber,
    distributorPartyId,
    distributorSubmittedAcquisitionCost,
    distributorSubmittedEndUserActualSellingPrice,
    distributorSubmittedEnduserAddress1,
    distributorSubmittedEnduserAddress2,
    distributorSubmittedEnduserAddress3,
    distributorSubmittedEnduserCity,
    distributorSubmittedEndUserContractNumber,
    distributorSubmittedEndUserContractPrice,
    distributorSubmittedEnduserId,
    distributorSubmittedEndUserName,
    distributorSubmittedEndUserState,
    distributorSubmittedEndUserZipCode,
    distributorSubmittedItemNumber,
    distributorSubmittedTotalClaimedRebateAmount,
    distributorSubmittedUnitRebateAmount,
    ebsAcceptedAmount,
    ebsEndUserSalesAmt,
    ebsTotalAllowedRebate,
    ebsTotalPaybackAmount,
    ebsUnitRebateAmount,
    endUserPrmsNumber,
    endUserProtectedPrice,
    gpoContractApprover,
    gpoContractDescription,
    gpoContractType,
    gpoExporationDate,
    gpoId,
    gpoInitiator,
    gpoStartDate,
    gpoStatus,
    prod.primaryUomConv / 2 as pairUomConv,
    posFlag,
    posSource,
    prod.productdivision,
    prod.primarysellinguom as primarysellinguom,
    prod.primaryUomConv as primaryUomConv,
    rejectReason,
    reportDate,
    resaleLineStatus,
    reSaleTransferType,
    endUserParty_ID,          
    approvalDate,
    transactionAmount,
    distributorInvoiceNumber,
    distributorOrderNumber,
    account_ID,
    dateInvoiced_ID,
    item_ID,
    owningBusinessUnit_ID,
    distributorParty_ID,
    point_of_sales_agg._SOURCE
  FROM point_of_sales_agg
  left join s_core.party_agg on party_agg._id = point_of_sales_agg.endUserParty_ID
  left join s_core.product_agg prod on prod._id = point_of_sales_agg.item_id
  left join s_core.account_agg acc on acc._id = point_of_sales_agg.account_ID
  WHERE 
    YEAR(point_of_sales_agg.dateInvoiced) >= YEAR(current_date) - 3
    AND point_of_sales_agg._SOURCE = 'EBS'
    AND point_of_sales_agg._DELETED IS FALSE
""")

main_df.cache()
main_df.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("dateInvoiced"))
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
