# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.lost_business

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sf.lost_business__c', prune_days)
  lostbusiness = load_incr_dataset('sf.lost_business__c', 'LastModifiedDate', cutoff_value)
else:
  lostbusiness = load_full_dataset('sf.lost_business__c')

# COMMAND ----------

# SAMPLING
if sampling:
  lostbusiness = lostbusiness.limit(10)

# COMMAND ----------

# VIEWS
lostbusiness.createOrReplaceTempView('lostbusiness')

# COMMAND ----------

main = spark.sql("""
SELECT
  CreatedDate AS createdOn,
  CreatedById AS createdBy,
  LastModifiedDate AS modifiedOn,
  LastModifiedById AS modifiedBy,
  CURRENT_DATE AS insertedOn,
  CURRENT_DATE AS updatedOn,
  Account_Name__c AS accountName,
  Billing_State__c AS billingState,
  Business_not_in_SFDC__c AS businessNotInSfdc,
  Competitor_list__c AS competitorList,
  Competitor_Product__c AS competitorProduct,
  CurrencyIsoCode AS currencyIsoCode,
  Distributor__c AS distributor,
  Industry_Segment__c AS industrySegment,
  IsDeleted AS isDeleted,
  Last_Order_Date__c AS lastOrderDate,
  LastActivityDate AS lastActivityDate,
  Lost_Business_Amount__c AS lostBusinessAmount,
  Id AS lostBusinessId,
  Lost_Reason_Comments__c AS lostReasonComments,
  Name AS name,
  Organisation__c AS organisation,
  OwnerId AS ownerId,
  Previous_Won_Opportunity__c AS previousWonOpportunity,
  Primary_Loss_Reason__c AS primaryLossReason,
  RecordTypeId AS recordTypeId,
  Region__c AS region,
  Region_Old__c AS regionOld,
  Sales_Division__c AS salesDivision,
  Status__c AS status,
  Supplied_From__c AS suppliedFrom,
  Territories__c AS territories,
  Total_Quantity__c AS totalQuantity,
  Total_Value__c AS totalValue,
  Vertical__c AS vertical,
  Vertical_Old__c AS verticalOld
FROM
  lostbusiness
  
""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'lostbusinessId')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .withColumn('orderType',f.lit('ORDER_TYPE'))
  .transform(tg_default(source_name))
  .transform(tg_service_lost_business())
  .transform(apply_schema(schema))
)

# main_f.cache()

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main_f, '_ID')

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)
add_unknown_record(main_f, table_name)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('sf.lost_business__c')
  .selectExpr('Id lostbusinessId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('lostbusinessId,_source', 'edm.lost_business'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')


# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(lostbusiness, "LastModifiedDate")
  update_cutoff_value(cutoff_value, table_name, 'sf.lost_business__c')
  update_run_datetime(run_datetime, table_name, 'sf.lost_business__c')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
