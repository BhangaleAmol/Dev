# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.business_overview

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sf.business_overview__c', prune_days)
  businessoverview = load_incr_dataset('sf.business_overview__c', 'LastModifiedDate', cutoff_value)
else:
  businessoverview = load_full_dataset('sf.business_overview__c')

# COMMAND ----------

# SAMPLING
if sampling:
  businessoverview = businessoverview.limit(10)

# COMMAND ----------

# VIEWS
businessoverview.createOrReplaceTempView('businessoverview')

# COMMAND ----------

main = spark.sql("""
SELECT
  CreatedDate AS createdOn,
  CreatedById AS createdBy,
  LastModifiedDate AS modifiedOn,
  LastModifiedById AS modifiedBy,
  CURRENT_DATE AS insertedOn,
  CURRENT_DATE AS updatedOn,
  Account__c AS account,
  Account_Organisation__c AS accountOrganisation,
  Account_Owner__c AS accountOwner,
  Account_Region__c AS accountRegion,
  Account_Type__c AS accountType,
  Business_Type__c AS businessType,
  Comments__c AS comments,
  Competitive_Product__c AS competitiveProduct,
  Competitor__c AS competitor,
  CurrencyIsoCode AS currencyIsoCode,
  Distributor__c AS distributor,
  Distributor_Region__c AS distributorRegion,
  Guardian_Opportunity__c AS guardianOpportunity,
  Id AS businessOverviewId,
  IsDeleted AS isDeleted,
  Linked_Opportunity__c AS linkedOpportunity,
  Lost_Date__c AS lostDate,
  Name AS name,
  NrOfProducts__c AS nrOfProducts,
  Opportunity_Type__c AS opportunityType,
  Original_Opportunity_Close_Date__c AS originalOpportunityCloseDate,
  Original_Opportunity_Creation_Date__c AS originalOpportunityCreationDate,
  Original_Opportunity_Owner__c AS originalOpportunityOwner,
  Original_Opportunity_Total_Value__c AS originalOpportunityTotalValue,
  Reason_for_Loss__c AS reasonForLoss,
  RecordTypeId AS recordTypeId,
  Reporting_Date__c AS reportingDate,
  Total_Value_Products__c AS totalValueProducts,
  Total_Value_Products_with_User_Currency__c AS totalValueUserCurrency,
  Type__c AS type,
  X1st_PO_Date__c AS x1stPoDate
FROM
  businessoverview
  
""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'businessoverviewId')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .withColumn('orderType',f.lit('ORDER_TYPE'))
  .transform(tg_default(source_name))
  .transform(tg_service_business_overview())
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
  spark.table('sf.business_overview__c')
  .selectExpr('Id businessoverviewId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('businessoverviewId,_source', 'edm.business_overview'))
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
  cutoff_value = get_incr_col_max_value(businessoverview, "LastModifiedDate")
  update_cutoff_value(cutoff_value, table_name, 'sf.business_overview__c')
  update_run_datetime(run_datetime, table_name, 'sf.business_overview__c')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
