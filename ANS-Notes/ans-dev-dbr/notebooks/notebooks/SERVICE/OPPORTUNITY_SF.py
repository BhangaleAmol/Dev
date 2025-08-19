# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.opportunity

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sf.opportunity', prune_days)
  opportunity = load_incr_dataset('sf.opportunity', 'LastModifiedDate', cutoff_value)
else:
  opportunity = load_full_dataset('sf.opportunity')

# COMMAND ----------

# SAMPLING
if sampling:
  opportunity = opportunity.limit(10)

# COMMAND ----------

# VIEWS
opportunity.createOrReplaceTempView('opportunity')

# COMMAND ----------

main = spark.sql("""
SELECT
  CreatedById createdBy,
  CreatedDate createdOn,
  LastModifiedById modifiedBy,
  LastModifiedDate modifiedOn,
  CURRENT_TIMESTAMP() insertedOn,
  CURRENT_TIMESTAMP() updatedOn,
  AccountId AS accountID,
  Amount AS amount,
  CloseDate AS closeDate,
  Competitor__c AS competitor,
  Covid_Opportunity__c AS covidOpportunity,
  CreatedById AS createdById,
  CreatedDate AS createdDate,
  Currency__c AS currency,
  CurrencyIsoCode AS currencyIsoCode,
  Distributor_lookup__c AS distributor,
  Distributor_Location__c AS distributorLocation,
  End_User_relationship__c AS endUserRelationship,
  Esales_ID__c AS eSalesId,
  IsClosed AS isClosed,
  IsWon AS isWon,
  LastActivityDate AS lastActivityDate,
  LastModifiedById AS lastModifiedById,
  LastModifiedDate AS lastModifiedDate,
  LeadSource AS leadSource,
  CASE WHEN iswon = 'false' AND  isclosed = 'true' THEN Id END  AS lostOpportunity,
  CASE WHEN isclosed = 'false' THEN Id END AS openOpportunity,
  id AS opportunityId,
  Name AS opportunityName,
  Opportunity_Number__c AS opportunityNumber,
  Opportunity_Owner_Name__c AS opportunityOwnerName,
  Organisation2__c AS organisation2,
  OwnerId AS ownerId,
  Primary_Loss_Reason__c AS primaryLossReason,
  Probability AS probability,
  Product_Code__c AS productCode,
  Product_focus__c AS productFocus,
  RecordTypeId AS recordTypeId,
  Region__c AS region,
  StageName AS stageName,
  Supplied_From__c AS suppliedFrom,
  Territory__c AS territory,
  TotalOpportunityQuantity AS totalOpportunityQuantity,
  Type AS type,
  Warehouse_Location__c AS warehouseLocation,
  CASE WHEN iswon = 'true' and isclosed = 'true' THEN Id END AS wonOpportunity
   FROM
   opportunity

  
""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'opportunityId')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_service_opportunity())
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
  spark.table('sf.opportunity')
  .selectExpr('Id opportunityId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'opportunityId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(opportunity, "LastModifiedDate")
  update_cutoff_value(cutoff_value, table_name, 'sf.opportunity')
  update_run_datetime(run_datetime, table_name, 'sf.opportunity')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
