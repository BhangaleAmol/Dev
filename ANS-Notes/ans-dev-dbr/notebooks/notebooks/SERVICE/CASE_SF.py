# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.case

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sf.case', prune_days)
  case = load_incr_dataset('sf.case', 'LastModifiedDate', cutoff_value)
else:
  case = load_full_dataset('sf.case')

# COMMAND ----------

# SAMPLING
if sampling:
  case = case.limit(10)

# COMMAND ----------

# VIEWS
case.createOrReplaceTempView('case')

# COMMAND ----------

main = spark.sql("""
SELECT
  CreatedById createdBy,
  CreatedDate createdOn,
  LastModifiedById modifiedBy,
  LastModifiedDate modifiedOn,
  CURRENT_TIMESTAMP() insertedOn,
  CURRENT_TIMESTAMP() updatedOn,
  AccountId AS accountid,
  Account_Subregion__c AS accountSubregion,
  id AS caseId,
  CaseNumber AS caseNumber ,
  CASE WHEN Category__c IS NULL THEN 'Unspecified' ELSE Category__c END AS category,
  closeddate AS closedDate,
  IsClosed AS isClosed,
  IsDeleted AS isDeleted,
  OwnerId AS ownerId,
  owner_region__c AS ownerRegion,
  PO_Number__c AS purchaseOrderNumber, 
  RecordTypeID AS recordTypeId,
  Resolution__c AS resolution,
  Order_Number__c AS salesOrderNumber,
  CASE WHEN SubCategory__C IS NULL THEN 'Unspecified' ELSE SubCategory__C END AS subCategory
   FROM
   case

  
""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'caseId')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .withColumn('orderType',f.lit('ORDER_TYPE'))
  .transform(tg_default(source_name))
  .transform(tg_service_case())
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
  spark.table('sf.case')
  .selectExpr('Id caseId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'caseId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(case, "LastModifiedDate")
  update_cutoff_value(cutoff_value, table_name, 'sf.case')
  update_run_datetime(run_datetime, table_name, 'sf.case')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
