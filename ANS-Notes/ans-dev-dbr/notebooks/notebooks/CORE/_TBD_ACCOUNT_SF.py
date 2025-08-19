# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account

# COMMAND ----------

# LOAD DATASETS
account = load_full_dataset('sf.account')
account.createOrReplaceTempView('account')

# COMMAND ----------

# SAMPLING
if sampling:
  account = account.limit(10)
  account.createOrReplaceTempView('account')

# COMMAND ----------

main = spark.sql("""
SELECT distinct
  CreatedById AS createdBy,
  CreatedDate AS createdOn,
  LastModifiedById AS modifiedBy,
  LastModifiedDate AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  id  AS accountId,
  CAST(NULL AS STRING) AS accountGroup,
  id  AS accountNumber,
  status__c AS accountStatus,
  Type__c  AS accountType,
  BillingStreet  AS address1Line1,
  CAST(NULL AS STRING) AS address1Line2,
  CAST(NULL AS STRING) AS address1Line3,
  BillingCity AS address1City,
  BillingCountry AS address1Country,
  BillingPostalCode AS address1PostalCode,
  BillingState AS address1State,
  CAST(NULL AS STRING)      AS businessGroupId,
  Business_Type__c   	AS businessType,
  '030' AS client,
  CAST(NULL AS STRING) AS customerDivision,
  CAST(NULL AS STRING) AS customerSegmentation,
  Customer_Tier__c AS customerTier,
  'External' AS customerType,
  EBS_Account_Number__c AS ebsAccountNumber,
  CAST(NULL AS boolean) AS eCommerceFlag,
  ERP_Id__c AS erpId,
  'Other'               AS forecastGroup,
  CAST(NULL AS STRING) AS gbu,
  gr.cvalue AS globalRegion,
  CAST(NULL AS STRING) AS industry,
  not isdeleted AS isActive,
  isdeleted AS isDeleted,
  Kingdee_ID__c AS kingdeeId,
  Location_Type__c AS locationType,
  mt.cvalue AS marketType,
  name AS name,
  National_Account__c   	AS nationalAccount,
  No_of_Operating_Rooms__c	AS numberOperatingRooms,
  Organisation2__c   		AS organization,
  OwnerId   				AS ownerId,
  Owner_Region__c AS ownerRegion,
  ParentId                  AS parentAccountId,
  CAST(NULL AS STRING) 		AS partyId,
  CAST(NULL AS STRING)      AS priceListId,
  region__c  				AS region,
  CAST(NULL AS STRING)      AS registrationId,
  SAP_Sold_To_Party__c      AS sapId,
  Territory__c              AS salesOrganizationID,
  Sync_with_Guardian__c  	AS syncWithGuardian,
  CAST(NULL AS STRING)      AS subIndustry,
  Sub_Vertical__c  			AS subVertical,
  Territory__c AS territoryId,
  Top_X_Account_Guardian__c AS topXAccountGuardian,
  Top_X_Target_Guardian__c  AS topXTargetGuardian,
  Vertical__c  AS vertical
FROM
  account
  left join smartsheets.edm_control_table gr on region__c = gr.key_value and gr.TABLE_ID = 'SFDC_REGIONS' AND gr.ACTIVE_FLG is True 
  left join smartsheets.edm_control_table mt on region__c = mt.key_value and mt.TABLE_ID = 'SFDC_MARKET_TYPE' AND mt.ACTIVE_FLG is True 
  """)
main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, key_columns = ['accountId'], tableName = table_name, sourceName = source_name, notebookName = NOTEBOOK_NAME, notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_account())
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.display()

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('sf.account')
  .filter('_DELETED IS FALSE')
  .selectExpr("id AS accountId")
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('accountId,_SOURCE', 'edm.account'))
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
  cutoff_value = get_incr_col_max_value(account)
  update_cutoff_value(cutoff_value, table_name, 'sf.account')
  update_run_datetime(run_datetime, table_name, 'sf.account')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
