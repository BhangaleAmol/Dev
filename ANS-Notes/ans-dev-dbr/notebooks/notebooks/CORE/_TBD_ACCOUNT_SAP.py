# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sapp01.kna1', prune_days)
  sap_kna1 = load_incr_dataset('sapp01.kna1', '_Modified', cutoff_value)
else:
  sap_kna1 = load_full_dataset('sapp01.kna1')
  
# VIEWS  
sap_kna1.createOrReplaceTempView('sap_kna1')

# COMMAND ----------

# SAMPLING
if sampling:
  sap_kna1 = sap_kna1.limit(10)
  sap_kna1.createOrReplaceTempView('sap_kna1')

# COMMAND ----------

main = spark.sql("""
select DISTINCT 
  KNA1.ERNAM createdBy,
  KNA1.ERDAT createdOn,
  null AS modifiedBy,
  null AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  KNA1.KUNNR accountId,
  KNA1.KTOKD accountGroup,
  KNA1.KUNNR accountNumber,
  CAST(NULL AS STRING)   AS accountStatus, 
  KNA1.KUKLA accountType,
  KNA1.STRAS address1Line1,
  CAST(NULL AS STRING)  AS address1Line2,
  CAST(NULL AS STRING)  AS address1Line3,
  KNA1.ORT01 address1City,
  KNA1.LAND1 address1Country,
  KNA1.PSTLZ address1PostalCode,
  KNA1.REGIO address1State,
  CAST(NULL AS STRING)  AS businessGroupId,
  KNA1.J_1KFTBUS   AS businessType,
  KNA1.MANDT client,
  CAST(NULL AS STRING)  AS customerDivision,
  CAST(NULL AS STRING)  AS customerSegmentation,
  CAST(NULL AS STRING)  AS customerTier,
  KNA1.KUKLA customerType,
  CAST(NULL AS STRING)  AS ebsAccountNumber,
  CAST(NULL as boolean) AS eCommerceFlag,
  CAST(NULL AS STRING)  AS erpId,
  'Other' forecastGroup, -- tbd
  CAST(null as STRING) as gbu,
  CAST(NULL AS STRING)  AS globalRegion, 
  KNA1.BRSCH industry,
  CAST(NULL as boolean) AS isActive,
  CAST(NULL as boolean) AS isDeleted,
  CAST(NULL AS STRING)  AS kingdeeId,
  CAST(NULL as STRING) as locationType,
  CAST(NULL AS STRING)  AS marketType, 
  TRIM(
    TRIM(KNA1.NAME1) || ' ' || TRIM(KNA1.NAME2) || ' ' || TRIM(KNA1.NAME3) || ' ' || TRIM(KNA1.NAME4)
  ) name,
  CAST(NULL as BOOLEAN) AS nationalAccount,
  CAST(NULL AS INT)  AS numberOperatingRooms,
  CAST(NULL AS STRING)  AS organization,
  CAST(NULL AS STRING)  AS ownerId,
  CAST(NULL AS STRING)  AS ownerRegion, 
  CAST(NULL AS STRING)  AS parentAccountId,
  CAST(NULL AS STRING)  AS partyId,
  CAST(NULL AS STRING)  AS priceListId,
  KNA1.REGIO region,
  CAST(NULL AS STRING)  AS registrationId,
  CAST(NULL AS STRING)  AS sapId,
  CAST(NULL AS STRING)  AS salesOrganizationID,
  CAST(NULL AS BOOLEAN)  AS syncWithGuardian,
  CAST(NULL AS STRING)  AS subIndustry,
  CAST(NULL AS STRING)  AS subVertical,
  CAST(NULL AS STRING)  AS territoryId,
  CAST(NULL AS BOOLEAN)  AS topXaccountGuardian,
  CAST(NULL AS BOOLEAN)  AS topXTargetGuardian,
  CAST(NULL AS STRING)  AS vertical
from
  sap_kna1 kna1
  where length(trim(kunnr))<> 0
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
  spark.table('sapp01.kna1')
  .selectExpr("KNA1.KUNNR AS accountId")
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
  cutoff_value = get_incr_col_max_value(sap_kna1,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'sapp01.kna1')
  update_run_datetime(run_datetime, table_name, 'sapp01.kna1')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
