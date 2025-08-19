# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account

# COMMAND ----------

# LOAD DATASETS
prmsf200_mscmp100 = load_full_dataset('prms.prmsf200_mscmp100')
prmsf200_mscmp100.createOrReplaceTempView('prmsf200_mscmp100')

# COMMAND ----------

# SAMPLING
if sampling:
  prmsf200_mscmp100 = prmsf200_mscmp100.limit(10)
  prmsf200_mscmp100.createOrReplaceTempView('prmsf200_mscmp100')

# COMMAND ----------

main = spark.sql("""
SELECT
  MSCMZ100.USRIDA AS createdBy,
  CAST(MSCMZ100.ADDDT AS TIMESTAMP) AS createdOn,
  MSCMZ100.USRIDA AS modifiedBy,
  CAST(MSCMZ100.ADDDT AS TIMESTAMP) AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  REPLACE(STRING(INT(MSCMP100.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(MSCMP100.CUSNO)), ",", "") AS accountId,
  CAST(NULL AS STRING) AS accountGroup,
  REPLACE(STRING(INT(MSCMP100.CUSNO)), ",", "") AS accountNumber,
  CAST(NULL AS STRING)   						AS accountStatus,
  CAST(NULL AS STRING)   						AS accountType,
  MSCMP100.CADD1   						   		AS address1Line1,
  MSCMP100.CADD2   						   		AS address1Line2,
  MSCMP100.CADDX || ' ' || MSCMP100.CADD3   	AS address1Line3,
  CAST(NULL AS STRING)   						AS address1City,
  CAST(NULL AS STRING)   						AS address1Country,
  MSCMP100.CZIPC   						   		AS address1PostalCode,
  MSCMP100.CSTTE   						   		AS address1State,
  CAST(NULL AS STRING)   						AS businessGroupId,
  CAST(NULL AS STRING)   						AS businessType,
  '001' AS client,
  MSCMZ100.CADIV   						   		AS customerDivision,
  CAST(NULL AS STRING)   						AS customerSegmentation,
  CAST(NULL AS STRING)   						AS customerTier,
  CASE
    WHEN MSCMZ100.ICCFLG = 'Y' THEN 'Internal'
    ELSE 'External'
  END   						   				AS customerType,
  CAST(NULL AS STRING)                          AS ebsAccountNumber,
  CAST(NULL AS boolean)                         AS eCommerceFlag,
  CAST(NULL AS STRING)                          AS erpId,
  'Other'                                       AS forecastGroup,
  CAST(NULL AS STRING)   						AS gbu,
  CAST(NULL AS STRING)   						AS globalRegion,
  CAST(NULL AS STRING)   						AS industry,
  TRUE   						   				AS isActive,
  false   						   				AS isDeleted,
  CAST(NULL AS STRING)                          AS kingdeeId,
  CAST(NULL AS STRING)   						AS locationType,
  CAST(NULL AS STRING)   						AS marketType,
  MSCMP100.CNAME        						AS name,
  CAST(NULL AS boolean)   					    AS nationalAccount,
  CAST(NULL AS int)   						    AS numberOperatingRooms,
  CAST(NULL AS STRING)   						AS organization,
  CAST(NULL AS STRING)   						AS ownerId,
  CAST(NULL AS STRING)   						AS ownerRegion,
  CAST(NULL AS STRING)   						AS parentAccountId,
  CAST(NULL AS STRING) 						    AS partyId,
  CAST(NULL AS STRING)                          AS priceListId,
  CAST(NULL AS STRING)   						AS region,
  CAST(NULL AS STRING)   						AS registrationId,
  CAST(NULL AS STRING)                          AS sapId,
  CAST(NULL AS STRING)						AS salesOrganizationID,
  CAST(NULL AS boolean)  						AS syncWithGuardian,
  CAST(NULL AS STRING)   						AS subIndustry,
  CAST(NULL AS STRING)   						AS subVertical,
  CAST(NULL AS STRING)   						AS territoryId,
  CAST(NULL AS STRING)   						AS topXAccountGuardian,
  CAST(NULL AS STRING)   						AS topXTargetGuardian,
  CAST(NULL AS STRING)   						AS vertical
FROM
  prmsf200_mscmp100 mscmp100
  LEFT JOIN prms.prmsf200_mscmz100 mscmz100 ON mscmp100.cusno = mscmz100.cusno
  AND mscmp100.cmpno = mscmz100.cmpno
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
  spark.table('prms.prmsf200_mscmp100')
  .filter('_DELETED IS FALSE')
  .selectExpr("REPLACE(STRING(INT(CMPNO)), ',', '') || '-' || REPLACE(STRING(INT(CUSNO)), ',', '') AS accountId")
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
  cutoff_value = get_incr_col_max_value(prmsf200_mscmp100)
  update_cutoff_value(cutoff_value, table_name, 'prms.prmsf200_mscmp100')
  update_run_datetime(run_datetime, table_name, 'prms.prmsf200_mscmp100')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
