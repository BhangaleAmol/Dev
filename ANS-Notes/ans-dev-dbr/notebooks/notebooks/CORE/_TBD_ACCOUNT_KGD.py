# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'kgd.dbo_tembo_customer', prune_days)
  dbo_tembo_customer = load_incr_dataset('kgd.dbo_tembo_customer', 'LastModifyDate', cutoff_value)
else:
  dbo_tembo_customer = load_full_dataset('kgd.dbo_tembo_customer')

# COMMAND ----------

# SAMPLING
if sampling:
  dbo_tembo_customer = dbo_tembo_customer.limit(10)

# COMMAND ----------

# VIEWS
dbo_tembo_customer.createOrReplaceTempView('dbo_tembo_customer')

# COMMAND ----------

main = spark.sql("""
SELECT
  dbo_tembo_customer.CreateBy AS createdBy,
  CAST(dbo_tembo_customer.CreateDate AS TIMESTAMP) AS createdOn,
  CAST(NULL AS STRING)  AS modifiedBy,
  CAST(dbo_tembo_customer.LastModifyDate AS TIMESTAMP) AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  REPLACE(
    STRING(INT(dbo_tembo_customer.Cust_ID)),
    ",",
    ""
  ) AS accountId,
  CAST(NULL AS STRING) AS accountGroup,
  
  REPLACE(
    STRING(INT(dbo_tembo_customer.Cust_ID)),
    ",",
    ""
  ) AS accountNumber,
  CAST(NULL AS STRING) AS accountStatus, 
  dbo_tembo_customer.CustomerType AS accountType,
  CAST(NULL AS STRING) AS address1Line1,
  CAST(NULL AS STRING) AS address1Line2,
  CAST(NULL AS STRING) AS address1Line3,
  CAST(NULL AS STRING) AS address1City,
  dbo_tembo_customer.AddressCountry AS address1Country,
  dbo_tembo_customer.PostalCode AS address1PostalCode,
  CAST(NULL AS STRING) AS address1State,
  CAST(NULL AS STRING) AS businessGroupId,
  CAST(NULL AS STRING) AS businessType,
  CAST(NULL AS STRING) AS client,
  CAST(NULL AS STRING) AS customerDivision,
  CAST(NULL AS STRING) AS customerSegmentation,
  CAST(NULL AS STRING) AS customerTier, 
  CAST(NULL AS STRING) AS customerType,
  CAST(NULL AS STRING)                      AS ebsAccountNumber,
  CAST(NULL AS boolean) AS eCommerceFlag,
  CAST(NULL AS STRING)                      AS erpId,
  CASE
    WHEN trim(dbo_tembo_customer.ForecastCode) = '' THEN 'Other'
    ELSE trim(dbo_tembo_customer.ForecastCode)
  END AS forecastGroup,
  dbo_tembo_customer.GBU AS  gbu,
  CAST(NULL AS STRING) AS globalRegion,
  CAST(NULL AS STRING) AS  industry,
  CAST(NULL AS boolean) AS isActive,
  CAST(NULL AS boolean) AS isDeleted,
  CAST(NULL AS STRING)                      AS kingdeeId,
  CAST(NULL AS STRING)   						AS locationType,
  CAST(NULL AS STRING) AS marketType,
  dbo_tembo_customer.CustomerName AS name,
  CAST(NULL AS boolean)   					AS nationalAccount,
  CAST(NULL AS int)   						AS numberOperatingRooms,
  CAST(NULL AS STRING)   					AS organization,
  CAST(NULL AS STRING)   					AS ownerId,
  CAST(NULL AS STRING) AS ownerRegion,
  CAST(NULL AS STRING)   					AS parentAccountId,
  CAST(NULL AS STRING)                      AS partyId,
  CAST(NULL AS STRING)                      AS priceListId,
  CAST(NULL AS STRING)            			AS region,
  CAST(NULL AS STRING)                      AS registrationId,
  CAST(NULL AS STRING)                      AS sapId,
  CAST(NULL AS STRING)						AS salesOrganizationID,
  CAST(NULL AS boolean)  						AS syncWithGuardian,
  CAST(NULL AS STRING)   				    AS subIndustry,
  CAST(NULL AS STRING)   						AS subVertical,
  CAST(NULL AS STRING) AS territoryId,
  CAST(NULL AS STRING)   						AS topXAccountGuardian,
  CAST(NULL AS STRING)   						AS topXTargetGuardian,
   CAST(NULL AS STRING) AS vertical
FROM
  dbo_tembo_customer
WHERE NOT(_DELETED)
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
  spark.table('kgd.dbo_tembo_customer')
  .filter("NOT(_DELETED)")
  .selectExpr("REPLACE(STRING(INT(Cust_ID)),',','') AS accountId")
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
  cutoff_value = get_incr_col_max_value(dbo_tembo_customer, 'LastModifyDate')
  update_cutoff_value(cutoff_value, table_name, 'kgd.dbo_tembo_customer')
  update_run_datetime(run_datetime, table_name, 'kgd.dbo_tembo_customer')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
