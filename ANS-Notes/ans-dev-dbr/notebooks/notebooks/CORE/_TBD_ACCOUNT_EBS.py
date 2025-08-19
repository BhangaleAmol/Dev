# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.hz_cust_accounts', prune_days)
  hz_cust_accounts = load_incr_dataset('ebs.hz_cust_accounts', 'LAST_UPDATE_DATE', cutoff_value)
else:
  hz_cust_accounts = load_full_dataset('ebs.hz_cust_accounts')

# COMMAND ----------

# SAMPLING
if sampling:
  hz_cust_accounts = hz_cust_accounts.limit(10)

# COMMAND ----------

# VIEWS
hz_cust_accounts.createOrReplaceTempView('hz_cust_accounts')

# COMMAND ----------

main = spark.sql("""
SELECT
  REPLACE(
    STRING(INT (hz_cust_accounts.CREATED_BY)),
    ",",
    ""
  ) AS createdBy,
  CAST(hz_cust_accounts.CREATION_DATE AS TIMESTAMP) AS createdOn,
  REPLACE(
    STRING(INT (hz_cust_accounts.LAST_UPDATED_BY)),
    ",",
    ""
  ) AS modifiedBy,
  CAST(hz_cust_accounts.LAST_UPDATE_DATE AS TIMESTAMP) AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  REPLACE(
    STRING(INT(hz_cust_accounts.CUST_ACCOUNT_ID)),
    ",",
    ""
  ) AS accountId,
  hz_cust_accounts.ATTRIBUTE10 AS accountGroup,
  
  REPLACE(
    STRING(INT(hz_cust_accounts.ACCOUNT_NUMBER)),
    ",",
    ""
  ) AS accountNumber,
  CAST(NULL AS STRING) AS accountStatus,
  HZ_CUST_ACCOUNTS.ATTRIBUTE4 AS accountType,
  CAST(NULL AS STRING) AS address1Line1,
  CAST(NULL AS STRING) AS address1Line2,
  CAST(NULL AS STRING) AS address1Line3,
  CAST(NULL AS STRING) AS address1City,
  CAST(NULL AS STRING) AS address1Country,
  CAST(NULL AS STRING) AS address1PostalCode,
  CAST(NULL AS STRING) AS address1State,
  CAST(NULL AS STRING) AS businessGroupId,
  CAST(NULL AS STRING) AS businessType,
  '001' AS client,
  hz_cust_accounts.ATTRIBUTE13 AS customerDivision,
  hz_cust_accounts.ATTRIBUTE20 customerSegmentation,
  hz_cust_accounts.ATTRIBUTE19 customerTier, 
  CASE
    WHEN hz_cust_accounts.CUSTOMER_TYPE = 'I' THEN 'Internal'
    ELSE 'External'
  END AS customerType,
  CAST(NULL AS STRING)                        AS ebsAccountNumber,
  CASE 
    WHEN hz_cust_accounts.ATTRIBUTE18 = 'YES' then TRUE 
    ELSE FALSE 
  END AS eCommerceFlag,
  CAST(NULL AS STRING)                        AS erpId,
  nvl(hz_cust_accounts.ATTRIBUTE8, 'Other') AS forecastGroup,
  hz_cust_accounts.ATTRIBUTE1 gbu,
  CAST(NULL AS STRING) AS globalRegion,
  hz_cust_accounts.ATTRIBUTE2 industry,
  True AS isActive,
  false AS isDeleted,
  CAST(NULL AS STRING)                        AS kingdeeId,
  CAST(NULL AS STRING)   						AS locationType,
  CAST(NULL AS STRING) AS marketType,
  hz_cust_accounts.ACCOUNT_NAME AS name,
  CAST(NULL AS boolean)   					AS nationalAccount,
  CAST(NULL AS int)   						AS numberOperatingRooms,
  CAST(NULL AS STRING)   					AS organization,
  CAST(NULL AS STRING)   					AS ownerId,
  CAST(NULL AS STRING) AS ownerRegion,
  CAST(NULL AS STRING)   					AS parentAccountId,
  REPLACE(
    STRING(INT(hz_cust_accounts.PARTY_ID)),
    ",",
    ""
  ) AS partyId,
  hz_cust_accounts.PRICE_LIST_ID            AS priceListId,
  hz_cust_accounts.ATTRIBUTE15   			AS region,
   Case 
    when hz_cust_accounts.attribute4 is not null
    and hz_cust_accounts.price_list_id is not null
    and hz_cust_accounts.status = 'A' then HZ_PARTIES.PARTY_NUMBER 
                else null 
 end
 AS registrationId,
  CAST(NULL AS STRING)                        AS sapId,
  CAST(NULL AS STRING)						AS salesOrganizationID,
  CAST(NULL AS boolean)  						AS syncWithGuardian,
  hz_cust_accounts.ATTRIBUTE3  				    AS subIndustry,
  CAST(NULL AS STRING)   						AS subVertical,
  CAST(NULL AS STRING) AS territoryId,
  CAST(NULL AS STRING)   						AS topXAccountGuardian,
  CAST(NULL AS STRING)   						AS topXTargetGuardian,
  hz_cust_accounts.ATTRIBUTE11 AS vertical
FROM
  hz_cust_accounts
  LEFT JOIN EBS.HZ_PARTIES ON HZ_PARTIES.PARTY_ID = hz_cust_accounts.PARTY_ID
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
  spark.table('ebs.HZ_CUST_ACCOUNTS')
  .selectExpr("REPLACE(STRING(INT(CUST_ACCOUNT_ID)),',','') AS accountId")
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
  cutoff_value = get_incr_col_max_value(hz_cust_accounts, 'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'ebs.HZ_CUST_ACCOUNTS')
  update_run_datetime(run_datetime, table_name, 'ebs.HZ_CUST_ACCOUNTS')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
