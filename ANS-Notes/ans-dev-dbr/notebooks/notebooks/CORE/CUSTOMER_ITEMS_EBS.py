# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.customer_item

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.mtl_customer_items', prune_days)
  main_inc = load_incr_dataset('ebs.mtl_customer_items', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.mtl_customer_items')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

main_grainger_bulk = spark.sql("""
 SELECT
  NULL AS createdBy,
  NULL AS createdOn,
  NULL AS modifiedBy,
  NULL AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  REPLACE(
    STRING(INT(hza.CUST_ACCOUNT_ID )),
    ",",
    ""
  ) AS customerId,
  GCT.GraingerSKU AS foreignCustomerItemNumber,
    REPLACE(
    STRING(INT(msib.inventory_item_id)),
    ",",
    ""
  )  AS foreignCustomerItemId,
  'GRAINGER BULK' AS crossReferenceType,
  False AS inactiveFlag,
  REPLACE(
    STRING(INT(msib_Bulk.inventory_item_id )),
    ",",
    ""
  )  AS internalItemId,
  BulkSKU AS internalItemNumber,
  1 preferenceNumber
FROM
  amazusftp1.grainger_conversion_table GCT
  join ebs.mtl_system_items_b msib
    on GCT.GraingerSKU = msib.segment1
  join ebs.mtl_system_items_b msib_Bulk
    on GCT.BulkSKU = msib_Bulk.segment1
  join ebs.hz_cust_accounts hza
    on  hza.ACCOUNT_NUMBER = '10624'
  where 
    msib.organization_id = 124
    and msib_Bulk.organization_id = 124
  """)
main_grainger_bulk.cache()
display(main_grainger_bulk)

# COMMAND ----------

main_ebs=spark.sql("""
WITH CTE AS
(SELECT 
  REPLACE(
    STRING(INT(mci.CREATED_BY)),
    ",",
    ""
  ) AS createdBy,
  mci.CREATION_DATE AS createdOn,
  REPLACE(
    STRING(INT(mci.LAST_UPDATED_BY)),
    ",",
    ""
  ) AS modifiedBy,
  mci.LAST_UPDATE_DATE  AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  REPLACE(
    STRING(INT(mci.CUSTOMER_ID)),
    ",",
    ""
  ) AS customerId,
  mci.CUSTOMER_ITEM_NUMBER AS foreignCustomerItemNumber,
  NULL  AS foreignCustomerItemId,
  'CUSTOMER_ITEM' AS crossReferenceType,
  NULL AS inactiveFlag,
  REPLACE(
    STRING(INT(mcixrf.INVENTORY_ITEM_ID)),
    ",",
    ""
  )  AS internalItemId,
  msib.segment1 AS internalItemNumber,
  mcixrf.preference_number preferenceNumber
FROM
     main_inc mci
     inner join ebs.mtl_customer_item_xrefs mcixrf on mcixrf.customer_item_id = mci.customer_item_id
     inner join ebs.mtl_system_items_b msib on mcixrf.INVENTORY_ITEM_ID = msib.inventory_item_id
 where 
   msib.organization_id = 124
   and mcixrf.inactive_flag = 'N'
--  and mci.customer_item_id in (119259, 810610)
)
SELECT max(createdBy) AS createdBy,
       max(createdOn) AS createdOn,
       max(modifiedBy) AS modifiedBy,
	   max(modifiedOn) AS modifiedOn,
	   insertedOn,
	   updatedOn,
	   customerId,
       max(foreignCustomerItemNumber) as foreignCustomerItemNumber,
       foreignCustomerItemId,
       crossReferenceType,
       inactiveFlag,
       internalItemId,
       internalItemNumber,
       preferenceNumber
FROM 
       CTE 
	   Group by 
       insertedOn,
       updatedOn,
	   customerId,
       foreignCustomerItemId,
       crossReferenceType,
       inactiveFlag,
       internalItemId,
       internalItemNumber,
       preferenceNumber""")

main_ebs.cache()
display(main_ebs)
main_ebs.createOrReplaceTempView('main_ebs')

# COMMAND ----------

main = main_grainger_bulk.union(main_ebs)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['customerId','foreignCustomerItemNumber', 'preferenceNumber'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_customer_items())
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
full_keys_ebs = spark.sql("""
  SELECT
	REPLACE(STRING(INT(mci.CUSTOMER_ID)), ",", "") AS customerId,
    mci.CUSTOMER_ITEM_NUMBER AS foreignCustomerItemNumber,
	mcixrf.preference_number preferenceNumber
  FROM ebs.MTL_CUSTOMER_ITEMS mci
  INNER JOIN ebs.mtl_customer_item_xrefs mcixrf ON mcixrf.customer_item_id = mci.customer_item_id
""")

full_keys_grainger = spark.sql("""
  SELECT
    REPLACE(STRING(INT(hza.CUST_ACCOUNT_ID )), ",", "") AS customerId,
    GCT.GraingerSKU AS foreignCustomerItemNumber,
    1 preferenceNumber
  FROM amazusftp1.grainger_conversion_table GCT
  JOIN ebs.mtl_system_items_b msib ON GCT.GraingerSKU = msib.segment1
  JOIN ebs.mtl_system_items_b msib_Bulk ON GCT.BulkSKU = msib_Bulk.segment1
  JOIN ebs.hz_cust_accounts hza ON hza.ACCOUNT_NUMBER = '10624'
  WHERE msib.organization_id = 124 AND msib_Bulk.organization_id = 124
""")

full_keys_f = (
	full_keys_grainger
	.union(full_keys_ebs)
	.transform(attach_source_column(source = source_name))
	.transform(attach_surrogate_key(columns = 'customerId,foreignCustomerItemNumber,preferenceNumber,_SOURCE'))
	.select('_ID')
    .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'customerId,_SOURCE', 'customer_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc)
  update_cutoff_value(cutoff_value, table_name, 'ebs.MTL_CUSTOMER_ITEMS')
  update_run_datetime(run_datetime, table_name, 'ebs.MTL_CUSTOMER_ITEMS')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
