# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.transaction_type

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.ra_cust_trx_types_all', prune_days)
  ra_cust_trx_types_all = load_incr_dataset('ebs.ra_cust_trx_types_all', 'LAST_UPDATE_DATE', cutoff_value)
  
  cutoff_value = get_cutoff_value(table_name, 'ebs.oe_transaction_types_tl', prune_days)
  oe_transaction_types_tl = load_incr_dataset('ebs.oe_transaction_types_tl', 'LAST_UPDATE_DATE', cutoff_value)  
else:
  ra_cust_trx_types_all = load_full_dataset('ebs.ra_cust_trx_types_all')
  oe_transaction_types_tl = load_full_dataset('ebs.oe_transaction_types_tl')

# COMMAND ----------

# SAMPLING
if sampling:
  ra_cust_trx_types_all = ra_cust_trx_types_all.limit(10)
  oe_transaction_types_tl = oe_transaction_types_tl.limit(10)

# COMMAND ----------

# VIEWS
ra_cust_trx_types_all.createOrReplaceTempView('ra_cust_trx_types_all')
oe_transaction_types_tl.createOrReplaceTempView('oe_transaction_types_tl')

# COMMAND ----------

main_invoice_type = spark.sql("""
select
  CASE
    WHEN ra_cust_trx_types_all.CREATED_BY > 0 THEN REPLACE(
      STRING(INT (ra_cust_trx_types_all.CREATED_BY)),
      ",",
      ""
    )
    ELSE '0'
  END createdBy,
  ra_cust_trx_types_all.CREATION_DATE createdOn,
  CASE
    WHEN ra_cust_trx_types_all.LAST_UPDATED_BY > 0 THEN REPLACE(
      STRING(INT (ra_cust_trx_types_all.LAST_UPDATED_BY)),
      ",",
      ""
    )
    ELSE '0'
  END modifiedBy,
  ra_cust_trx_types_all.LAST_UPDATE_DATE modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  NVL(
    ra_cust_trx_types_all.DESCRIPTION,
    'No Description'
  ) description,
  'WH' dropShipmentFlag,
  ra_cust_trx_types_all.NAME name,
  False as sampleOrderFlag,
  'INVOICE_TYPE' transactionGroup,
  null as  transactionTypeGroup,
  REPLACE(
    STRING(INT (ra_cust_trx_types_all.CUST_TRX_TYPE_ID)),
    ",",
    ""
  ) || '-' || REPLACE(
    STRING(INT (ra_cust_trx_types_all.org_id)),
    ",",
    ""
  ) transactionId,
  ra_cust_trx_types_all.TYPE transactionTypeCode
from
  ra_cust_trx_types_all
  where org_id is not null
""")

main_invoice_type.count()
main_invoice_type.cache()
display(main_invoice_type)

# COMMAND ----------

main_order_type = spark.sql("""
SELECT
  CASE
    WHEN oe_transaction_types_tl.CREATED_BY > 0 THEN REPLACE(
      STRING(INT (oe_transaction_types_tl.CREATED_BY)),
      ",",
      ""
    )
    ELSE '0'
  END createdBy,
  oe_transaction_types_tl.creation_date createdOn,
  CASE
    WHEN oe_transaction_types_tl.CREATED_BY > 0 THEN REPLACE(
      STRING(INT (oe_transaction_types_tl.CREATED_BY)),
      ",",
      ""
    )
    ELSE '0'
  END modifiedBy,
  oe_transaction_types_tl.LAST_UPDATE_DATE modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  oe_transaction_types_tl.description,
  case 
    when upper(OE_TRANSACTION_TYPES_TL.name) like '%DROP%' 
      or upper(OE_TRANSACTION_TYPES_TL.name) like '%DIRECT%' 
    then 'DS' 
    else 'WH' 
  end dropShipmentFlag,
  oe_transaction_types_tl.name,
  CASE WHEN upper(name) LIKE '%SAMPLE%' THEN True ELSE False END as sampleOrderFlag,
  'ORDER_TYPE' transactionGroup,
  upper(name) as  transactionTypeGroup,
  'ORDER' || '-' || REPLACE(
      STRING(INT (oe_transaction_types_tl.transaction_type_id)),
      ",",
      ""
    )  transactionId,
  'ORDER' transactionTypeCode
FROM
  oe_transaction_types_tl
WHERE
  oe_transaction_types_tl.LANGUAGE = 'US'
  
union

SELECT
  CASE
    WHEN oe_transaction_types_tl.CREATED_BY > 0 THEN REPLACE(
      STRING(INT (oe_transaction_types_tl.CREATED_BY)),
      ",",
      ""
    )
    ELSE 0
  END createdBy,
  oe_transaction_types_tl.creation_date createdOn,
  CASE
    WHEN oe_transaction_types_tl.CREATED_BY > 0 THEN REPLACE(
      STRING(INT (oe_transaction_types_tl.CREATED_BY)),
      ",",
      ""
    )
    ELSE 0
  END modifiedBy,
  oe_transaction_types_tl.LAST_UPDATE_DATE modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  oe_transaction_types_tl.description,
  case 
    when upper(OE_TRANSACTION_TYPES_TL.name) like '%DROP%' 
      or upper(OE_TRANSACTION_TYPES_TL.name) like '%DIRECT%' 
    then 'DS' 
    else 'WH' 
  end dropShipmentFlag,
  oe_transaction_types_tl.name,
  CASE WHEN upper(name) LIKE '%SAMPLE%' THEN True ELSE False END as sampleOrderFlag,
  'ORDER_TYPE' transactionGroup,
  upper(name) as  transactionTypeGroup,
  'RETURN' || '-' || REPLACE(
      STRING(INT (oe_transaction_types_tl.transaction_type_id)),
      ",",
      ""
    )  transactionId,
  'RETURN' transactionTypeCode
FROM
  oe_transaction_types_tl
WHERE
  oe_transaction_types_tl.LANGUAGE = 'US'
""")

main_order_type.count()
main_order_type.cache()
display(main_order_type)

# COMMAND ----------

main = main_order_type.union(main_invoice_type)
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['transactionId','transactionGroup'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(map_ebs_transaction_type_group('transactionTypeGroup'))
  .transform(tg_default(source_name))
  .transform(tg_core_transaction_type())
  .transform(apply_schema(schema))
)

main_f.cache()
main_f.display()

# COMMAND ----------

main_e2eFlagTbl = spark.sql("""
SELECT DISTINCT 
   KEY_VALUE
FROM smartsheets.edm_control_table 
WHERE TABLE_ID = 'E2E_EXCLUDE_ORDER_TYPES' 
  and ACTIVE_FLG = true 
  and current_date between VALID_FROM and VALID_TO
  """)

main_e2eFlagTbl.cache()
main_e2eFlagTbl.display()

# COMMAND ----------

main_f = (
main_f
  .join(main_e2eFlagTbl, main_e2eFlagTbl.KEY_VALUE.contains(main_f.transactionTypeGroup), how='left')\
  .withColumn("e2eFlag", f.when(main_f.transactionTypeGroup.contains(main_e2eFlagTbl.KEY_VALUE), False).otherwise(True))\
  .select("createdBy", "createdOn", "modifiedBy", "modifiedOn", "insertedOn", "updatedOn", "description","dropShipmentFlag", "e2eFlag", "name","sampleOrderFlag", "transactionGroup", "transactionTypeGroup", "transactionId", "transactionTypeCode", "_SOURCE", "_DELETED", "_PART", "_MODIFIED", "_ID", "createdBy_ID", "modifiedBy_ID")
)
main_f.display()

# COMMAND ----------

main_myAnsellFlag = spark.sql("""
SELECT DISTINCT 
   KEY_VALUE
FROM smartsheets.edm_control_table 
WHERE TABLE_ID = 'MYANSELL_EXCLUDE_ORDER_TYPE' 
  and ACTIVE_FLG = true 
  and current_date between VALID_FROM and VALID_TO
  """)

main_myAnsellFlag.cache()
main_myAnsellFlag.display()

# COMMAND ----------

main_f = (
main_f
  .join(main_myAnsellFlag, main_myAnsellFlag.KEY_VALUE.contains(main_f.transactionTypeGroup), how='left')\
  .withColumn("myAnsellFlag", f.when(main_f.transactionTypeGroup.contains(main_myAnsellFlag.KEY_VALUE), False).otherwise(True))\
  .select("createdBy", "createdOn", "modifiedBy", "modifiedOn", "insertedOn", "updatedOn", "description","dropShipmentFlag", "e2eFlag", "myAnsellFlag", "name","sampleOrderFlag", "transactionGroup", "transactionTypeGroup", "transactionId", "transactionTypeCode", "_SOURCE", "_DELETED", "_PART", "_MODIFIED", "_ID", "createdBy_ID", "modifiedBy_ID")
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)
main_f.display()

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_it_keys = spark.sql("""
  SELECT
    'INVOICE_TYPE' transactionGroup,
    REPLACE(STRING(INT(CUST_TRX_TYPE_ID)), ",", "") || '-' || REPLACE(STRING(INT(org_id)), ",", "") transactionId
  FROM ra_cust_trx_types_all
  WHERE org_id IS NOT NULL
""")

full_ot_keys = spark.sql("""
  SELECT
    'ORDER_TYPE' transactionGroup,
    'ORDER' || '-' || REPLACE(STRING(INT(transaction_type_id)), ",", "") transactionId
  FROM oe_transaction_types_tl 
  WHERE LANGUAGE = 'US'
  
  UNION
  
  SELECT
    'ORDER_TYPE' transactionGroup,
    'RETURN' || '-' || REPLACE(STRING(INT(transaction_type_id)), ",", "") transactionId
	FROM oe_transaction_types_tl
  WHERE LANGUAGE = 'US'

""")

full_keys_f = (
  full_ot_keys
  .union(full_it_keys)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'transactionId,transactionGroup,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(ra_cust_trx_types_all)
  update_cutoff_value(cutoff_value, table_name, 'ebs.ra_cust_trx_types_all')
  update_run_datetime(run_datetime, table_name, 'ebs.ra_cust_trx_types_all')

  cutoff_value = get_incr_col_max_value(oe_transaction_types_tl)
  update_cutoff_value(cutoff_value, table_name, 'ebs.oe_transaction_types_tl')
  update_run_datetime(run_datetime, table_name, 'ebs.oe_transaction_types_tl')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
