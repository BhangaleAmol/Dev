# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.transaction_type

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'prms.prmsf200_obirp111', prune_days)
  prmsf200_obirp111 = load_incr_dataset('prms.prmsf200_obirp111', 'TADCD', cutoff_value)
  
  cutoff_value = get_cutoff_value(table_name, 'prms.prmsf200_obcdp100', prune_days)
  prmsf200_obcdp100 = load_incr_dataset('prms.prmsf200_obcdp100', 'CMRDT', cutoff_value)
  
else:
  prmsf200_obirp111 = load_full_dataset('prms.prmsf200_obirp111')
  prmsf200_obcdp100 = load_full_dataset('prms.prmsf200_obcdp100')

# COMMAND ----------

# SAMPLING
if sampling:
  prmsf200_obirp111 = prmsf200_obirp111.limit(10)
  prmsf200_obcdp100 = prmsf200_obcdp100.limit(10)

# COMMAND ----------

# VIEWS
prmsf200_obirp111.createOrReplaceTempView('prmsf200_obirp111')
prmsf200_obcdp100.createOrReplaceTempView('prmsf200_obcdp100')

# COMMAND ----------

main_invoices = spark.sql("""
SELECT DISTINCT
    cast(NULL as string) AS createdBy,
    cast(NULL AS timestamp) AS createdOn,
    cast(NULL AS STRING) AS modifiedBy,
    cast(NULL AS timestamp) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    'Invoice' AS  description,
    'WH' dropShipmentFlag,
    True as e2eFlag,
    True as myAnsellFlag,
    'Invoice' AS name,
    False as sampleOrderFlag,
    'INVOICE_TYPE' AS transactionGroup,
    null as transactionTypeGroup,
    'INV' AS  transactionId,
    'INV' AS transactionTypeCode
FROM
   prmsf200_obirp111 obirp111

""")
main_invoices.count()
main_invoices.cache()
display(main_invoices)

# COMMAND ----------

main_credit_notes = spark.sql("""
SELECT DISTINCT
    cast(NULL as string) AS createdBy,
    cast(NULL AS timestamp) AS createdOn,
    cast(NULL AS STRING) AS modifiedBy,
    cast(NULL AS timestamp) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    'CM' AS  description,
    'WH' dropShipmentFlag,
    True as e2eFlag,
    True as myAnsellFlag,
    'CM' AS name,
    False as sampleOrderFlag,
    'INVOICE_TYPE' AS transactionGroup,
    null as transactionTypeGroup,
    'CM' AS  transactionId,
    'CM' AS transactionTypeCode
FROM
    prmsf200_obcdp100 obcdp100

""")
main_credit_notes.count()
main_credit_notes.cache()
display(main_credit_notes)

# COMMAND ----------

main_orders = spark.sql("""
SELECT DISTINCT
    cast(NULL as string) AS createdBy,
    cast(NULL AS timestamp) AS createdOn,
    cast(NULL AS STRING) AS modifiedBy,
    cast(NULL AS timestamp) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    'ORDER' AS  description,
    'WH' dropShipmentFlag,
    True as e2eFlag,
    True as myAnsellFlag,
    'ORDER' AS name,
    False as sampleOrderFlag,
    'ORDER_TYPE' AS transactionGroup,
    null as transactionTypeGroup,
    OBCOP100.C1OTP AS  transactionId,
    'ORDER' AS transactionTypeCode
FROM
     prms.prmsf200_obcop100 obcop100 

""")
main_orders.count()
main_orders.cache()
display(main_orders)

# COMMAND ----------

main= main_invoices.union(main_credit_notes)
main = main.union(main_orders)
display(main)

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'transactionId,transactionGroup')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_transaction_type())
  .transform(apply_schema(schema))
)

main_f.cache()
main_f.display()

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
full_obirp111_keys = (
  spark.table('prms.prmsf200_obirp111')
  .filter('_DELETED IS FALSE')
  .selectExpr("'INV' AS transactionId","'INVOICE_TYPE' AS transactionGroup")
)

full_obcdp100_keys = (
  spark.table('prms.prmsf200_obcdp100')
  .filter('_DELETED IS FALSE')
  .selectExpr("'CM' AS transactionId","'INVOICE_TYPE' AS transactionGroup")
)

full_obcop100_keys = (
  spark.table('prms.prmsf200_obcop100')
  .filter('_DELETED IS FALSE')
  .selectExpr("C1OTP AS transactionId","'ORDER_TYPE' AS transactionGroup")
)

full_keys_f = (
  full_obirp111_keys
  .union(full_obcdp100_keys)
  .union(full_obcop100_keys)
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
  
  cutoff_value = get_incr_col_max_value(prmsf200_obirp111)
  update_cutoff_value(cutoff_value, table_name, 'prms.prmsf200_obirp111')
  update_run_datetime(run_datetime, table_name, 'prms.prmsf200_obirp111')

  cutoff_value = get_incr_col_max_value(prmsf200_obcdp100)
  update_cutoff_value(cutoff_value, table_name, 'prms.prmsf200_obcdp100')
  update_run_datetime(run_datetime, table_name, 'prms.prmsf200_obcdp100')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
