# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.bill_to

# COMMAND ----------

# LOAD DATASETS
salesorders = load_full_dataset('sap.salesorders')
salesorders.createOrReplaceTempView('salesorders')

# COMMAND ----------

# SAMPLING
if sampling:
  salesorders = salesorders.limit(10)
  salesorders.createOrReplaceTempView('salesorders')

# COMMAND ----------

main = spark.sql("""
  SELECT 
  null AS createdBy,
  null AS createdOn,
  null AS modifiedBy,
  null AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  SoldToParty AS accountId,
  SoldToParty AS accountNumber,
  SoldToParty AS billToAddressId,
  null AS billToCity,
  null AS billToCountry,
  null AS billToLine1,
  null AS billToLine2,
  null AS billToLine3,
  SoldToPartyPostalCode AS billToPostalCode,
  max(SoldToPartyREGION) billToRegion,
  min(SoldToPartyAREA) billToSubRegion,
  null AS billToState,
  Customer AS name
FROM
  salesorders  
GROUP BY
  SoldToParty,
  SoldToPartyPostalCode,
  SoldToPartyPostalCode,
  Customer
""")

display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['billToAddressId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_bill_to())
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns
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
  spark.table('sap.salesorders')
  .filter('_DELETED IS FALSE')
  .selectExpr("SoldToParty AS billToAddressId")
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'billToAddressId,_SOURCE'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(salesorders)
  update_cutoff_value(cutoff_value, table_name, 'sap.salesorders')
  update_run_datetime(run_datetime, table_name, 'sap.salesorders')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
