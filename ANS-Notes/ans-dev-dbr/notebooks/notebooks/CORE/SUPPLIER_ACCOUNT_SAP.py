# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.supplier_account

# COMMAND ----------

# LOAD DATASETS
# if incremental:
#   cutoff_value = get_cutoff_value(table_name, 'sap.openorders', prune_days)
#   main_inc = load_incr_dataset('sap.openorders', 'LAST_UPDATE_DATE', cutoff_value)
# else:
main_inc = load_full_dataset('sap.openorders')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('vw_openorders')

# COMMAND ----------

main = spark.sql("""
SELECT DISTINCT 
    NULL AS createdBy,
    NULL AS createdOn,
    NULL AS modifiedBy,
    NULL AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    lm.GlobalLocationCode AS globalLocationCode,
    NULL AS supplierGroup,
    CAST(vw_openorders.vendor AS INT) AS supplierId,
    vw_openorders.vendorName AS supplierName,
    CAST(vw_openorders.vendor AS INT) AS supplierNumber
  FROM vw_openorders
  LEFT JOIN spt.locationmaster AS lm 
    ON  CAST(vw_openorders.vendor AS INT) = CAST(lm.SAPGOODSSUPPLIERCODE AS INT) 
        AND lm.MainOrigin = 'Y' 
        AND lm.StatusValue = 'Active'
        AND lm._DELETED = False
  WHERE vw_openorders.OriginalPONumber IS NULL
  AND CAST(vw_openorders.vendor AS INT) IS NOT NULL
 """)

main.createOrReplaceTempView('main')
main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['supplierId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_supplier_account())
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

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# HANDLE DELETE
# full_keys_f = (
#   spark.table('sap.openorders')
#   .selectExpr("Vendor AS supplierId")
#   .transform(attach_source_column(source = source_name))
#   .transform(attach_surrogate_key(columns = 'supplierId,_SOURCE'))
#   .select('_ID')
# )

# apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')
# apply_soft_delete(full_keys_f, table_name_agg, key_columns = '_ID', source_name = source_name)

# COMMAND ----------

# # UPDATE CUTOFF VALUE
# if not test_run:
#   cutoff_value = get_incr_col_max_value(main_inc, 'LAST_UPDATE_DATE')
#   update_cutoff_value(cutoff_value, table_name, 'sap.openorders')
#   update_run_datetime(run_datetime, table_name, 'sap.openorders')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
