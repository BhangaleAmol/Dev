# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.supplier_account

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.ap_suppliers', prune_days)
  main_inc = load_incr_dataset('ebs.ap_suppliers', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.ap_suppliers')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('ap_suppliers')

# COMMAND ----------

main = spark.sql("""
SELECT
     REPLACE(STRING(INT (AP_SUPPLIERS.CREATED_BY)),
    ",",
    ""
     ) AS createdBy,
    AP_SUPPLIERS.CREATION_DATE AS createdOn,
     REPLACE(STRING(INT (AP_SUPPLIERS.LAST_UPDATED_BY )),
    ",",
    ""
    ) AS modifiedBy,
    AP_SUPPLIERS.LAST_UPDATE_DATE AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    lm.GlobalLocationCode AS globalLocationCode,
    AP_SUPPLIERS.ORGANIZATION_TYPE_LOOKUP_CODE AS supplierGroup,
    CAST(AP_SUPPLIERS.VENDOR_ID AS INT) AS supplierId,
    AP_SUPPLIERS.VENDOR_NAME AS supplierName,
    AP_SUPPLIERS.SEGMENT1 AS supplierNumber
  FROM AP_SUPPLIERS
  LEFT JOIN spt.locationmaster lm 
    ON  CAST(AP_SUPPLIERS.SEGMENT1 AS INT)=lm.EBSSUPPLIERCODE 
        AND lm.MainOrigin = 'Y' 
        AND lm.StatusValue = 'Active'
        AND lm._DELETED = False
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

# HANDLE DELETE
full_keys_f = (
  spark.table('ebs.ap_suppliers')
  .selectExpr("CAST(AP_SUPPLIERS.VENDOR_ID AS INT) AS supplierId")
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'supplierId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(main_inc, 'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'ebs.ap_suppliers')
  update_run_datetime(run_datetime, table_name, 'ebs.ap_suppliers')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
