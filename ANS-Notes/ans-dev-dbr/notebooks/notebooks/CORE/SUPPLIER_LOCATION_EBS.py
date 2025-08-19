# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.supplier_location

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.ap_supplier_sites_all', prune_days)
  main_inc = load_incr_dataset('ebs.ap_supplier_sites_all', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.ap_supplier_sites_all')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('ap_supplier_sites_all')

# COMMAND ----------

main = spark.sql("""
SELECT
     REPLACE(STRING(INT (AP_SUPPLIER_SITES_ALL.CREATED_BY)),
    ",",
    ""
     )                                                                    AS createdBy,
     AP_SUPPLIER_SITES_ALL.CREATION_DATE                                  AS createdOn,
     REPLACE(STRING(INT (AP_SUPPLIER_SITES_ALL.LAST_UPDATED_BY )),
    ",",
    ""
    )                                                                     AS modifiedBy,
    AP_SUPPLIER_SITES_ALL.LAST_UPDATE_DATE                                AS modifiedOn,
    CURRENT_TIMESTAMP()                                                   AS insertedOn,
    CURRENT_TIMESTAMP()                                                   AS updatedOn,
    AP_SUPPLIER_SITES_ALL.CITY                                            AS city,
    AP_SUPPLIER_SITES_ALL.COUNTRY                                         AS country,
    AP_SUPPLIER_SITES_ALL.ZIP                                             AS PostalCode,
    AP_SUPPLIER_SITES_ALL.AREA_CODE                                       AS region,
    AP_SUPPLIER_SITES_ALL.state                                           AS state,
    CAST(AP_SUPPLIER_SITES_ALL.ACCTS_PAY_CODE_COMBINATION_ID AS Int)      AS supplierGlAccount,
    AP_SUPPLIERS.ORGANIZATION_TYPE_LOOKUP_CODE                            AS supplierGroup,
    CAST(AP_SUPPLIERS.VENDOR_ID AS Int)                                   AS supplierId,
    AP_SUPPLIERS.VENDOR_NAME                                              AS supplierName,
    AP_SUPPLIERS.SEGMENT1                                                 AS supplierNumber,
    AP_SUPPLIER_SITES_ALL.PAY_GROUP_LOOKUP_CODE                           AS supplierPaymentGroup,
    AP_TERMS_TL.NAME                                                      AS supplierPaymentTerms,
    AP_SUPPLIER_SITES_ALL.VENDOR_SITE_CODE                                AS supplierSiteCode,
    CAST(AP_SUPPLIER_SITES_ALL.VENDOR_SITE_ID AS Int)                     AS supplierSiteId
    
  FROM EBS.AP_SUPPLIER_SITES_ALL 
  Left join EBS.AP_SUPPLIERS on AP_SUPPLIER_SITES_ALL.Vendor_ID = AP_SUPPLIERS.Vendor_ID
  Left join EBS.AP_TERMS_TL on AP_SUPPLIER_SITES_ALL.terms_id = double(AP_TERMS_TL.term_id)
  Where AP_TERMS_TL.Language='US'
 """)

main.createOrReplaceTempView('main')
main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['supplierSiteId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_supplier_location())
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
  spark.table('ebs.ap_supplier_sites_all')
  .selectExpr("CAST(VENDOR_SITE_ID AS INT) AS supplierSiteId")  
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'supplierSiteId,_SOURCE'))
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
  update_cutoff_value(cutoff_value, table_name, 'ebs.ap_supplier_sites_all')
  update_run_datetime(run_datetime, table_name, 'ebs.ap_supplier_sites_all')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
