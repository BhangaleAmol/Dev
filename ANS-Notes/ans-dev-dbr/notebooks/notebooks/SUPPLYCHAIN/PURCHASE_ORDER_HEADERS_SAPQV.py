# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.purchase_order_headers

# COMMAND ----------

# LOAD DATASETS
vw_qv_purchaseorder = load_full_dataset('sap.purchaseorders')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_qv_purchaseorder = vw_qv_purchaseorder.limit(10)

# COMMAND ----------

# VIEWS
vw_qv_purchaseorder.createOrReplaceTempView('vw_qv_purchaseorder')

# COMMAND ----------

main = spark.sql("""
  SELECT
       NULL AS createdBy
      ,TO_TIMESTAMP(FileCreatedDate,'dd/MM/yyyy hh:mm:ss') AS createdOn
      ,NULL AS modifiedBy
      ,TO_TIMESTAMP(FileCreatedDate,'dd/MM/yyyy hh:mm:ss') AS modifiedOn
      ,NULL AS insertedOn
      ,NULL AS updatedOn
      ,NULL AS approvalDate
      ,NULL AS approvalFlag
      ,NULL AS authorizationStatus
      ,NULL AS baseCurrencyId
      ,NULL AS buyerId
      ,NULL AS cancelledFlag
      ,NULL AS closedDate
      ,NULL AS currency
      ,NULL AS endDate
      ,NULL AS exchangeRate
      ,NULL AS exchangeRateDate
      ,NULL AS exchangeRateUsd
      ,NULL AS fobLookupCode
      ,NULL AS freightTermsLookupCode
      ,NULL AS inTransitTime
      ,NULL AS orderNumber
      ,POStatus AS orderStatus
      ,EntityCode AS owningBusinessUnitId
      ,PONumber AS purchaseOrderId
      ,NULL AS revisionDate
      ,NULL AS revisionNumber
      ,NULL AS shipViaLookupCode
      ,NULL AS startDate
      ,VendorId AS supplierId
      ,NULL AS supplierSiteId
      ,NULL AS typeLookupCode
  FROM vw_qv_purchaseorder
""")
main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['purchaseOrderId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(convert_null_to_unknown('buyerId'))
  .transform(tg_default(source_name))
  .transform(tg_supplychain_purchaseorder())
  .transform(attach_surrogate_key(columns = 'purchaseOrderId,_SOURCE'))
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
# full_keys_f = (
#   spark.sql("""
#     SELECT
#       PONumber AS purchaseOrderId
#     FROM sap.purchaseorders
#   """)
#   .transform(attach_source_column(source = source_name))
#   .transform(attach_surrogate_key(columns = 'purchaseOrderId,_SOURCE'))
#   .select('_ID')
# )

# apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')
# apply_soft_delete(full_keys_f, table_name_agg, key_columns = '_ID', source_name = source_name)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(vw_qv_purchaseorder)
  update_cutoff_value(cutoff_value, table_name, 'sap.purchaseorders')
  update_run_datetime(run_datetime, table_name, 'sap.purchaseorders')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO s_supplychain.purchase_order_headers_sapqv AS Target
# MAGIC USING sap.purchaseorders AS Source
# MAGIC  ON Target.purchaseOrderId = Source.PONumber
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET Target.orderStatus= 
# MAGIC       CASE 
# MAGIC         WHEN Target.purchaseOrderId = Source.PONumber
# MAGIC         THEN 'Open'
# MAGIC         ELSE 'Closed'
# MAGIC       END 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO s_supplychain.purchase_order_headers_agg AS Target
# MAGIC USING sap.purchaseorders AS Source
# MAGIC  ON Target.purchaseOrderId = Source.PONumber AND Target._SOURCE='SAPQV'
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET Target.orderStatus= 
# MAGIC       CASE 
# MAGIC         WHEN Target.purchaseOrderId = Source.PONumber
# MAGIC         THEN 'Open'
# MAGIC         ELSE 'Closed'
# MAGIC       END 

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
