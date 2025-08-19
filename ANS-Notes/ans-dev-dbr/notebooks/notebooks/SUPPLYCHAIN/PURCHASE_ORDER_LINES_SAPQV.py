# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.purchase_order_lines

# COMMAND ----------

# LOAD DATASETS
vw_qv_purchaseorderlines = load_full_dataset('sap.purchaseorderlines')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_qv_purchaseorderlines = vw_qv_purchaseorderlines.limit(10)

# COMMAND ----------

# VIEWS
vw_qv_purchaseorderlines.createOrReplaceTempView('vw_qv_purchaseorderlines')

# COMMAND ----------

main = spark.sql("""
  SELECT
       NULL AS createdBy
      ,TO_TIMESTAMP(FileCreatedDate,'dd/MM/yyyy hh:mm:ss') AS createdOn
      ,NULL AS modifiedBy
      ,TO_TIMESTAMP(FileCreatedDate,'dd/MM/yyyy hh:mm:ss') AS modifiedOn
      ,CAST(NULL AS TIMESTAMP) AS insertedOn
      ,CAST(NULL AS TIMESTAMP) AS updatedOn
      ,'N' AS cancelledFlag
      ,CAST(NULL AS TIMESTAMP) CETD
      ,NULL AS closedDate
      ,NULL AS inTransitTime
      ,plant AS inventoryWarehouseID
      ,MaterialNumber AS itemId
      ,POLine AS lineNumber
      ,CAST(NULL AS TIMESTAMP) needByDate
      ,POLineStatus AS orderLineStatus
      ,POLineUoM AS orderUomCode
      ,NULL AS pricePerUnit
      ,CONCAT(PONumber,'-',POLine) AS purchaseOrderDetailId
      ,PONumber AS purchaseOrderId
      ,CAST(PODeliveredQty AS INT) AS quantityDelivered
      ,CAST(POLineQty AS INT) AS quantityOrdered
      ,CAST(POShippedQty AS INT) AS quantityShipped
      ,CAST(NULL AS TIMESTAMP) RCETD
      ,CAST(NULL AS TIMESTAMP) RETD
      ,POShippedDate AS shippedDate
  FROM vw_qv_purchaseorderlines
""")
main.cache()

# display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['purchaseOrderDetailId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_supplychain_purchaseorderlines())
  .transform(attach_surrogate_key(columns = 'purchaseOrderDetailId,_SOURCE'))
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.createOrReplaceTempView('main_f')
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
#     PONumber AS purchaseOrderDetailId
#    FROM sap.purchaseorderlines
#    """)
#   .transform(attach_source_column(source = source_name))
#   .transform(attach_surrogate_key(columns = 'purchaseOrderDetailId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(vw_qv_purchaseorderlines)
  update_cutoff_value(cutoff_value, table_name, 'sap.purchaseorderlines')
  update_run_datetime(run_datetime, table_name, 'sap.purchaseorderlines')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO s_supplychain.purchase_order_lines_sapqv AS Target
# MAGIC USING sap.purchaseorderlines AS Source
# MAGIC  ON Target.purchaseOrderDetailId = CONCAT(Source.PONumber,'-',Source.POLine)
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET Target.orderLineStatus= 
# MAGIC       CASE 
# MAGIC         WHEN Target.purchaseOrderDetailId = CONCAT(Source.PONumber,'-',Source.POLine)
# MAGIC         THEN 'Open'
# MAGIC         ELSE 'Closed'
# MAGIC       END 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO s_supplychain.purchase_order_lines_agg AS Target
# MAGIC USING sap.purchaseorderlines AS Source
# MAGIC  ON Target.purchaseOrderDetailId = CONCAT(Source.PONumber,'-',Source.POLine) AND Target._SOURCE='SAPQV'
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET Target.orderLineStatus= 
# MAGIC       CASE 
# MAGIC         WHEN Target.purchaseOrderDetailId = CONCAT(Source.PONumber,'-',Source.POLine)
# MAGIC         THEN 'Open'
# MAGIC         ELSE 'Closed'
# MAGIC       END 

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
