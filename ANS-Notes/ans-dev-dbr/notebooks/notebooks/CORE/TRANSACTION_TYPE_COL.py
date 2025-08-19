# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.transaction_type

# COMMAND ----------

# LOAD DATASETS
vw_qv_invoices = load_full_dataset('col.vw_qv_invoices')
vw_qv_orders = load_full_dataset('col.vw_qv_orders')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_qv_invoices = vw_qv_invoices.limit(10)
  vw_qv_orders = vw_qv_orders.limit(10)

# COMMAND ----------

# VIEWS
# VIEWS
vw_qv_invoices.createOrReplaceTempView('vw_qv_invoices')
vw_qv_orders.createOrReplaceTempView('vw_qv_orders')

# COMMAND ----------

main_invoice = spark.sql("""
select distinct
  null createdBy,
  null createdOn,
  null modifiedBy,
  null modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  case when i.Doc_Type = '3' then 'Invoice' else  i.Doc_Type end description,
  'WH' dropShipmentFlag,
  True as e2eFlag,
  True as myAnsellFlag,
  case when i.Doc_Type = '3' then 'Invoice' else  i.Doc_Type end name,
  False as sampleOrderFlag,
  'INVOICE_TYPE' transactionGroup,
  null as transactionTypeGroup,
  i.Doc_Type || '-' || i.Company  transactionId,
  case 
    when i.Doc_Type = '3' then 'INV' 
    when i.Doc_Type = 'Returns' then 'Return' 
    when i.Doc_Type = 'Credit Memo' then 'CM' 
   end transactionTypeCode
from
  vw_qv_invoices i
""")

main_invoice.count()
main_invoice.cache()
main_invoice.display()

# COMMAND ----------

main_order = spark.sql("""
SELECT distinct 
  cast(NULL as string) createdBy,
  cast(NULL AS timestamp) createdOn,
  cast(NULL as string) modifiedBy,
  cast(NULL AS timestamp) modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  'Standard Order' description,
  'WH' dropShipmentFlag,
  true e2eFlag,
  True as myAnsellFlag,
  'Standard Order' name,
  False as sampleOrderFlag,
  'ORDER_TYPE' transactionGroup,
  'Standard'  transactionTypeGroup,
  'Standard' transactionId,
  'ORDER' transactionTypeCode
FROM
  vw_qv_orders
  
union 

SELECT distinct 
  cast(NULL as string) createdBy,
  cast(NULL AS timestamp) createdOn,
  cast(NULL as string) modifiedBy,
  cast(NULL AS timestamp) modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  'Return' description,
  'WH' dropShipmentFlag,
  False e2eFlag,
  False as myAnsellFlag,
  'Return' name,
  False as sampleOrderFlag,
  'ORDER_TYPE' transactionGroup,
  'Return'  transactionTypeGroup,
  'Return' transactionId,
  'ORDER' transactionTypeCode
FROM
  vw_qv_orders
  
""")

main_order.count()
main_order.cache()
display(main_order)

# COMMAND ----------

main = main_invoice.union(main_order)
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
  .transform(tg_default(source_name))
  .transform(tg_core_transaction_type())
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
full_keys_invoice_type = spark.sql("""SELECT distinct
i.Doc_Type || '-' || i.Company  transactionId,
'INVOICE_TYPE' transactionGroup
FROM col.vw_qv_invoices i
""")
  
full_keys_order_type = spark.sql("""
  select distinct
    'Standard' transactionId,
    'ORDER_TYPE' transactionGroup  
  from
    col.vw_qv_orders
  
  union
  
  Select distinct
    'Return' transactionId,
    'ORDER_TYPE' transactionGroup 
  from
    col.vw_qv_orders
""")

full_keys_f = (
#   spark.table('col.vw_qv_invoices')
#   .filter('_DELETED IS FALSE')
#   .selectExpr("Doc_Type || '-' || Company transactionId","'INVOICE_TYPE' transactionGroup")
  full_keys_order_type
  .unionAll(full_keys_invoice_type)
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
  cutoff_value = get_incr_col_max_value(vw_qv_invoices)
  update_cutoff_value(cutoff_value, table_name, 'col.vw_qv_invoices')
  update_run_datetime(run_datetime, table_name, 'col.vw_qv_invoices')
  
  cutoff_value = get_incr_col_max_value(vw_qv_orders)
  update_cutoff_value(cutoff_value, table_name, 'col.vw_qv_orders')
  update_run_datetime(run_datetime, table_name, 'col.vw_qv_orders')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
