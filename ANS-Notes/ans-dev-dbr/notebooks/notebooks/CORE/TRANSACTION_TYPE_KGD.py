# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.transaction_type

# COMMAND ----------

# LOAD DATASETS
tembo_seorder = load_full_dataset('kgd.dbo_tembo_seorder')
tembo_icsale= load_full_dataset('kgd.dbo_tembo_seorder')

# COMMAND ----------

# SAMPLING
if sampling:
  tembo_seorder = tembo_seorder.limit(10)
  tembo_icsale = tembo_icsale.limit(10)

# COMMAND ----------

# VIEWS
tembo_seorder.createOrReplaceTempView('tembo_seorder')
tembo_icsale.createOrReplaceTempView('tembo_icsale')

# COMMAND ----------

main_invoice = spark.sql("""
select distinct
  null createdBy,
  null createdOn,
  null modifiedBy,
  null modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
   'Invoice'  description,
  'WH' dropShipmentFlag,
  True as e2eFlag,
  True as myAnsellFlag,
  'Invoice' name,
  False as sampleOrderFlag,
  'INVOICE_TYPE' transactionGroup,
  null as transactionTypeGroup,
  'INV' transactionId,
  'INV' transactionTypeCode
from
  tembo_icsale i
where NOT(_DELETED)

union

select distinct
  null createdBy,
  null createdOn,
  null modifiedBy,
  null modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
   'Credit Note'  description,
  'WH' dropShipmentFlag,
  False as e2eFlag,
  False as myAnsellFlag,
  'Credit Note' name,
  False as sampleOrderFlag,
  'INVOICE_TYPE' transactionGroup,
  null as transactionTypeGroup,
  'CM' transactionId,
  'CM' transactionTypeCode
from
  tembo_icsale i
where NOT(_DELETED)
  
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
  tembo_seorder
WHERE NOT(_DELETED)
  
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
  tembo_seorder
WHERE NOT(_DELETED)
  
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

full_inv_keys = spark.sql("""
select distinct
  'INV' transactionId,'INVOICE_TYPE' transactionGroup
from
  kgd.dbo_tembo_seorder tembo_icsale 
WHERE NOT(_DELETED)
union
select distinct
  'CM' transactionId,'INVOICE_TYPE' transactionGroup
from
  kgd.dbo_tembo_seorder tembo_icsale  
WHERE NOT(_DELETED)
""")

full_order_keys = spark.sql("""
SELECT distinct 
  'Standard' transactionId,'ORDER_TYPE' transactionGroup
FROM
  kgd.dbo_tembo_seorder tembo_seorder
WHERE NOT(_DELETED)
union 
SELECT distinct 
  'Return' transactionId,'ORDER_TYPE' transactionGroup
FROM
  kgd.dbo_tembo_seorder tembo_seorder  
WHERE NOT(_DELETED)
""")

full_keys_f = (
  full_inv_keys
  .union(full_order_keys)
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
  cutoff_value = get_incr_col_max_value(tembo_seorder)
  update_cutoff_value(cutoff_value, table_name, 'kgd.dbo_tembo_seorder')
  update_run_datetime(run_datetime, table_name, 'kgd.dbo_tembo_seorder')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
