# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.tmb_intransit

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'PONumber,itemNumber')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'tmb_intransit')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/fin_qv/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT 
source_table = 's_supplychain.purchase_order_headers_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  purchase_order_headers_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  purchase_order_headers_agg = load_full_dataset(source_table)
  
purchase_order_headers_agg.createOrReplaceTempView('purchase_order_headers_agg')

# COMMAND ----------

# EXTRACT 
source_table = 's_supplychain.purchase_order_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  purchase_order_lines_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  purchase_order_lines_agg = load_full_dataset(source_table)
  
purchase_order_lines_agg.createOrReplaceTempView('purchase_order_lines_agg')

# COMMAND ----------

main = spark.sql("""
select
  pr.productCode itemNumber,
  pr.name ItemDescription,
  org.commonOrganizationCode inventoryOrg,
  poh.orderNumber PONumber,
  poh.orderNumber GTCPONumber,
  sup.supplierNumber POVendorNumber,
  sup.supplierName POVendorName,
  '' orderType,
  pr.ansStdUom UOM,
  pol.needByDate DueDate,
  pol.RETD,
  pol.CETD,
  pol.RCETD,
  pol.quantityShipped InTransitQty
from
  s_supplychain.purchase_order_headers_agg poh
  join s_supplychain.purchase_order_lines_agg pol on poh._id = pol.purchaseOrder_ID
  join s_core.product_agg pr on pol.item_ID = pr._ID
  join s_core.organization_agg org on pol.inventoryWarehouse_ID = org._ID
  left join s_core.supplier_account_agg sup on poh.supplier_ID = sup._ID
where
  not poh._deleted
  and not pol._deleted
  and poh._source in ('COL', 'KGD','TOT')
  and pol.quantityShipped  > 0
  """)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("DueDate"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# DUPLICATES NOTIFICATION
if key_columns is not None:
  duplicates = get_duplicate_rows(main_f, key_columns)
  duplicates.cache()

  if not duplicates.rdd.isEmpty():
    notebook_data = {
      'notebook_name': NOTEBOOK_NAME,
      'notebook_path': NOTEBOOK_PATH,
      'target_name': table_name,
      'duplicates_count': duplicates.count(),
      'duplicates_sample': duplicates.select(key_columns).limit(50)
    }
    send_mail_duplicate_records_found(notebook_data)
  
  duplicates.unpersist()

# COMMAND ----------

# DROP DUPLICATES  
if key_columns is not None:
  key_columns_list = key_columns.replace(' ', '').split(',')
  main_f = main_f.dropDuplicates(key_columns_list)

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
