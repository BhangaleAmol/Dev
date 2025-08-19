# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_inventory

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'inventoryDate,productCode,owningBusinessUnitId')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_inventory')
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
source_table = 's_supplychain.inventory_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  inventory_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  inventory_agg = load_full_dataset(source_table)
  
inventory_agg.createOrReplaceTempView('inventory_agg')

# COMMAND ----------

main = spark.sql("""
select
  inv.inventoryDate,
  inv.productCode,
  inv.owningBusinessUnitId,
  case
    when inv.subInventoryCode = 'AVAIL' then inv.primaryQty
    else 0   end AvailableStock,
  inv.primaryQty totalStock,
  inv.primaryUomCode,
  case
    when inv.subInventoryCode = 'AVAIL' then inv.primaryQty
    else 0   end * pr.ansStduomConv AvailableStock_Ansstduom,
    inv.primaryQty * pr.ansStduomConv totalStock_Ansstduom,
    pr.ansStdUom ansStdUom
    
from
  s_supplychain.inventory_agg inv
  join s_core.product_agg pr on inv.item_ID = pr._id
  join s_core.organization_agg wh on inv.inventoryWarehouse_ID = wh._id
where
  inv._source in ('COL', 'TOT', 'KGD')
  and not inv._deleted and inv.inventoryDate = LAST_DAY(CURRENT_DATE () - INTERVAL 1 DAYS)
  
  """)
main.createOrReplaceTempView("main")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("inventoryDate"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
