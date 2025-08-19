# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.tmb_inventory

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'itemNumber,InventoryOrg,SubInventoryCode,LotExpiryDate')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'tmb_inventory')
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
    prod.e2eProductCode itemNumber,
    prod.name itemDescription,
    nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode)  InventoryOrg,
    inventory.subInventoryCode SubInventoryCode,
    prod.ansStdUom,
    nvl(inventory.lotExpirationDate, add_months(current_date, 18)) LotExpiryDate,
    sum(inventory.ansStdQty) OnHandQty,
    '' StorageLocationCode,
    '' StorageLocationDescription
from
  s_supplychain.inventory_agg inventory
  INNER JOIN s_core.organization_agg
    ON inventory.inventoryWarehouse_ID = organization_agg._id
  INNER JOIN s_core.organization_agg comp
    ON inventory.owningBusinessUnit_ID = comp._id
  INNER JOIN s_core.product_agg prod
    ON inventory.item_id = prod._id
where
  nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode)  not in ('DCHER')
 -- and year(inventory.inventoryDate) * 100 + month(inventory.inventoryDate) = (select max(year(inventory.inventoryDate) * 100 + month(inventory.inventoryDate)) from  s_supplychain.inventory_agg)
  and inventory.inventoryDate = (select max(inventoryDate) from  s_supplychain.inventory_agg)
  and not inventory._deleted
  and inventory.ansStdQty > 0
  and inventory._source in ('COL', 'KGD', 'TOT')
  and nvl(upper(prod.itemType), 'FINISHED GOODS') in ('FINISHED GOODS', 'FINISHED GOOD', 'FERT', 'ZPRF', 'ACCESSORIES')
--   and inventory.subInventoryCode not in (select subInventoryCode from TEMBO_HOLD_SUBINVENTORY_CODES)
--   and inventory.subInventoryCode not in (select subInventoryCode from TEMBO_INTRANSIT_SUBINVENTORY_CODES)
group by
  prod.e2eProductCode,
  nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode),
  nvl(inventory.lotExpirationDate, add_months(current_date, 18)) ,
  prod.name,
  inventory.subInventoryCode,
  prod.ansStdUom
  """)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("LotExpiryDate"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# VALIDATE DATA
# if incremental:
check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
