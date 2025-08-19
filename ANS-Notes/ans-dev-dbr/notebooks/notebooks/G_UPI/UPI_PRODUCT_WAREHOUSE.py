# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/header_gold_notebook

# COMMAND ----------

# database_name = "g_upi" # [str]
# incremental = False # [bool]
# metadata_container = "datalake" # [str]
# metadata_folder = "/datalake/g_upi/metadata" # [str]
# metadata_storage = "edmans{env}data003" # [str]
# overwrite = True # [bool]
# prune_days = 10 # [int]
# sampling = False # [bool]
# table_name = "UPI_PRODUCT_WAREHOUSE" # [str]
# target_container = "datalake" # [str]
# target_folder = "/datalake/g_upi/full_data" # [str]

# COMMAND ----------

# VARIABLES
table_name = get_table_name(database_name, table_name)

# COMMAND ----------

# drop_table('g_upi._map_upi_product_warehouse')
# drop_table('g_upi.upi_product_warehouse')

# COMMAND ----------

# EXTRACT
if incremental:
  cutoff_value = get_cutoff_value(table_name, 's_core.product_org_agg', prune_days)
  product_org_agg = load_incremental_dataset('s_core.product_org_agg', '_MODIFIED', cutoff_value)

  cutoff_value = get_cutoff_value(table_name, 's_core.organization_agg', prune_days)
  organization_agg = load_incremental_dataset('s_core.organization_agg', '_MODIFIED', cutoff_value)

  cutoff_value = get_cutoff_value(table_name, 's_core.product_agg', prune_days)
  product_agg = load_incremental_dataset('s_core.product_agg', '_MODIFIED', cutoff_value)

  cutoff_value = get_cutoff_value(table_name, 's_supplychain.inventory_agg', prune_days)
  inventory_agg = load_incremental_dataset('s_supplychain.inventory_agg', '_MODIFIED', cutoff_value)  
else:
  product_org_agg = load_full_dataset('s_core.product_org_agg')
  organization_agg = load_full_dataset('s_core.organization_agg')
  product_agg = load_full_dataset('s_core.product_agg')
  inventory_agg = load_full_dataset('s_supplychain.inventory_agg')

product_org_agg.createOrReplaceTempView('product_org_agg')
organization_agg.createOrReplaceTempView('organization_agg')
product_agg.createOrReplaceTempView('product_agg')
inventory_agg.createOrReplaceTempView('inventory_agg')

# COMMAND ----------

# Compute inventory
primary_qty_inventory = spark.sql("""
select
  item_ID,
  inventoryWarehouse_ID,
  ansStdUomCode,
  CAST(sum(ansStdQtyAvailableToTransact) AS BIGINT) Qty,
  inventoryDate,
  _SOURCE
from
  s_supplychain.inventory_agg
where
  inventoryDate = last_day(current_date)
  and _SOURCE in ('EBS', 'SAP')
group by
  item_ID,
  inventoryWarehouse_ID,
  ansStdUomCode,
  inventoryDate,
  _SOURCE
""")

primary_qty_inventory.createOrReplaceTempView('primary_qty_inventory')

# COMMAND ----------

main_1 = spark.sql("""
select
  pa.productCode Internal_Item_Id,
  po.shelfLife Shelf_life,
  CASE
    WHEN inv.ansStdUomCode = "PAA" THEN "PAIR"
    WHEN inv.ansStdUomCode = "ST" THEN "PIECE"
    WHEN inv.ansStdUomCode = "PC" THEN "PIECE"
    WHEN inv.ansStdUomCode = "PR" THEN "PAIR"
    WHEN inv.ansStdUomCode = "GR" THEN "GROSS"
    ELSE inv.ansStdUomCode
  END Primary_UOM,
  pa.secondarySellingUom Secondary_UOM,
  pa.lowestShippableUom Lowest_Shippable_UOM,
  po.commodityCode Commodity_HTS_Code,
  org.region Region,
  po._SOURCE System,
  org.organizationCode Warehouse_Id,
  org.name Description,
  inv.Qty,
  inv.inventoryDate Inventory_Date,
  po.productStatus Status,
  po._deleted Is_Active,
  po._SOURCE
from
  product_org_agg po
  left join s_core.organization_agg org on po.organization_ID = org._ID
  left join primary_qty_inventory inv on po.organization_ID = inv.inventoryWarehouse_ID
      and po.item_ID = inv.item_ID
 right join s_core.product_agg pa on po.item_ID = pa._ID
where
  po._SOURCE in ('EBS', 'SAP')
  and inv.inventoryDate = last_day(current_date)
""")

# main_1.display()

# COMMAND ----------

main_2 = (
  main_1
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  #.transform(attach_source_column('TAA'))
)

# COMMAND ----------

# MAP TABLE
nk_columns = ['Internal_Item_ID', 'Warehouse_Id', '_SOURCE']

map_table = get_map_table_name(table_name)
if not table_exists(map_table):
  map_table_path = get_table_path(map_table, target_container, target_storage)
  create_map_table(map_table, map_table_path)
    
update_map_table(map_table, main_2, nk_columns)

# COMMAND ----------

main_f = (
  main_2
  .transform(attach_primary_key(nk_columns, map_table, '_ID'))
  .transform(sort_columns)
)

main_f.createOrReplaceTempView('main_f')
# main_f.display()

# COMMAND ----------

# LOAD
options = {
  'overwrite': overwrite, 
  'storage_name': target_storage, 
  'container_name': target_container
}
register_hive_table(main_f, table_name, target_folder, options)
merge_into_table(main_f, table_name, ['_ID'])

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(product_org_agg, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.product_org_agg')
  update_run_datetime(run_datetime, table_name, 's_core.product_org_agg')

  cutoff_value = get_max_value(product_org_agg, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.organization_agg')
  update_run_datetime(run_datetime, table_name, 's_core.organization_agg')

  cutoff_value = get_max_value(product_org_agg, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.product_agg')
  update_run_datetime(run_datetime, table_name, 's_core.product_agg')

  cutoff_value = get_max_value(product_org_agg, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.inventory_agg')
  update_run_datetime(run_datetime, table_name, 's_supplychain.inventory_agg')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/footer_gold_notebook
