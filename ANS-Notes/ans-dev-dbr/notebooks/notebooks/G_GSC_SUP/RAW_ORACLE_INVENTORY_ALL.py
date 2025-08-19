# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_gsc_sup.raw_oracle_inventory_all

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_gsc_sup')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'ITEM_NUMBER,ORGANIZATION_CODE,TRANSDATE,SUBINVENTORY_CODE,LOT_NUMBER')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'raw_oracle_inventory_all')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/g_gsc_sup/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)
target_table

# COMMAND ----------

# EXTRACT 
source_table = 's_supplychain.inventory_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  inventory = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  inventory = load_full_dataset(source_table)
  
inventory.createOrReplaceTempView('inventory')
# inventory.display()

# COMMAND ----------

main = spark.sql("""
Select
    Product.productCode AS ITEM_NUMBER
    , Product.itemId AS INVENTORY_ITEM_ID
    , Organization.organizationCode AS ORGANIZATION_CODE
    , Inventory.primaryQty AS PRIMARY_QUANTITY
    , Inventory.primaryUOMCode AS PRIMARY_UOM_CODE
    , Inventory.secondaryQty AS SECONDARY_QUANTITY
    , Inventory.secondaryUOMCode AS SECONDARY_UOM_CODE
    , Product.name AS ITEM_DESCRIPTION
    , current_date - 1 AS TRANSDATE
    , Organization.name AS ORGANIZATION_NAME
    , Inventory.subInventoryCode AS SUBINVENTORY_CODE
    , Inventory.inventoryWarehouseID AS ORGANIZATION_ID
    , Inventory.dualUOMControl AS DUAL_UOM_CONTROL
    , Product.glProductDivision AS DIVISION
    , Product.itemType AS ITEM_TYPE
    , Inventory.glPeriodCode AS PERIOD_CODE
    , Inventory.lotNumber AS LOT_NUMBER
    , Inventory.totalCost AS STND_CST
    , (Inventory.totalCost) * Inventory.primaryQty AS EXTENDED_CST
    , Inventory.sgaCostPerUnitPrimary AS SGA_CST
    , Inventory.sgaCostPerUnitPrimary * Inventory.primaryQty AS EX_SGA_CST
    , Inventory.lotExpirationDate AS EXP_DATE
    , Inventory.lotControlFlag AS LOT_CONTROL_FLAG
    , Inventory.originId AS ORIGIN_CODE
    , Inventory.dateReceived AS DATE_RECEIVED
    , origin.originName AS ORIGIN_NAME
    , '' AS PO_NUMBER
    , Inventory.shelfLifeDays AS SHELF_LIFE_DAYS
    , Inventory.ansStdUomCode AS ANS_STD_UOM
    , Inventory.ansStdQty AS ANS_STD_QTY
    , case
          when Inventory.lotExpirationDate is not null
              and Inventory.lotExpirationDate <= add_months(current_date, 6)
          then 'Hold'
       end AS STATUS
    , Inventory.lotExpirationDate - Inventory.shelfLifeDays AS MFG_DATE
    , Product.marketingCode AS MRPN
    , Inventory._Source AS _Source
    , CURRENT_TIMESTAMP AS CreatedDate
    , Inventory.exchangeRate Exchange_Rate
    , Inventory.currency Currency
    , Inventory.primaryQty * (Product.primaryuomconv/nvl(Product.piecesInCase,Product.piecesInCarton)) AS QTY_IN_CASES
from
  s_supplychain.inventory_agg Inventory
join s_core.product_agg Product
  on Inventory.item_id = Product._id
left join s_core.organization_agg Organization
  on Inventory.inventoryWarehouse_ID = Organization._ID
left join s_core.origin_agg origin  
  on Inventory.origin_ID = origin._ID
Where 1=1
  and not inventory._DELETED
  and Organization.organizationCode not in ('532','600','601','602','605')
  and inventory.subinventorycode <>'IN TRANSIT'
  and inventory.inventoryDate = (select max(inventoryDate) from  s_supplychain.inventory_agg Inventory where not _deleted)
  and nvl(Inventory.primaryQty,0) <> 0""")


# main.display()
main.createOrReplaceTempView("main")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("TRANSDATE"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)
# main_f.display()

# COMMAND ----------

# VALIDATE DATA
#if incremental:
 # check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
