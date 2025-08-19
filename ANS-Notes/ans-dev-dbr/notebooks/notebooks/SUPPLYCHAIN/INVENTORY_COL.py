# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.inventory

# COMMAND ----------

# LOAD DATASETS
vw_qv_inventory = load_full_dataset('col.vw_qv_inventory')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_qv_inventory = vw_qv_inventory.limit(10)

# COMMAND ----------

# VIEWS
vw_qv_inventory.createOrReplaceTempView('vw_qv_inventory')

# COMMAND ----------

main = spark.sql("""
  SELECT
    NULL AS createdBy,
    NULL AS createdOn,
    NULL AS modifiedBy,
    NULL AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    i.SECONDARY_QUANTITY  AS ansStdQty,
    NULL AS ansStdQtyAvailableToReserve,
    NULL AS ansStdQtyAvailableToTransact,
    case when SECONDARY_UOM_CODE = 'EA' then 'PC'
      else
       SECONDARY_UOM_CODE
    end AS ansStdUomCode,
    i.currency AS currency,
    NULL AS dateReceived,
    i.DUAL_UOM_CONTROL AS dualUomControl,
    1 exchangeRate,
    UPPER(DATE.periodName)  AS glPeriodCode,
    NULL AS holdStatus,
    LAST_DAY(CURRENT_DATE () - INTERVAL 1 DAYS) AS inventoryDate,
    case when trim(Stock_ID) = '' then 'Not Available' else Stock_ID end  AS inventoryWarehouseId,
    concat(i.company,'-',INVENTORY_ITEM_ID) AS itemId,
    NULL AS lotControlFlag,
    i.EXP_DATE AS lotExpirationDate,
     case when trim(i.LOT_NUMBER) = '' then 'Not Available' else i.LOT_NUMBER end  AS lotNumber,
    NULL AS lotStatus,
    NULL AS manufacturingDate,
    NULL AS mrpn,
    case when i.ORGANIZATION_CODE = 'ACSAS' then '5400' else i.ORGANIZATION_CODE end AS owningBusinessUnitId,
    NULL AS originId,
    i.PRIMARY_QUANTITY AS primaryQty,
    0 AS primaryQtyAvailableToTransact,
    i.PRIMARY_UOM_CODE AS primaryUomCode,
    i.ITEM_NUMBER AS productCode,
    i.SECONDARY_QUANTITY AS secondaryQty,
    0 AS secondaryQtyAvailableToTransact,
    i.SECONDARY_UOM_CODE AS secondaryUomCode,
    i.SGA_CST / i.PRIMARY_QUANTITY AS sgaCostPerUnitPrimary,
    NULL AS shelfLifeDays,
    i.SGA_CST / i.PRIMARY_QUANTITY AS stdCostPerUnitPrimary,
    i.SUBINVENTORY_CODE AS subInventoryCode,
    NULL AS  totalCost
  FROM vw_qv_inventory i
  LEFT JOIN S_CORE.DATE DATE ON DATE.DAYID = DATE_FORMAT(LAST_DAY(CURRENT_DATE -1), 'yyyyMMdd')
""")

main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['itemID','inventoryWarehouseID','subInventoryCode','lotNumber','inventoryDate','owningBusinessUnitId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_supplychain_inventory())
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
  spark.sql("""
     SELECT
      concat(company,'-',INVENTORY_ITEM_ID) AS itemId,
      case when trim(Stock_ID) = '' then 'Not Available' else Stock_ID end  AS inventoryWarehouseId,
       case when trim(LOT_NUMBER) = '' then 'Not Available' else LOT_NUMBER end  AS lotNumber,
      SUBINVENTORY_CODE AS subInventoryCode,
      LAST_DAY(CURRENT_DATE() - INTERVAL 1 DAYS) AS inventoryDate,
      case when ORGANIZATION_CODE = 'ACSAS' then '5400' else ORGANIZATION_CODE end AS owningBusinessUnitId
    FROM col.vw_qv_inventory
    WHERE _DELETED IS FALSE
  """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'itemID,inventoryWarehouseID,subInventoryCode,lotNumber,inventoryDate,owningBusinessUnitId,_SOURCE'))
  .select('_ID')
  .transform(add_unknown_ID())
)

filter_date = str(spark.sql('SELECT LAST_DAY(CURRENT_DATE() - INTERVAL 1 DAYS) AS inventoryDate').collect()[0]['inventoryDate'])
apply_soft_delete(full_keys_f, table_name, key_columns = '_ID', date_field = 'inventoryDate', date_value = filter_date)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(vw_qv_inventory)
  update_cutoff_value(cutoff_value, table_name, 'col.vw_qv_inventory')
  update_run_datetime(run_datetime, table_name, 'col.vw_qv_inventory')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
