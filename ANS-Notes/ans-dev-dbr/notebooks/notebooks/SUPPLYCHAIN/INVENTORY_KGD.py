# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.inventory

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('kgd.dbo_tembo_inventory')
# main_inc_int = load_full_dataset('kgd.dbo_tembo_inventory')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)
  main_inc_int = main_inc_int.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

main = spark.sql("""
  SELECT
    NULL AS createdBy,
    NULL AS createdOn,
    NULL AS modifiedBy,
    NULL AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    i.PRIMARY_QUANTITY AS ansStdQty,
    NULL AS ansStdQtyAvailableToReserve,
    NULL AS ansStdQtyAvailableToTransact,
    i.PRIMARY_UOM_CODE AS ansStdUomCode,
    'RMB' AS currency,
    NULL AS dateReceived,
    'N' AS dualUomControl,
     NULL AS exchangeRate,
    UPPER(DATE.periodName) AS glPeriodCode,
    NULL AS holdStatus,
    LAST_DAY(CURRENT_DATE () - INTERVAL 1 DAYS) AS inventoryDate,
    'ASWH'  AS inventoryWarehouseId,
    i.ITEM_NUMBER AS itemId,
    NULL AS lotControlFlag,
    i.EXP_DATE AS lotExpirationDate,
    i.LOT_NUMBER  AS lotNumber,
    NULL AS lotStatus, 
    i.MAF_DATE AS manufacturingDate,
    i.prodCode AS mrpn,
    'AS' AS owningBusinessUnitId,
      right(i.LOT_NUMBER,2) AS originId,
    i.SECONDARY_QUANTITY AS primaryQty,
    0 AS primaryQtyAvailableToTransact,
    i.SECONDARY_UOM_CODE AS primaryUomCode,
    i.ITEM_NUMBER AS productCode,
    i.SECONDARY_QUANTITY AS secondaryQty,
    0 AS secondaryQtyAvailableToTransact,
    i.SECONDARY_UOM_CODE AS secondaryUomCode,
    0 AS sgaCostPerUnitPrimary,
    p.FKFPeriod AS shelfLifeDays,
    0 AS stdCostPerUnitPrimary,
    'unknown' AS subInventoryCode,
    NULL AS  totalCost
  FROM main_inc i
  Left Join kgd.dbo_tembo_product p on i.ITEM_NUMBER = p.productCode 
  LEFT JOIN s_core.DATE DATE ON DATE.DAYID = DATE_FORMAT(LAST_DAY(CURRENT_DATE-1),'yyyyMMdd')
  WHERE NOT(i._DELETED)
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
      ITEM_NUMBER AS itemId,
      'ASWH'  AS inventoryWarehouseId, 
      'unknown' AS subInventoryCode,
      LOT_NUMBER AS lotNumber,      
      LAST_DAY(CURRENT_DATE() - INTERVAL 1 DAYS) AS inventoryDate,
      'AS' AS owningBusinessUnitId
    FROM kgd.dbo_tembo_inventory
    WHERE NOT(_DELETED)
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
  cutoff_value = get_incr_col_max_value(main_inc)
  update_cutoff_value(cutoff_value, table_name, 'kgd.dbo_tembo_inventory')
  update_run_datetime(run_datetime, table_name, 'kgd.dbo_tembo_inventory')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
