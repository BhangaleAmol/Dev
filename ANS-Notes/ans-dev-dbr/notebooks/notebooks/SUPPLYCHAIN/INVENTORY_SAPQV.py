# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.inventory

# COMMAND ----------

# LOAD DATASETS
vw_qv_inventory = load_full_dataset('sap.inventory')

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
  0                                           createdBy,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP)                createdOn,
  0                                           modifiedBy,
  CAST(NULL AS TIMESTAMP)             modifiedOn,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP)     insertedOn,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP)     updatedOn,
  CAST(Max(CASE
      WHEN StorageLocation <> 'IN TRANSIT'
      THEN STOCK
      ELSE 0
  END) AS DECIMAL(22,7))                                         AS ansStdQty,
  NULL                                        AS ansStdQtyAvailableToReserve,
  NULL                                        AS ansStdQtyAvailableToTransact,
  UOM                                         AS ansStdUomCode,
  NULL                                        AS currency,
  NULL                                        AS dateReceived,
  NULL                                        AS dualUOMControl,
  NULL                                        AS exchangeRate,
  Period                                      AS glPeriodCode,
  NULL                                        AS holdStatus,
  InventoryDate                               AS inventoryDate, 
  Plant                                       AS inventoryWarehouseID,
  MaterialNumber                              AS itemId,
  NULL                                        AS lotControlFlag,
  max(ExpiryDate)                             AS lotExpirationDate,
  COALESCE(UPPER(BatchNumber),'No Lot Number')AS lotNumber,
  NULL                                        AS lotStatus,
  NULL                                        AS manufacturingDate,
  MaterialNumber                              AS MRPN,
  EntityCode                                  AS owningBusinessUnitId,
  NULL                                        AS originId,
  CAST(Max(Stock) AS DECIMAL(22,7))           AS primaryQty,
  NULL                                        AS primaryUOMCode,
  NULL                                        AS productCode,
  NULL                                        AS secondaryQty,
  NULL                                        AS secondaryUOMCode,
  NULL                                        AS sgaCostPerUnitPrimary,
  NULL                                        AS shelfLifeDays,
  NULL                                        AS stdCostPerUnitPrimary,
  StorageLocation                             AS subInventoryCode,
  NULL                                        AS totalCost
FROM vw_qv_inventory inventory
GROUP BY 
   StorageLocation
  ,EntityCode
  ,MaterialNumber
  ,UPPER(BatchNumber)
  ,Plant
  ,InventoryDate
  ,Period
  ,UOM
""")
main.createOrReplaceTempView("main")
main.count()
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
  .withColumn('itemID', f.regexp_replace('itemID', r'^[0]*', ''))
  .withColumn('MRPN', f.regexp_replace('MRPN', r'^[0]*', ''))
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
          MaterialNumber                              AS itemId,
          Plant                                       AS inventoryWarehouseID,
          StorageLocation                             AS subInventoryCode,
          COALESCE(UPPER(BatchNumber),'No Lot Number')       AS lotNumber,
          InventoryDate                               AS inventoryDate ,
          EntityCode                                  AS owningBusinessUnitId
    FROM sap.inventory
  """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'itemID,inventoryWarehouseID,subInventoryCode,lotNumber,inventoryDate,owningBusinessUnitId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(vw_qv_inventory)
  update_cutoff_value(cutoff_value, table_name, 'sap.inventory')
  update_run_datetime(run_datetime, table_name, 'sap.inventory')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
