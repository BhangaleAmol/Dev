# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.inventory

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('tot.tb_pbi_inventory')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('tb_pbi_inventory')

# COMMAND ----------

main = spark.sql("""
  SELECT
      CAST(NULL AS STRING) AS createdBy,
      CAST(i.Date AS TIMESTAMP) AS createdOn,
      CAST(NULL AS STRING) AS modifiedBy,
      CAST(i.Date AS TIMESTAMP) AS modifiedOn,
      CURRENT_TIMESTAMP() AS insertedOn,
      CURRENT_TIMESTAMP() AS updatedOn,
      sum(CAST(i.Stock_qtty_converted AS DECIMAL(22,7))) AS ansStdQty,
      NULL                                          AS ansStdQtyAvailableToReserve,
      NULL                                          AS ansStdQtyAvailableToTransact,
      UOM_std_Ansell AS ansStdUomCode,
      'BRL' AS currency,
      CAST(NULL AS TIMESTAMP) AS dateReceived,
      CAST(NULL AS STRING) AS dualUomControl,
      0                                             AS exchangeRate,
      upper(periodName)  AS glPeriodCode,
      CAST(NULL AS STRING) AS holdStatus,
      LAST_DAY(CURRENT_DATE() - INTERVAL 1 DAYS) AS inventoryDate,
      case
      when i.Company = '5210'
        then '520-ABL'
      when i.Company = '5220'
        then '521-HER'
      end AS inventoryWarehouseId,
      concat(i.Company, '-', i.Product_cod) AS itemId,
      CAST(NULL AS STRING) AS lotControlFlag,
      CAST(i.Expired_date AS DATE) AS lotExpirationDate,
      i.Lot_number AS lotNumber,
      NULL                                          AS lotStatus,
      CAST(NULL AS TIMESTAMP) AS manufacturingDate,
      CAST(NULL AS STRING) AS mrpn,
      i.Company AS owningBusinessUnitId,
      CAST(NULL AS STRING) AS originId,
      sum(CAST(i.Stock_qtty AS DECIMAL(22,7))) AS primaryQty,
      0 AS primaryQtyAvailableToTransact,
      p.UOM AS primaryUomCode,
      i.Product_cod AS productCode,
      sum(CAST(i.Stock_qtty AS DECIMAL(22,7))) AS secondaryQty,
      0 AS secondaryQtyAvailableToTransact,
      CAST(NULL AS STRING) AS secondaryUomCode,
      CAST(NULL AS INT)            AS sgaCostPerUnitPrimary,
      CAST(NULL AS INT) AS shelfLifeDays,
      CAST(NULL AS INT)            AS stdCostPerUnitPrimary,
      i.Stock_name AS subInventoryCode,
      NULL                                          AS totalCost
  FROM tb_pbi_inventory i
    LEFT JOIN tot.tb_pbi_product p ON i.Company = p.Company AND i.Product_cod = p.Product_cod
     join s_core.date d on  current_date() - 1 = dateid
  group by
      CAST(i.Date AS TIMESTAMP),
      uom_std_Ansell,
      case
      when i.Company = '5210'
        then '520-ABL'
      when i.Company = '5220'
        then '521-HER'
      end,
      concat(i.Company, '-', i.Product_cod),
      CAST(i.Expired_date AS DATE),
      i.Lot_number,
      i.Company,
      p.UOM,
      i.Stock_name,
      upper(d.periodName),
      i.Product_cod
""")

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
  .transform(convert_null_to_zero(['lotNumber'])) # when fixed on source, it will generate new SK!
  .transform(tg_supplychain_inventory())
  .transform(apply_schema(schema))  
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
display(main_f)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.sql("""
    SELECT distinct
concat(Company, '-', Product_cod) AS itemId,
case
            when Company = '5210'
        then '520-ABL'
            when Company = '5220'
        then '521-HER'
        end AS inventoryWarehouseId,
Stock_name AS subInventoryCode,  
Lot_number AS lotNumber,
LAST_DAY(CURRENT_DATE() - INTERVAL 1 DAYS) AS inventoryDate,
        Company AS owningBusinessUnitId
    FROM tot.tb_pbi_inventory
    WHERE _DELETED IS FALSE
  """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'itemID,inventoryWarehouseId,subInventoryCode,lotNumber,inventoryDate,owningBusinessUnitId,_SOURCE'))
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
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_inventory')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_inventory')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
