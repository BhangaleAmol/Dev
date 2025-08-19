# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.inventory

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sapp01.mchb', prune_days)
  main_inc = load_incr_dataset('sapp01.mchb', '_MODIFIED', cutoff_value)
else:
  main_inc = load_full_dataset('sapp01.mchb')
  # VIEWS
main_inc.createOrReplaceTempView('mchb')
# main_inc.display()

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

selected_items = spark.sql("""
select
OBJEK  AS MATNR from sapp01.AUSP
        join sapp01.CABN on AUSP.MANDT = CABN.MANDT and AUSP.ATINN = CABN.ATINN
where AUSP.MANDT = '030'
and AUSP.ATWRT = 'Y'
and CABN.ATNAM = 'Z_MLE_RELEVANCE'
""")

selected_items.createOrReplaceTempView("selected_items")
selected_items.cache()

# COMMAND ----------

WHS_LKP = spark.sql("""
select distinct Z_LOW from sapp01.ZBC_INTERF_PARAM where MANDT = '030' and INTEFRACE =  'SAP2MLE' and ID_PARAM = 'WERKS' and nvl(_deleted,0) = 0
""")

WHS_LKP.createOrReplaceTempView("WHS_LKP")
WHS_LKP.cache()

# COMMAND ----------

LGORT_LKP = spark.sql("""select distinct Z_LOW from sapp01.ZBC_INTERF_PARAM where MANDT = '030' and INTEFRACE =  'SAP2MLE' and ID_PARAM = 'LGORT' and nvl(_deleted,0) = 0""")
LGORT_LKP.createOrReplaceTempView('LGORT_LKP')

# COMMAND ----------

TEMBO_AVAIL_SUBINVENTORY_CODE_LKP = spark.sql("""
SELECT DISTINCT
    KEY_VALUE 
FROM SMARTSHEETS.EDM_Control_Table 
WHERE table_Id = 'TEMBO_AVAILABLE_SUBINVENTORY_CODES' 
""")
TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.createOrReplaceTempView("TEMBO_AVAIL_SUBINVENTORY_CODE_LKP")

# COMMAND ----------

main = spark.sql("""
SELECT   
  NULL createdBy,
  NULL createdOn,
  NULL modifiedBy,
  NULL modifiedOn,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP) insertedOn,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP) updatedOn,
  sum(MCHB.CLABS) AS ansStdQty,
  NULL AS ansStdQtyAvailableToReserve,
  sum(CASE
                WHEN MCHB.LGORT = TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.KEY_VALUE
                THEN MCHB.CLABS
                ELSE 0
            END) AS ansStdQtyAvailableToTransact,
  mara.meins AS ansStdUomCode,
  'EUR' AS currency,
  NULL AS dateReceived,
  NULL AS dualUOMControl,
  1 AS exchangeRate,
  (SELECT MAX(UPPER(periodName)) AS periodName FROM s_core.date WHERE date.dateId=CURRENT_DATE - 1)AS glPeriodCode,
  NULL AS holdStatus,
  LAST_DAY(CURRENT_DATE () - INTERVAL 1 DAYS) AS inventoryDate, 
  MCHB.WERKS AS inventoryWarehouseID,
  selected_items.MATNR AS itemId,
  NULL AS lotControlFlag,
  MCHA.VFDAT AS lotExpirationDate,
  MCHB.CHARG AS lotNumber,
  NULL AS lotStatus,
  NULL AS manufacturingDate,
  NULL AS MRPN,
  T001W.VKORG AS owningBusinessUnitId,
  NULL AS originId,
  sum(MCHB.CLABS) AS primaryQty,
  sum(CASE
          WHEN MCHB.LGORT = TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.KEY_VALUE
          THEN MCHB.CLABS
          ELSE 0
      END) AS primaryQtyAvailableToTransact,
  MARA.MEINS AS primaryUOMCode,
  selected_items.MATNR                        AS productCode,
  sum(MCHB.CLABS) AS secondaryQty,
  sum(CASE
          WHEN MCHB.LGORT = TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.KEY_VALUE
          THEN MCHB.CLABS
          ELSE 0
      END) AS secondaryQtyAvailableToTransact,
  MARA.MEINS AS secondaryUOMCode,
  0 AS sgaCostPerUnitPrimary,
  NULL AS shelfLifeDays,
  MBEW.VERPR / MBEW.PEINH AS stdCostPerUnitPrimary,
  MCHB.LGORT                                  AS subInventoryCode,
  MBEW.VERPR / MBEW.PEINH AS totalCost
FROM   MCHB 
Inner Join WHS_LKP on MCHB.WERKS=WHS_LKP.Z_LOW
Inner Join sapp01.mcha on mchb.mandt = mcha.mandt and mchb.matnr = mcha.matnr and mchb.werks = mcha.werks and mchb.charg = mcha.charg 
Inner Join selected_items on MCHB.MATNR = selected_items.MATNR
--Inner Join LGORT_LKP  on  MCHB.LGORT  = LGORT_LKP.Z_LOW
Left outer Join sapp01.mara on MCHB.MATNR = MARA.MATNR and MCHB.MANDT = MARA.MANDT
Left outer Join sapp01.T001W on MCHB.MANDT = T001W.MANDT and MCHB.WERKS = T001W.WERKS
LEFT JOIN TEMBO_AVAIL_SUBINVENTORY_CODE_LKP
     ON TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.KEY_VALUE = MCHB.LGORT
LEFT JOIN
  sapp01.MBEW MBEW ON MCHB.MANDT = MBEW.MANDT AND MCHB.MATNR = MBEW.MATNR AND MCHB.WERKS = MBEW.BWKEY AND MBEW._DELETED = 0
where
 nvl(mchb._deleted,0) = 0
 and nvl(mcha._deleted,0) = 0
 and nvl(mara._deleted,0) = 0
 and nvl(T001W._deleted,0) = 0
 --and MCHB.MATNR = '000000000000823018'
GROUP BY 
  mara.meins,
  MCHB.MANDT,
  MCHB.MATNR,
  MCHB.WERKS,
  MCHA.VFDAT,
  MCHB.CHARG,
  MCHB.LGORT,
  selected_items.MATNR,
  T001W.VKORG,
  MBEW.VERPR,
  MBEW.PEINH
""")
main.createOrReplaceTempView("main")
main.count()
# display(main)

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
# main_f.display()
main_f.createOrReplaceTempView('main_f')

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
query = """
    SELECT
          selected_items.MATNR AS itemId,
          MCHB.WERKS AS inventoryWarehouseID,
          MCHB.LGORT AS subInventoryCode,
          MCHB.CHARG AS lotNumber,
          LAST_DAY(CURRENT_DATE () - INTERVAL 1 DAYS) AS inventoryDate ,
          T001W.VKORG AS owningBusinessUnitId
    FROM 
    sapp01.MCHB
    Inner Join selected_items on MCHB.MATNR = selected_items.MATNR
    Inner Join sapp01.T001W on MCHB.MANDT = T001W.MANDT and MCHB.WERKS = T001W.WERKS
where
 nvl(mchb._deleted,0) = 0
GROUP BY 
  selected_items.MATNR,
  MCHB.WERKS,
  MCHB.CHARG,
  MCHB.LGORT,
  T001W.VKORG  
  """


full_keys_f = (
  spark.sql(query)
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
  cutoff_value = get_incr_col_max_value(main_inc)
  update_cutoff_value(cutoff_value, table_name, 'sapp01.mchb')
  update_run_datetime(run_datetime, table_name, 'sapp01.mchb')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
