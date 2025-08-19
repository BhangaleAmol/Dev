# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.inventory

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('prms.PRMSF200_MFLBP100')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('PRMSF200_MFLBP100')

# COMMAND ----------

main = spark.sql("""
SELECT   
  0                                           createdBy,
  CURRENT_DATE - 1                            createdOn,
  0                                           modifiedBy,
  CURRENT_DATE - 1                            modifiedOn,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP)                                        insertedOn,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP)                                       updatedOn,
  SUM(CASE
        WHEN PM.UTMES = PZ.ANSUM                       
          THEN(LB.LBBAL-LB.LBISS+LB.LBREC+LB.LBADJ)
        WHEN PM.UTMES = 'DZ' AND PZ.ANSUM = 'PR' 
          THEN(LB.LBBAL-LB.LBISS+LB.LBREC+LB.LBADJ) * 12
        WHEN PM.UTMES = 'CT' AND PZ.ANSUM = 'PR' 
          THEN(LB.LBBAL-LB.LBISS+LB.LBREC+LB.LBADJ) * 144
        WHEN COALESCE(UTP.UTMDF, UTP_DEFAULT.UTMDF) = 2 
          THEN(LB.LBBAL-LB.LBISS+LB.LBREC+LB.LBADJ) * COALESCE(UTP.UTCON, UTP_DEFAULT.UTCON)
        WHEN COALESCE(UTP.UTMDF, UTP_DEFAULT.UTMDF) = 1 
          THEN(LB.LBBAL-LB.LBISS+LB.LBREC+LB.LBADJ) / COALESCE(UTP.UTCON, UTP_DEFAULT.UTCON)
        ELSE 999999999
  END)                                        ansStdQty,
  NULL                                        AS ansStdQtyAvailableToReserve,
  NULL                                        AS ansStdQtyAvailableToTransact,
  TRIM(PZ.ANSUM)                              ansStdUomCode,
  'AUD'                                       currency,
  ''                                          dateReceived,
  'N'                                         dualUOMControl,
  0                                           exchangeRate,
  DATE_FORMAT(ADD_MONTHS(CURRENT_DATE-1, 6), 'MMM-yy')              
                                              glPeriodCode,
  ''                                          holdStatus,
  LAST_DAY(CURRENT_DATE-1)                    inventoryDate,
  LB.HOUSE                                    inventoryWarehouseID,
  TRIM(LB.PRDNO)                              itemId,
  ''                                          lotControlFlag,
  LT.EXPDT                                    lotExpirationDate,
  COALESCE(TRIM(LT.LOTID), 'No Lot Number')   lotNumber,
  NULL                                        AS lotStatus,
  DATE_ADD(LT.EXPDT,  - INT(LT.SLIFE) )       manufacturingDate,
  TRIM(LB.PRDNO)                              MRPN,
  REPLACE(STRING(INT(LB.CMPNO)), ",", "")     owningBusinessUnitId,
  TRIM(LT.LOTSP)                              originId,
  SUM(LB.LBBAL-LB.LBISS+LB.LBREC+LB.LBADJ)    primaryQty,
  TRIM(PM.UTMES)                              primaryUOMCode,
  TRIM(LB.PRDNO)                              productCode,
  SUM(LB.LBBAL-LB.LBISS+LB.LBREC+LB.LBADJ)    secondaryQty,
  TRIM(PM.UTMES)                              secondaryUOMCode,
  COALESCE(0,0)                               sgaCostPerUnitPrimary,
  LT.SLIFE                                    shelfLifeDays,
  COALESCE(OC.STDC1,0)                        stdCostPerUnitPrimary,
  ''                                          subInventoryCode,
  NULL                                        totalCost
FROM
   PRMSF200_MFLBP100 LB
    LEFT JOIN PRMS.PRMSF200_MSPMP100 PM ON LB.PRDNO = PM.PRDNO
    LEFT JOIN (SELECT LTPRD,
                      LOTID,
                      MAX(LOTSP) LOTSP,
                      MAX(EXPDT) EXPDT,
                      MAX(SLIFE) SLIFE
               FROM PRMS.PRMSF200_MFLTP100 
               WHERE ACTIV = '1'
                --  AND NOT (LOTID = 160832 AND LTPRD = '70-342-9' AND LTADJ < 0)
               GROUP BY LTPRD, LOTID
               ) LT ON LB.PRDNO = LT.LTPRD
                                      AND LB.LOTID = LT.LOTID
    LEFT JOIN PRMS.PRMSF200_MFOSPA00 OC ON LB.PRDNO = OC.PRDNO
                                      AND LT.LOTSP = OC.SPECF
    LEFT JOIN PRMS.PRMSF200_MSPMZ100 PZ ON LB.PRDNO = PZ.PAPRD
    LEFT JOIN PRMS.PRMSF200_MSWMP100 WM ON LB.HOUSE = WM.HOUSE
    LEFT JOIN PRMS.PRMSF200_MSUTP100 UTP ON LB.PRDNO = UTP.UTPRD
                                       AND PM.UTMES = UTP.UTTUM
                                       AND PZ.ANSUM = UTP.UTFUM
    LEFT JOIN PRMS.PRMSF200_MSUTP100 UTP_DEFAULT ON PM.UTMES = UTP_DEFAULT.UTTUM
                                               AND PZ.ANSUM = UTP_DEFAULT.UTFUM
                                               AND UTP_DEFAULT.UTPRD IS NULL
WHERE
    LB.LBBAL - LB.LBISS + LB.LBREC + LB.LBADJ <> 0
    AND LB.HOUSE IN ('KL','S3','FW','KO','W','NW')
GROUP BY
    TRIM(LB.PRDNO),
    TRIM(LB.HOUSE),
    TRIM(PZ.ANSUM),
    TRIM(PM.DESCP),
    TRIM(PM.UTMES),
    OC.STDC1,
    TRIM(PZ.PADIV),
    TRIM(LT.LOTID),
    LT.EXPDT,
    TRIM(LT.LOTSP),
    LT.SLIFE,
    REPLACE(STRING(INT(LB.CMPNO)), ",", ""),
    LB.HOUSE     
""")

main.cache()
print(main.count())
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
      TRIM(PRDNO) itemId,
      HOUSE inventoryWarehouseID,
      '' subInventoryCode,
      COALESCE(TRIM(LOTID), 'No Lot Number') lotNumber,
      LAST_DAY(CURRENT_DATE-1) inventoryDate,
      REPLACE(STRING(INT(CMPNO)), ",", "")     owningBusinessUnitId
    FROM prms.prmsf200_mflbp100
    WHERE _DELETED IS FALSE
  """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'itemID,inventoryWarehouseID,subInventoryCode,lotNumber,inventoryDate,owningBusinessUnitId,_SOURCE'))
  .select('_ID')
  .transform(add_unknown_ID())
)

filter_date = str(spark.sql('SELECT LAST_DAY(CURRENT_DATE-1) inventoryDate').collect()[0]['inventoryDate'])
apply_soft_delete(full_keys_f, table_name, key_columns = '_ID', date_field = 'inventoryDate', date_value = filter_date)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc)
  update_cutoff_value(cutoff_value, table_name, 'prms.PRMSF200_MFLBP100')
  update_run_datetime(run_datetime, table_name, 'prms.PRMSF200_MFLBP100')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
