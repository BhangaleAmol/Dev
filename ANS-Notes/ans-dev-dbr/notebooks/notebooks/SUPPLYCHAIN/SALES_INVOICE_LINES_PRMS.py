# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_lines

# COMMAND ----------

# LOAD DATASETS
prmsf200_obcop300 = load_full_dataset('prms.prmsf200_obcop300')
prmsf200_obcop300.createOrReplaceTempView('prmsf200_obcop300')

prmsf200_obcopa03 = load_full_dataset('prms.prmsf200_obcopa03')
prmsf200_obcopa03.createOrReplaceTempView('prmsf200_obcopa03')

prmsf200_obcdp200 = load_full_dataset('prms.prmsf200_obcdp200')
prmsf200_obcdp200.createOrReplaceTempView('prmsf200_obcdp200')

# COMMAND ----------

# SAMPLING
if sampling:
  prmsf200_obcop300 = prmsf200_obcop300.limit(10)
  prmsf200_obcop300.createOrReplaceTempView('prmsf200_obcop300')
  
  prmsf200_obcopa03 = prmsf200_obcopa03.limit(10)
  prmsf200_obcopa03.createOrReplaceTempView('prmsf200_obcopa03')
  
  prmsf200_obcdp200 = prmsf200_obcdp200.limit(10)
  prmsf200_obcdp200.createOrReplaceTempView('prmsf200_obcdp200')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(prmsf200_obcop300, 'CMPNO,C3INV,ORDNO,LINE,SEQNO')
valid_count_rows(prmsf200_obcopa03, 'CMPNO,C3INV,ORDNO,LINE,SEQNO')
valid_count_rows(prmsf200_obcdp200, 'CMPNO,MEMNO,CMLN')

# COMMAND ----------

invoices = spark.sql("""
SELECT 
    OBIRP111.IRLUS AS createdBy,
	OBIRP111.TADCD AS createdOn,
	OBIRP111.IRLUS AS modifiedBy,
	OBIRP111.TADCD AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
    NULL AS accrualBogo,
    NULL AS accrualCoop,
    NULL AS accrualEndUserRebate,
    NULL AS accrualOthers,
    NULL AS accrualTpr,
    NULL AS accrualVolumeDiscount,
	NULL AS actualDeliveryOn,
	OBCOP300.S30DT AS actualShipDate,
    NULL AS ansStdUom,
	NULL AS ansStdUomConv,
	OBCOP300.SHIPD * OBCOP300.SELLP AS baseAmount,
	OBCOP200.C2ODT AS bookedDate,
	'Y' AS bookedFlag,
	'N' AS cancelledFlag,
	NULL AS caseUomConv,
	NULL AS cetd,
	'001' AS client,
	NULL AS customerLineNumber,
	NULL AS customerPoNumber,
	NULL AS deliverNoteDate,
	NULL AS deliveryNoteId,
    NULL AS discountLineFlag,
    NULL AS distributionChannel,
	OBCOP300.C3EXR AS exchangeRate,
	OBIRP111.TTDDT AS exchangeRateDate,
	NULL AS intransitTime,
	REPLACE(STRING(INT (OBCOP300.CMPNO)), ",", "") || '-' || OBCOP300.HOUSE AS inventoryWarehouseId,
	REPLACE(STRING(INT(OBCOP300.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCOP300.C3INV)), ",", "")  || '-' || REPLACE(STRING(INT(OBCOP300.ORDNO)), ",", "")  || '-' || REPLACE(STRING(INT(OBCOP300.LINE)), ",", "")  || '-' || REPLACE(STRING(INT(OBCOP300.SEQNO)), ",", "")   AS invoiceDetailId, 
	REPLACE(STRING(INT(OBCOP300.C3INV)), ",", "") AS invoiceId,
    NULL AS invoiceUomCode,
    TRIM(OBCOP300.PRDNO) AS itemId,
	OBCOP300.C3CST AS legalEntityCost,
    OBCOP300.C3CST AS legalEntityCostLeCurrency,
    'LINE' AS lineType,
	TRIM(OBCOP300.LOTID) AS lotNumber,
	NULL AS needByDate,
	OBCOP200.QUANO * OBCOP200.ACTSP AS orderAmount,
	TRIM(OBCOP200.PRDNO) AS orderedItem,
	NULL AS orderLineHoldType,
    NULL AS orderLineNumber,
	OBCOP200.C2STS AS orderLineStatus,
	REPLACE(STRING(INT(OBCOP300.ORDNO)), ",", "") AS orderNumber,
	OBCOP200.C2STS AS orderStatusDetail,
    OBCOP100.C1OTP ordertypeId,
	OBCOP200.UNITM AS orderUomCode,
	REPLACE(STRING(INT(OBCOP300.CMPNO)), ",", "") AS owningBusinessUnitId,
    NULL AS priceListId,
    NULL AS priceListName,
	ROUND(OBCOP300.SELLP, 2) AS pricePerUnit,
	TRIM(MSPMP100.UTMES) AS primaryUomCode,
	NULL AS primaryUomConv,
	TRIM(OBCOP300.PRDNO) AS productCode,
	NULL AS promiseDate,
	NULL AS promisedOnDate,
	NULL AS quantityBackordered,
	NULL AS quantityCancelled,
	OBCOP300.SHIPD AS quantityInvoiced,
	OBCOP200.QUANO AS quantityOrdered,
	OBCOP200.QUANA AS quantityReserved,
    NULL AS quantityReturned,
	OBCOP200.QUANS AS quantityShipped,
	NULL AS requestDeliveryBy,
	NULL AS retd,
    NULL AS returnAmount,
    NULL AS returnFlag,
	REPLACE(STRING(INT(OBCOP300.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCOP200.ORDNO)), ",", "")  AS salesOrderId,
	REPLACE(STRING(INT(OBCOP300.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCOP200.ORDNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCOP200.LINE)), ",", "") AS salesorderDetailId,
	OBCOP300.C3SDT AS scheduledShipDate,
	NULL AS sentDate943,
	REPLACE(STRING(INT( OBCOP300.SEQNO)), ",", "") AS sequenceNumber,
    NULL AS settlementDiscountEarned,
    NULL AS settlementDiscountsCalculated,
    NULL AS settlementDiscountUnEarned,
	NULL AS shipDate945,
	OBCOP300.C3CST AS seeThruCost,
    OBCOP300.C3CST AS seeThruCostLeCurrency,
    NULL AS shipToAddressId,
	OBCOP300.HOUSE AS warehouseCode 
FROM prms.prmsf200_obcop300 obcop300
  INNER JOIN prms.prmsf200_obirp111 obirp111 ON obcop300.c3inv = obirp111.invno
  INNER JOIN prms.prmsf200_obcop200 obcop200 on obcop300.cmpno = obcop200.cmpno
    and obcop300.ordno = obcop200.ordno
    and obcop300.line = obcop200.line
  INNER JOIN prms.prmsf200_mspmp100 mspmp100 on obcop300.prdno = mspmp100.prdno
  INNER JOIN prms.prmsf200_mspmz100 mspmz100 on obcop300.prdno = mspmz100.paprd
  INNER JOIN prms.prmsf200_obcop100 obcop100 on obcop200.ordno = obcop100.ordno 
  
  where obirp111.invno <> 0
""")

invoices.cache()
invoices.createOrReplaceTempView('invoices')
display(invoices)

# COMMAND ----------

invoices_hist = spark.sql("""
SELECT 
    OBIRP111.IRLUS AS createdBy,
	OBIRP111.TADCD AS createdOn,
	OBIRP111.IRLUS AS modifiedBy,
	OBIRP111.TADCD AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
    NULL AS accrualBogo,
    NULL AS accrualCoop,
    NULL AS accrualEndUserRebate,
    NULL AS accrualOthers,
    NULL AS accrualTpr,
    NULL AS accrualVolumeDiscount,
	NULL AS actualDeliveryOn,
	OBCOP300.S30DT AS actualShipDate,
	NULL AS ansStdUomConv,
    NULL AS ansStdUom,
	OBCOP300.SHIPD * OBCOP300.SELLP AS baseAmount,
	OBCOP200.C2ODT AS bookedDate,
	'Y' AS bookedFlag,
	'N' AS cancelledFlag,
	NULL AS caseUomConv,
	NULL AS cetd,
	'001' AS client,
	NULL AS customerLineNumber,
	NULL AS customerPoNumber,
	NULL AS deliverNoteDate,
	NULL AS deliveryNoteId,
    NULL AS discountLineFlag,
    NULL AS distributionChannel,
	OBCOP300.C3EXR AS exchangeRate,
	OBIRP111.TTDDT AS exchangeRateDate,
	NULL AS intransitTime,
	REPLACE(STRING(INT (OBCOP300.CMPNO)), ",", "") || '-' || OBCOP300.HOUSE AS inventoryWarehouseId,
	REPLACE(STRING(INT(OBCOP300.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCOP300.C3INV)), ",", "")  || '-' || REPLACE(STRING(INT(OBCOP300.ORDNO)), ",", "")  || '-' || REPLACE(STRING(INT(OBCOP300.LINE)), ",", "")  || '-' || REPLACE(STRING(INT(OBCOP300.SEQNO)), ",", "")   AS invoiceDetailId, 
	REPLACE(STRING(INT(OBCOP300.C3INV)), ",", "") AS invoiceId,
    NULL AS invoiceUomCode,
    TRIM(OBCOP300.PRDNO) AS itemId,
	OBCOP300.C3CST AS legalEntityCost,
    OBCOP300.C3CST AS legalEntityCostLeCurrency,
    'LINE' AS lineType,
	TRIM(OBCOP300.LOTID) AS lotNumber,
	NULL AS needByDate,
	OBCOP200.QUANO * OBCOP200.ACTSP AS orderAmount,
	TRIM(OBCOP200.PRDNO) AS orderedItem,
	NULL AS orderLineHoldType,
    NULL AS orderLineNumber,
	OBCOP200.C2STS AS orderLineStatus,
	REPLACE(STRING(INT(OBCOP300.ORDNO)), ",", "") AS orderNumber,
	OBCOP200.C2STS AS orderStatusDetail,
    OBCOP100.C1OTP ordertypeId,
	OBCOP200.UNITM AS orderUomCode,
	REPLACE(STRING(INT(OBCOP300.CMPNO)), ",", "") AS owningBusinessUnitId,
    NULL AS priceListId,
    NULL AS priceListName,
	ROUND(OBCOP300.SELLP, 2) AS pricePerUnit,
	TRIM(MSPMP100.UTMES) AS primaryUomCode,
	NULL AS primaryUomConv,
	TRIM(OBCOP300.PRDNO) AS productCode,
	NULL AS promiseDate,
	NULL AS promisedOnDate,
	NULL AS quantityBackordered,
	NULL AS quantityCancelled,
	OBCOP300.SHIPD AS quantityInvoiced,
	OBCOP200.QUANO AS quantityOrdered,
	OBCOP200.QUANA AS quantityReserved,
    NULL AS quantityReturned,
	OBCOP200.QUANS AS quantityShipped,
	NULL AS requestDeliveryBy,
	NULL AS retd,
    NULL AS returnAmount,
    NULL AS returnFlag,
	REPLACE(STRING(INT(OBCOP300.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCOP200.ORDNO)), ",", "")  AS salesOrderId,
	REPLACE(STRING(INT(OBCOP300.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCOP200.ORDNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCOP200.LINE)), ",", "") AS salesorderDetailId,
	OBCOP300.C3SDT AS scheduledShipDate,
	NULL AS sentDate943,
	REPLACE(STRING(INT( OBCOP300.SEQNO)), ",", "") AS sequenceNumber,
    NULL AS settlementDiscountEarned,
    NULL AS settlementDiscountsCalculated,
    NULL AS settlementDiscountUnEarned,
	NULL AS shipDate945,
	OBCOP300.C3CST AS seeThruCost,
    OBCOP300.C3CST AS seeThruCostLeCurrency,
    NULL AS shipToAddressId,
	OBCOP300.HOUSE AS warehouseCode
FROM prms.prmsf200_obcopa03 obcop300
  INNER JOIN prms.prmsf200_obirp111 obirp111 ON obcop300.c3inv = obirp111.invno
  INNER JOIN prms.prmsf200_obcop200 obcop200 on obcop300.cmpno = obcop200.cmpno
    and obcop300.ordno = obcop200.ordno
    and obcop300.line = obcop200.line
  INNER JOIN prms.prmsf200_mspmp100 mspmp100 on obcop300.prdno = mspmp100.prdno
  INNER JOIN prms.prmsf200_mspmz100 mspmz100 on obcop300.prdno = mspmz100.paprd
  INNER JOIN prms.prmsf200_obcop100 obcop100 on obcop200.ordno = obcop100.ordno 
  
  where obirp111.invno <> 0
""")

invoices_hist.cache()
invoices_hist.createOrReplaceTempView('invoices_hist')
display(invoices_hist)

# COMMAND ----------

credit_notes = spark.sql("""
SELECT 
    OBCDP100.ENTUS AS createdBy,
	OBCDP100.CMRDT AS createdOn,
	OBCDP100.MNTUS AS modifiedBy,
	OBCDP100.CMRDT AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
    NULL AS accrualBogo,
    NULL AS accrualCoop,
    NULL AS accrualEndUserRebate,
    NULL AS accrualOthers,
    NULL AS accrualTpr,
    NULL AS accrualVolumeDiscount,
	NULL AS actualDeliveryOn,
	OBCDP100.CMRDT AS actualShipDate,
    NULL AS ansStdUom,
	NULL AS ansStdUomConv,
	OBCDP200.MEMQT * OBCDP200.MPRIC * -1 AS baseAmount,
	OBCDP100.CMRDT AS bookedDate,
	'Y' AS bookedFlag,
	'N' AS cancelledFlag,
	NULL AS caseUomConv,
	NULL AS cetd,
	'001' AS client,
	NULL AS customerLineNumber,
	NULL AS customerPoNumber,
	NULL AS deliverNoteDate,
	NULL AS deliveryNoteId,
    NULL AS discountLineFlag,
    NULL AS distributionChannel,
	OBCDP200.CM2ER AS exchangeRate,
	OBCDP100.CMRDT AS exchangeRateDate,
	NULL AS intransitTime,
	'' AS inventoryWarehouseId,
	REPLACE(STRING(INT( OBCDP200.CMPNO)), ",", "")  || '-' || REPLACE(STRING(INT( OBCDP200.MEMNO)), ",", "")   || '-' || REPLACE(STRING(INT( OBCDP200.CMLN)), ",", "")  AS invoiceDetailId, 
	REPLACE(STRING(INT( OBCDP200.MEMNO)), ",", "") AS invoiceId,
    NULL AS invoiceUomCode,
    TRIM(OBCDP200.PRDNO) AS itemId,
	OBCDP200.MEMQT * OBCDP200.CSTSP * -1 AS legalEntityCost,
    OBCDP200.MEMQT * OBCDP200.CSTSP * -1 AS legalEntityCostLeCurrency,
    'LINE' AS lineType,
	TRIM(OBCDP200.CMLOT) AS lotNumber,
	NULL AS needByDate,
	OBCDP200.MEMQT * OBCDP200.MPRIC * -1  AS orderAmount,
	TRIM(OBCDP200.PRDNO) AS orderedItem,
	NULL AS orderLineHoldType,
    NULL AS orderLineNumber,
	'' AS orderLineStatus,
	REPLACE(STRING(INT( OBCDP200.ORDNO)), ",", "")  AS orderNumber,
	'' AS orderStatusDetail,
    '' AS ordertypeId,
	OBCDP200.UNITM AS orderUomCode,
	REPLACE(STRING(INT( OBCDP200.CMPNO)), ",", "") AS owningBusinessUnitId,
    NULL AS priceListId,
    NULL AS priceListName,
	ROUND(OBCDP200.MPRIC, 2) AS pricePerUnit,
	TRIM(MSPMP100.UTMES) AS primaryUomCode,
	OBCDP200.CM2CF AS primaryUomConv,
	TRIM(OBCDP200.PRDNO) AS productCode,
	NULL AS promiseDate,
	NULL AS promisedOnDate,
	NULL AS quantityBackordered,
	NULL AS quantityCancelled,
	OBCDP200.MEMQT * -1 AS quantityInvoiced,
	0 AS quantityOrdered,
	0 AS quantityReserved,
    NULL AS quantityReturned,
	0 AS quantityShipped,
	NULL AS requestDeliveryBy,
	NULL AS retd,
    NULL AS returnAmount,
    NULL AS returnFlag,
	REPLACE(STRING(INT(OBCDP200.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCDP200.ORDNO)), ",", "")  AS salesOrderId,
	REPLACE(STRING(INT(OBCDP200.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCDP200.ORDNO)), ",", "")  || '-' || REPLACE(STRING(INT(OBCDP200.LINE)), ",", "")   AS salesorderDetailId,
	NULL AS scheduledShipDate,
	NULL AS sentDate943,
	'' AS sequenceNumber,
    NULL AS settlementDiscountEarned,
    NULL AS settlementDiscountsCalculated,
    NULL AS settlementDiscountUnEarned,
	NULL AS shipDate945,
	OBCDP200.MEMQT * OBCDP200.CSTSP * -1 AS seeThruCost,
    OBCDP200.MEMQT * OBCDP200.CSTSP * -1 AS seeThruCostLeCurrency,
    NULL AS shipToAddressId,
	'' AS warehouseCode

FROM prms.prmsf200_obcdp100 obcdp100
  INNER JOIN prms.prmsf200_obcdp200 obcdp200 ON obcdp100.memno = obcdp200.memno
    and obcdp100.cmpno = obcdp200.cmpno
  LEFT JOIN prms.prmsf200_mspmp100 mspmp100 on obcdp200.prdno = mspmp100.prdno
  LEFT JOIN prms.prmsf200_mspmz100 mspmz100 on obcdp200.prdno = mspmz100.paprd
""")

credit_notes.cache()
credit_notes.createOrReplaceTempView('credit_notes')
display(credit_notes)

# COMMAND ----------

main = spark.sql("""
  SELECT * FROM invoices
  UNION
  SELECT * FROM invoices_hist
  UNION 
  SELECT * FROM credit_notes
""")

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['invoiceDetailId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .withColumn('orderType',f.lit('ORDER_TYPE'))
  .transform(tg_default(source_name))
  .transform(tg_supplychain_sales_invoice_lines())
  .drop('orderType')
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
full_obcop300_keys = spark.sql("""
  SELECT
    REPLACE(STRING(INT(CMPNO)), ",", "") || '-' || 
    REPLACE(STRING(INT(C3INV)), ",", "") || '-' || 
    REPLACE(STRING(INT(ORDNO)), ",", "") || '-' || 
    REPLACE(STRING(INT(LINE)), ",", "")  || '-' || 
    REPLACE(STRING(INT(SEQNO)), ",", "") AS invoiceDetailId
  FROM prms.prmsf200_obcop300
  WHERE _DELETED IS FALSE
""")

full_obcopa03_keys = spark.sql("""
  SELECT
    REPLACE(STRING(INT(CMPNO)), ",", "") || '-' || 
    REPLACE(STRING(INT(C3INV)), ",", "") || '-' || 
    REPLACE(STRING(INT(ORDNO)), ",", "") || '-' || 
    REPLACE(STRING(INT(LINE)), ",", "")  || '-' || 
    REPLACE(STRING(INT(SEQNO)), ",", "") AS invoiceDetailId
  FROM prms.prmsf200_obcopa03
  WHERE _DELETED IS FALSE
""")

full_obcdp100_keys = spark.sql("""
  SELECT
    REPLACE(STRING(INT(CMPNO)), ",", "") || '-' || 
    REPLACE(STRING(INT(MEMNO)), ",", "") || '-' || 
    REPLACE(STRING(INT(CMLN)), ",", "") AS invoiceDetailId
  FROM prms.prmsf200_obcdp100
  WHERE _DELETED IS FALSE
""")

full_keys_f = (
  full_obcop300_keys
  .union(full_obcopa03_keys)
  .union(full_obcdp100_keys)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'invoiceDetailId,_SOURCE'))
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

  cutoff_value = get_incr_col_max_value(prmsf200_obcop300)
  update_cutoff_value(cutoff_value, table_name, 'prms.prmsf200_obcop300')
  update_run_datetime(run_datetime, table_name, 'prms.prmsf200_obcop300')

  cutoff_value = get_incr_col_max_value(prmsf200_obcdp200)
  update_cutoff_value(cutoff_value, table_name, 'prms.prmsf200_obcdp200')
  update_run_datetime(run_datetime, table_name, 'prms.prmsf200_obcdp200')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
