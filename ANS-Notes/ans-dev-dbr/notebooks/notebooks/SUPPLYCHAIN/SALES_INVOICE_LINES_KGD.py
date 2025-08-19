# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_lines

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'kgd.dbo_tembo_icsaleentry', prune_days)
  icsaleentry = load_incr_dataset('kgd.dbo_tembo_icsaleentry', 'modifiedOn', cutoff_value)
else:
  icsaleentry = load_full_dataset('kgd.dbo_tembo_icsaleentry')
icsaleentry.createOrReplaceTempView('icsaleentry')

# COMMAND ----------

# SAMPLING
if sampling:
  icsaleentry = icsaleentry.limit(10)
  icsaleentry.createOrReplaceTempView('icsaleentry')  

# COMMAND ----------

# VALIDATE NK
valid_count_rows(icsaleentry, 'Invoicenumber,finvoiceLineNumber')

# COMMAND ----------

main = spark.sql("""
SELECT 
    NULL createdBy,
	icsaleentry.createdOn,
	NULL AS modifiedBy,
	icsaleentry.modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
    NULL AS accrualBogo,
    NULL AS accrualCoop,
    NULL AS accrualEndUserRebate,
    NULL AS accrualOthers,
    NULL AS accrualTpr,
    NULL AS accrualVolumeDiscount,
	NULL AS actualDeliveryOn,
	NULL AS actualShipDate,
    stdUom AS ansStdUom,
    stdUomQuantity / sellQty AS ansStdUomConv,
	Amount AS baseAmount,
	NULL AS bookedDate,
	'Y' AS bookedFlag,
	'N' AS cancelledFlag,
	NULL AS caseUomConv,
	NULL AS cetd,
	'001' AS client,
	NULL AS customerLineNumber,
	NULL AS customerPoNumber,
	NULL AS deliverNoteDate,
	NULL AS deliveryNoteId,
    'N' AS discountLineFlag,
    NULL AS distributionChannel,
	1 AS exchangeRate,
	icsale.dateInvoiced AS exchangeRateDate,
	NULL AS intransitTime,
    icsaleentry.WarehouseCode AS inventoryWarehouseId,
	CONCAT(icsaleentry.Invoicenumber,'-',icsaleentry.finvoiceLineNumber) AS invoiceDetailId, 
	icsaleentry.Invoicenumber AS invoiceId,
    icsaleentry.SellUOM AS invoiceUomCode,
    icsaleentry.Productcode AS itemId,
	0 AS legalEntityCost,
    0 AS legalEntityCostLeCurrency,
    'LINE' AS lineType,
	NULL AS lotNumber,
	NULL AS needByDate,
	NULL AS orderAmount,
	icsaleentry.Productcode AS orderedItem,
	NULL AS orderLineHoldType,
    NULL AS orderLineNumber,
	NULL AS orderLineStatus,
	Null AS orderNumber,
	NULL AS orderStatusDetail,
    NULL AS ordertypeId,
	SellUOM AS orderUomCode,
	'AS' AS owningBusinessUnitId,
    NULL AS priceListId,
    NULL AS priceListName,
	Unit_price AS pricePerUnit,
	NULL AS primaryUomCode,
	NULL AS primaryUomConv,
	productCode,
	NULL AS promiseDate,
	NULL AS promisedOnDate,
	NULL AS quantityBackordered,
	NULL AS quantityCancelled,
	SellQty AS quantityInvoiced,
	NULL AS quantityOrdered,
	NULL AS quantityReserved,
    NULL AS quantityReturned,
	SellQty AS quantityShipped,
	NULL AS requestDeliveryBy,
	NULL AS retd,
    NULL AS returnAmount,
    NULL AS returnFlag,
	seorder.FInterId AS salesOrderId,
	concat(seorder.FInterId, '-', icsaleentry.FOrderEntryID) AS salesorderDetailId,
	NULL AS scheduledShipDate,
	NULL AS sentDate943,
	NULL AS sequenceNumber,
    NULL AS settlementDiscountEarned,
    NULL AS settlementDiscountsCalculated,
    NULL AS settlementDiscountUnEarned,
	'' AS shipDate945,
	0 AS seeThruCost,
    0 AS seeThruCostLeCurrency,
    Concat('SHIP_TO-','-',icsale.CustomerId) AS shipToAddressId,
	NULL AS warehouseCode
FROM icsaleentry
  join kgd.dbo_tembo_icsale icsale on icsaleentry.finterID = icsale.FinterID
  join kgd.dbo_tembo_seorder seorder on icsaleentry.FOrderBillNo = seorder.CustomerPONumber

WHERE
  not icsaleentry._DELETED
  and not icsale._DELETED
  and not seorder._DELETED
""")

# main.cache()
# display(main)

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
full_keys_f = (
  spark.sql("""
    SELECT
      CONCAT(icsaleentry.Invoicenumber,'-',icsaleentry.finvoiceLineNumber) AS invoiceDetailId
    FROM kgd.dbo_tembo_icsaleentry icsaleentry
--       join kgd.dbo_tembo_icsale icsale on icsaleentry.finterID = icsale.FinterID
--       join kgd.dbo_tembo_seorder seorder on icsaleentry.FOrderBillNo = seorder.CustomerPONumber
    WHERE 
      not icsaleentry._DELETED
--       and not icsale._DELETED
--       and not seorder._DELETED
  """)
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

# UPDATE CUTOFF VALUE ', 'modifiedOn',
if not test_run:
  cutoff_value = get_incr_col_max_value(icsaleentry)
  update_cutoff_value(cutoff_value, table_name, 'kgd.dbo_tembo_icsaleentry')
  update_run_datetime(run_datetime, table_name, 'kgd.dbo_tembo_icsaleentry')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
