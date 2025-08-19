# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_lines

# COMMAND ----------

# LOAD DATASETS
vw_qv_invoices = load_full_dataset('col.vw_qv_invoices')
vw_qv_invoices.createOrReplaceTempView('vw_qv_invoices')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_qv_invoices = vw_qv_invoices.limit(10)
  vw_qv_invoices.createOrReplaceTempView('vw_qv_invoices')

# COMMAND ----------

# VALIDATE NK - DISTINCT COUNT
valid_count_rows(vw_qv_invoices, ['Invoice_Number', 'order_Line_Number'])

# COMMAND ----------

main = spark.sql("""
SELECT 
    NULL createdBy,
	NULL AS createdOn,
	NULL AS modifiedBy,
	NULL AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
    NULL AS accrualBogo,
    NULL AS accrualCoop,
    NULL AS accrualEndUserRebate,
    NULL AS accrualOthers,
    NULL AS accrualTpr,
    NULL AS accrualVolumeDiscount,
	NULL AS actualDeliveryOn,
	i.Ship_Date AS actualShipDate,
    NULL AS ansStdUom,
	1 AS ansStdUomConv,
	i.Net_Sales_LC AS baseAmount,
	NULL AS bookedDate,
	NULL AS bookedFlag,
	NULL AS cancelledFlag,
	NULL AS caseUomConv,
	NULL AS cetd,
	'001' AS client,
	NULL AS customerLineNumber,
	NULL AS customerPoNumber,
	NULL AS deliverNoteDate,
	NULL AS deliveryNoteId,
    NULL AS discountLineFlag,
    NULL AS distributionChannel,
	1 AS exchangeRate,
	i.Invoice_Date AS exchangeRateDate,
	NULL AS intransitTime,
	i.Stock_ID AS inventoryWarehouseId,
	i.Invoice_Number || '-' || i.Order_Line_Number AS invoiceDetailId,
	i.Invoice_Number AS invoiceId,
    NULL AS invoiceUomCode,
	i.company || '-' || i.Item_Number AS itemId,
	i.Net_Sales_LC AS legalEntityCost,
    i.Net_Sales_LC AS legalEntityCostLeCurrency,
    'LINE' AS lineType,
	i.order_Line_Number AS lotNumber,
	NULL AS needByDate,
	NULL AS orderAmount,
	i.Item_Number AS orderedItem,
	NULL AS orderLineHoldType,
    NULL AS orderLineNumber,
	NULL AS orderLineStatus,
	i.Order_Number AS orderNumber,
	NULL AS orderStatusDetail,
    NULL AS ordertypeId,
	i.Ansell_Std_U_M AS orderUomCode,
	i.Company AS owningBusinessUnitId,
    NULL AS priceListId,
    NULL AS priceListName,
	ROUND(i.Net_Sales_LC / i.sales_quantity, 2) AS pricePerUnit,
	NULL AS primaryUomCode,
	NULL AS primaryUomConv,
	i.Item_Number AS productCode,
	NULL AS promiseDate,
	NULL AS promisedOnDate,
	NULL AS quantityBackordered,
	NULL AS quantityCancelled,
	i.sales_quantity AS quantityInvoiced,
	NULL AS quantityOrdered,
	NULL AS quantityReserved,
    NULL AS quantityReturned,
	i.sales_quantity AS quantityShipped,
	NULL AS requestDeliveryBy,
	NULL AS retd,
    NULL AS returnAmount,
    NULL AS returnFlag,
	i.Order_Number AS salesOrderId,
	i.Invoice_Number || '-' || i.Order_Number || '-' || i.Order_Line_Number AS salesOrderDetailId,
	NULL AS scheduledShipDate,
	NULL AS sentDate943,
	NULL AS sequenceNumber,
    NULL AS settlementDiscountEarned,
    NULL AS settlementDiscountsCalculated,
    NULL AS settlementDiscountUnEarned,
	NULL AS shipDate945,
	i.Sales_Cost_Amount_LC__net_see_thru_ AS seeThruCost,
    i.Sales_Cost_Amount_LC__net_see_thru_ AS seeThruCostLeCurrency,
    COncat('SHIP_TO-',i.ShipToDeliveryLocationID) AS shipToAddressId,
	i.ShipToDeliveryLocationID AS warehouseCode
FROM vw_qv_invoices i
""")

main.cache()
display(main)

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
  spark.table('col.vw_qv_invoices')
  .filter('_DELETED IS FALSE')
  .selectExpr("Invoice_Number || '-' || Order_Line_Number AS invoiceDetailId")
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
  cutoff_value = get_incr_col_max_value(vw_qv_invoices)
  update_cutoff_value(cutoff_value, table_name, 'col.vw_qv_invoices')
  update_run_datetime(run_datetime, table_name, 'col.vw_qv_invoices')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
