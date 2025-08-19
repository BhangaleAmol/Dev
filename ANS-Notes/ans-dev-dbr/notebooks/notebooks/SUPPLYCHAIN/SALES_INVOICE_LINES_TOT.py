# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_lines

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'tot.tb_pbi_invoices', prune_days)
  tb_pbi_invoices = load_incr_dataset('tot.tb_pbi_invoices', 'Invoice_date', cutoff_value)
  cutoff_value = get_cutoff_value(table_name, 'tb_pbi_returns', prune_days)
  tb_pbi_returns = load_incr_dataset('tb_pbi_returns', 'entry_date', cutoff_value)
else:  
  tb_pbi_invoices = load_full_dataset('tot.tb_pbi_invoices')
  tb_pbi_returns = load_full_dataset('tot.tb_pbi_returns')

tb_pbi_invoices.createOrReplaceTempView('tb_pbi_invoices')
tb_pbi_returns.createOrReplaceTempView('tb_pbi_returns')

# COMMAND ----------

# SAMPLING
if sampling:
  tb_pbi_invoices = tb_pbi_invoices.limit(10)
  tb_pbi_invoices.createOrReplaceTempView('tb_pbi_invoices')
  
  tb_pbi_returns = tb_pbi_returns.limit(10)
  tb_pbi_returns.createOrReplaceTempView('tb_pbi_returns')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(tb_pbi_invoices, 'Company,Invoice_number,Item')
valid_count_rows(tb_pbi_returns, 'Company,Invoice_number,Invoice_number_origin,Item_Origin,Product_cod')

# COMMAND ----------

main = spark.sql("""
SELECT 
    NULL createdBy,
	NULL AS createdOn,
	NULL AS modifiedBy,
	NULL AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
    0 AS accrualBogo,
    0 AS accrualCoop,
    0 AS accrualEndUserRebate,
    0 AS accrualOthers,
    0 AS accrualTpr,
    0 AS accrualVolumeDiscount,
	i.Invoice_date AS actualDeliveryOn,
	i.Invoice_date AS actualShipDate,
    o.UOM_Ansell AS ansStdUom,
	nvl(o.Order_qtty_conveted / o.Order_qtt, 1) AS ansStdUomConv,
	round(i.Invoice_unit_value * i.Qtty_invoice,2) AS baseAmount,
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
	i.Invoice_date AS exchangeRateDate,
	NULL AS intransitTime,
    case 
      when i.Company = '5210' 
        then '520-ABL'
      when i.Company = '5220'
        then '521-HER'
      else 
        i.company
    end AS inventoryWarehouseId,
	CONCAT(i.Company,'-',i.Invoice_number,'-',i.Item) AS invoiceDetailId, 
	CONCAT(i.Company,'-',i.Invoice_number) AS invoiceId,
    i.uom AS invoiceUomCode,
    CONCAT(i.Company, '-', i.Product_cod) AS itemId,
	i.COGS AS legalEntityCost,
    i.COGS AS legalEntityCostLeCurrency,
    'LINE' AS lineType,
	i.Item AS lotNumber,
	NULL AS needByDate,
	nvl(o.net_value ,0) AS orderAmount,
	NULL AS orderedItem,
	NULL AS orderLineHoldType,
    NULL AS orderLineNumber,
	NULL AS orderLineStatus,
	i.Order_number AS orderNumber,
	NULL AS orderStatusDetail,
    NULL AS ordertypeId,
	nvl(o.UOM, i.UOM) AS orderUomCode,
	i.Company AS owningBusinessUnitId,
    NULL AS priceListId,
    NULL AS priceListName,
	ROUND(i.Invoice_unit_value, 2) AS pricePerUnit,
	NULL AS primaryUomCode,
	NULL AS primaryUomConv,
	i.Product_cod AS productCode,
	NULL AS promiseDate,
	NULL AS promisedOnDate,
	NULL AS quantityBackordered,
	NULL AS quantityCancelled,
	i.Qtty_invoice AS quantityInvoiced,
	nvl(o.order_qtt,0) AS quantityOrdered,
	NULL AS quantityReserved,
    NULL AS quantityReturned,
	i.Qtty_invoice AS quantityShipped,
	o.Request_date AS requestDeliveryBy,
	NULL AS retd,
    0 AS returnAmount,
    NULL AS returnFlag,
	CONCAT(i.Company,'-',i.Order_number,'-',i.order_item) AS salesorderDetailId,
	CONCAT(i.Company,'-',i.Order_number) AS salesOrderId,
    NULL AS scheduledShipDate,
	NULL AS sentDate943,
	1 AS sequenceNumber,
    0 AS settlementDiscountEarned,
    0 AS settlementDiscountsCalculated,
    0 AS settlementDiscountUnEarned,
	'' AS shipDate945,
	i.COGS AS seeThruCost,
    i.COGS AS seeThruCostLeCurrency,
    Concat('SHIP_TO-',i.company,'-',i.Customer_delivery) AS shipToAddressId,
	NULL AS warehouseCode
FROM tb_pbi_invoices i
  left join tot.tb_pbi_orders o on i.company = o.company and i.Order_number = o.Order_number and i.order_item = o.item

UNION

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
	NULL AS actualShipDate,
    NULL AS ansStdUom,
	NULL AS ansStdUomConv,
	round(r.Return_LC,2) AS baseAmount,
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
	r.Date_issue AS exchangeRateDate,
	NULL AS intransitTime,
	case 
      when r.Company = '5210' 
        then '520-ABL'
      when r.Company = '5220'
        then '521-HER'
      else 
        r.company
    end AS inventoryWarehouseId,
	CONCAT(r.company,'-',r.Invoice_number,'-',r.Invoice_number_origin,'-',r.Item_Origin,'-', r.Product_cod) AS invoiceDetailId, 
	CONCAT (r.Company,'-',r.Invoice_number,'-',r.Invoice_number_origin) AS invoiceId,
    NULL AS invoiceUomCode,
    CONCAT(r.Company, '-', r.Product_cod) AS itemId,
	r.COGS * (-1) AS legalEntityCost,
    r.COGS * (-1) AS legalEntityCostLeCurrency,
    'LINE' AS lineType,
	r.Item_origin AS lotNumber,
	NULL AS needByDate,
	NULL AS orderAmount,
	NULL AS orderedItem,
	NULL AS orderLineHoldType,
    NULL AS orderLineNumber,
	NULL AS orderLineStatus,
	i.Order_number AS orderNumber,
	NULL AS orderStatusDetail,
	i.UOM AS orderUomCode,
	r.Company AS owningBusinessUnitId,
    NULL AS priceListId,
    NULL AS priceListName,
	i.Invoice_unit_value AS pricePerUnit,
	NULL AS primaryUomCode,
	NULL AS primaryUomConv,
	r.Product_cod AS productCode,
	NULL AS promiseDate,
	NULL AS promisedOnDate,
	NULL AS quantityBackordered,
	NULL AS quantityCancelled,
	r.Invoice_qtty * (-1) AS quantityInvoiced,
	NULL AS quantityOrdered,
	NULL AS quantityReserved,
    NULL AS quantityReturned,
	NULL AS quantityShipped,
	NULL AS requestDeliveryBy,
	NULL AS retd,
    NULL AS returnAmount,
    NULL AS returnFlag,
	CONCAT(i.Company,'-',i.Order_number) AS salesOrderId,
	CONCAT(i.Company,'-',i.Order_number,'-',i.Item) AS salesorderDetailId,
	NULL AS scheduledShipDate,
	NULL AS sentDate943,
	NULL AS sequenceNumber,
    NULL AS settlementDiscountEarned,
    NULL AS settlementDiscountsCalculated,
    NULL AS settlementDiscountUnEarned,
	'' AS shipDate945,
	r.COGS * (-1) AS seeThruCost,
    r.COGS * (-1) AS seeThruCostLeCurrency,
    Concat('SHIP_TO-',i.company,'-',i.Customer_delivery) AS shipToAddressId,
	NULL AS warehouseCode,
    NULL AS ordertypeId
FROM tb_pbi_returns r
LEFT JOIN tot.tb_pbi_invoices i ON r.Invoice_number_origin = i.Invoice_number AND r.Item_Origin = i.Item AND r.Company = i.Company
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
full_inv_keys = (
  spark.table('tot.tb_pbi_invoices')
  .filter('_DELETED IS FALSE')
  .selectExpr("CONCAT(Company,'-',Invoice_number,'-',Item) AS invoiceDetailId")
  .select('invoiceDetailId')
)

full_ret_keys = (
  spark.table('tot.tb_pbi_returns')
  .filter('_DELETED IS FALSE')
  .selectExpr("CONCAT(company,'-',Invoice_number,'-',Invoice_number_origin,'-',Item_Origin,'-', Product_cod) AS invoiceDetailId")
  .select('invoiceDetailId')
)

full_keys_f = (
  full_inv_keys
  .union(full_ret_keys)
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
  cutoff_value = get_incr_col_max_value(tb_pbi_invoices)
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_invoices')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_invoices')

  cutoff_value = get_incr_col_max_value(tb_pbi_returns)
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_returns')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_returns')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
