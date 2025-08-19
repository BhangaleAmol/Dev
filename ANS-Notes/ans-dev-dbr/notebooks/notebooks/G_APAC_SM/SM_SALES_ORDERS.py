# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_apac_sm.sm_sales_orders

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_apac_sm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'salesorderdetailid')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'sm_sales_orders')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/apac_sm/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT POINT_OF_SALES
#source_table = 's_supplychain.sales_order_headers_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, 's_supplychain.sales_order_headers_agg', prune_days)  
  sales_order_headers_agg = load_incremental_dataset('s_supplychain.sales_order_headers_agg', '_MODIFIED', cutoff_value)
  cutoff_value = get_cutoff_value(target_table, 's_supplychain.sales_order_lines_agg', prune_days)  
  sales_order_lines_agg = load_incremental_dataset('s_supplychain.sales_order_lines_agg', '_MODIFIED', cutoff_value)
else:
  sales_order_headers_agg = load_full_dataset('s_supplychain.sales_order_headers_agg')
  sales_order_lines_agg = load_full_dataset('s_supplychain.sales_order_lines_agg')

  
sales_order_headers_agg.createOrReplaceTempView('sales_order_headers_agg')
sales_order_lines_agg.createOrReplaceTempView('sales_order_lines_agg')


# COMMAND ----------

# SAMPLING
if sampling:
  sales_order_headers_agg = sales_order_headers_agg.limit(10)
  sales_order_headers_agg.createOrReplaceTempView('sales_order_headers_agg')
  
  sales_order_lines_agg = sales_order_lines_agg.limit(10)
  sales_order_lines_agg.createOrReplaceTempView('sales_order_lines_agg')


# COMMAND ----------

main = spark.sql("""
select
  ol.salesorderdetailid,
  ol._ID salesOrderDetail_ID,
  oh.orderDate,
  year(oh.orderDate) orderedYear,
  year(oh.orderDate) || ' / ' || month(oh.orderDate) OrderedMonth,
  int(date_format(oh.orderDate, 'yyyyMMdd')) orderDateId,
  ol.requestDeliveryBy,
  ol.scheduledShipDate,
  year(ol.requestDeliveryBy) requestYear,
  year(ol.requestDeliveryBy) || ' / ' || month(ol.requestDeliveryBy) RequestMonth,
  case
    when ol.orderLineStatus in ('CLOSED', 'CANCELLED') then ol.orderLineStatus
    when ol.orderLineHoldType is not null
    or oh.orderHoldType is not null then 'On Hold'
    else ol.orderLineStatus
  end orderLineStatus,
  case
    when ol.orderStatusDetail in ('Closed', 'Cancelled') then ol.orderStatusDetail
    when ol.orderLineHoldType is not null
    or oh.orderHoldType is not null then 'On Hold'
    else ol.orderStatusDetail
  end orderStatusDetail,
  orderLineStatus originalOrderLineStatus,
  oh.ordernumber,
  ol.customerPoNumber,
  ol.lineNumber,
  ol.sequenceNumber,
  ol.returnReasonCode,
  round(ol.returnAmount, 2) returnAmount,
  round(ol.returnAmount / oh.exchangeSpotRate, 2) returnAmountUsd,
  round(ol.quantityReturned * ol.primaryUomConv, 2) quantityReturned,
  round(ol.backOrderValue, 2) backOrderValue,
  round(ol.backOrderValue / oh.exchangeSpotRate, 2) backOrderValueUsd,
  round(ol.quantityBackOrdered, 2) quantityBackOrdered,
  round(ol.quantityOrdered, 2) quantityOrdered,
  round(ol.quantityReserved, 2) quantityReserved,
  round(ol.quantityCancelled, 2) quantityCancelled,
  round(ol.quantityReserved * ol.pricePerUnit, 2) reservedAmount,
 
  round(
    ol.quantityReserved * ol.pricePerUnit / exchangeSpotRate,
    2
  ) reservedAmountUsd,
  round(ol.quantityOrdered * ol.caseUomConv, 2) quantityOrderedCase,
  round(ol.quantityOrdered * ol.ansStdUomConv, 2) quantityOrderedAnsStduom,
  round(ol.orderAmount, 2) orderAmount,
  round(ol.quantityBackOrdered * ol.caseUomConv, 2) quantityBackOrderedCase,
  round(ol.quantityBackOrdered * ol.ansStdUomConv, 2) quantityBackOrderedAnsStdUom,
  ol.pricePerUnit,
  round(ol.orderAmount / oh.exchangeSpotRate, 2) orderAmountUsd,
  ol.apacScheduledShipDate,
  ol.orderUomCode,
  round(ol.pricePerUnit * ol.quantityCancelled, 2) cancelledAmount,
  round(
    ol.pricePerUnit * ol.quantityCancelled / oh.exchangeSpotRate,
    2
  ) cancelledAmountUsd,
  oh.transactionCurrencyId,
  oh.orderHeaderStatus,
  oh.customer_ID,
  oh.orderType_ID,
  oh.owningBusinessUnit_ID operatingUnit_ID,
  ol.inventoryWarehouse_ID inventoryUnit_ID,
  ol.item_id product_ID,
  oh.createdBy_ID headerCreatedBy,
  ol.createdBy_ID lineCreatedBy,
  oh.modifiedBy_ID headerModifiedBy,
  ol.modifiedBy_ID lineModifiedBy,
  oh.party_ID,
  oh.customer_ID account_ID,
  case
    when ol.deliveryNoteId is null then 0
    else round(ol.quantityOrdered, 2)
  end quantityPickSlip,
  case
    when ol.deliveryNoteId is null then 0
    else round(ol.quantityOrdered * ol.caseUomConv, 2)
  end quantityPickSlipCase,
  case
    when ol.deliveryNoteId is null then 0
    else round(ol.quantityOrdered * ol.ansStdUomConv, 2)
  end quantityPickSlipAnsStdUom,
  case
    when ol.deliveryNoteId is null then 0
    else round(ol.orderAmount, 2)
  end pickSlipAmount,
  case
    when ol.deliveryNoteId is null then 0
    else round(ol.orderAmount / oh.exchangeSpotRate, 2)
  end pickSlipAmountUsd,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate <> '' then round(ol.orderAmount, 2)
    else 0
  end notAllocatedScheduledAmount,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate <> '' then round(ol.orderAmount / oh.exchangeSpotRate, 2)
    else 0
  end notAllocatedScheduledAmountUsd,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate <> '' then round(ol.quantityOrdered, 2)
    else 0
  end quantityNotAllocatedScheduled,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate <> '' then round(ol.quantityOrdered * ol.caseUomConv, 2)
    else 0
  end quantityNotAllocatedScheduledCase,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate <> '' then round(ol.quantityOrdered * ol.ansStdUomConv, 2)
    else 0
  end quantityNotAllocatedScheduledAnsStdUom,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate = '' then round(ol.orderAmount, 2)
    else 0
  end notAllocatedNotScheduledAmount,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate = '' then round(ol.orderAmount / oh.exchangeSpotRate, 2)
    else 0
  end notAllocatedNotScheduledAmountUsd,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate = '' then round(ol.quantityOrdered, 2)
    else 0
  end quantityNotAllocatedNotScheduled,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate = '' then round(ol.quantityOrdered * ol.caseUomConv, 2)
    else 0
  end quantityNotAllocatedNotScheduledCase,
  case
    when ol.quantityReserved = 0
    and ol.deliveryNoteId is null
    and apacScheduledShipDate = '' then round(ol.quantityOrdered * ol.ansStdUomConv, 2)
    else 0
  end quantityNotAllocatedNotScheduledAnsStdUom,
  case
    when oh.orderHoldType in (
      'Ansell Credit Check Failure',
      'Credit Card Auth Failure',
      'Credit Check Failure'
    ) then 'Credit Hold'
  end creditHoldFlag,
  oh.orderHoldType,
  ol.orderLineHoldType,
  ol.supplierNumber,
  ol.supplierName,
  ol.shipToAddress_ID,
  oh.billToAddress_ID,
  ol.deliveryNoteId,
  ol.actualShipDate,
  oh.orderHoldDate1,
  oh.orderHoldDate2,
  ol.orderLineHoldDate1,
  ol.orderLineHoldDate2,
  ol.orderedItem,
  ol.quantityInvoiced,
  round(ol.quantityInvoiced * pricePerUnit , 2) invoicedAmount,
  oh.baseCurrencyId,
  oh.paymentTerm_ID,
  ol.itemIdentifierType,
  ol._modified,
  ol.primaryUomCode,
  ol.dropShipPoNumber
   
from
  s_supplychain.sales_order_headers_agg oh
  join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
  join s_core.transaction_type_agg ot on oh.orderType_ID = ot._ID
where
  oh._source = 'EBS'
  and not OL._DELETED
  and not OH._DELETED
  and not ot._DELETED
    """)
main.createOrReplaceTempView("main")

display(main)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("orderDate"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# VALIDATE DATA
if incremental:
  check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.sql("""Select salesorderdetailid
  from
  s_supplychain.sales_order_headers_agg oh
  join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
  join s_core.transaction_type_agg ot on oh.orderType_ID = ot._ID
where
  oh._source = 'EBS'
  and not OL._DELETED
  and not OH._DELETED
  and not ot._DELETED
 
  """)
  .select('salesorderdetailid')
  )

apply_soft_delete(full_keys_f, table_name, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_headers_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_headers_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_lines_agg')

