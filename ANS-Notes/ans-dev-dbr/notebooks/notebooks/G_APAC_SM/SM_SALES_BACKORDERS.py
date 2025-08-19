# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_apac_sm.sm_sales_backorders

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_apac_sm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'salesOrderDetailId') 
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'sm_sales_backorders')
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
  oh.orderDate,
  int(date_format(oh.orderDate, 'yyyyMMdd') ) orderDateId,
  ol.requestDeliveryBy,
  case when ol.orderLineStatus in ('CLOSED', 'CANCELLED')
                      then ol.orderLineStatus
            when ol.orderLineHoldType is not null or oh.orderHoldType is not null
                      then 'On Hold'
            else ol.orderLineStatus
  end  orderLineStatus,
  case when ol.orderStatusDetail in ('Closed', 'Cancelled')
                      then ol.orderStatusDetail
            when ol.orderLineHoldType is not null or oh.orderHoldType is not null
                      then 'On Hold'
            else ol.orderStatusDetail
  end  orderStatusDetail,
  oh.ordernumber,
  ol.lineNumber,
  ol.returnReasonCode ,
  ol.returnAmount,
  round(ol.returnAmount / exchangeSpotRate,2) returnAmountUsd,
  round(ol.quantityReturned *  ol.primaryUomConv,2) quantityReturned,
  ol.backOrderValue,
  round(ol.backOrderValue / oh.exchangeSpotRate,2) backOrderValueUsd,
  ol.quantityBackOrdered,
  round(ol.quantityBackOrdered * ol.caseUomConv, 2) quantityBackOrderedCase,
  round(ol.quantityBackOrdered * ol.ansStdUomConv, 2) quantityBackOrderedAnsStdUom,
  oh.customer_ID,
  oh.orderType_ID,
  oh.owningBusinessUnit_ID operatingUnit_ID,
  ol.inventoryWarehouse_ID inventoryUnit_ID,
  ol.item_id product_ID,
  oh.createdBy_ID,
  oh.party_ID,
  oh.customer_ID account_ID,
  oh.billtoAddress_ID,
  oh.shipToAddress_ID,
  ol.item_ID,
  ol.salesOrderDetailId
from
  s_supplychain.sales_order_headers_agg oh
  join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
  join s_core.TRANSACTION_TYPE_AGG ot on oh.orderType_ID = ot._ID
  join s_core.account_agg ac on oh.customer_ID = ac._ID
where oh._source = 'EBS'
and ol.bookedFlag = 'Y'
and not OL._DELETED
and not OH._DELETED
and not ot._DELETED
and ol.backOrderValue <> 0
and ol.requestDeliveryBy < current_date
and ol.orderStatusDetail not in ('Cancelled', 'Order Cancelled', 'Pending pre-billing acceptance', 'Closed')
and ac.customerType = 'External'
and ol.orderLineHoldType is  null -- tbd whether orders on hold should be incluced
and oh.orderHoldType is  null -- tbd whether orders on hold should be incluced
and ot.name in ('ASIA_Broken Case Order', 'ASIA_Broken Case Order Line', 'ASIA_Donation Order', 'ASIA_Donation Order Line', 'ASIA_Drop Shipment Order', 'ASIA_Drop Shipment Order Line', 'ASIA_Internal Line', 'ASIA_Internal Order', 'ASIA_Manual PriceAllowed Line', 'ASIA_Manual PriceAllowed Order', 'ASIA_Prepayment Order', 'ASIA_Prepayment Order Line', 'ASIA_Price related CN', 'ASIA_Price related DN', 'ASIA_Price related_CN Line', 'ASIA_Price related_CN Line1', 'ASIA_Price related_DN Line', 'ASIA_Return CN_No Inv Line', 'ASIA_Return CN_No Inv Line1', 'ASIA_Return DN_No Inv Line', 'ASIA_Return Order', 'ASIA_Return Order CN_No Inv', 'ASIA_Return Order DN_No Inv', 'ASIA_Return Order Line', 'ASIA_Return Order Line1', 'ASIA_Sample Drop Ship Order', 'ASIA_Sample Drop Shipment Line', 'ASIA_Sample Order Line', 'ASIA_Samples Order', 'ASIA_Service Order CN', 'ASIA_Service Order DN', 'ASIA_Service Order_CN Line', 'ASIA_Service Order_CN Line1', 'ASIA_Service Order_DN Line', 'ASIA_Standard Order', 'ASIA_Standard Order Line', 'IN_PP_Standard_Order', 'IN_PP_Drop Shipment Order')
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
  spark.sql("""Select salesOrderDetailId from
  s_supplychain.sales_order_headers_agg oh
  join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
  join s_core.TRANSACTION_TYPE_AGG ot on oh.orderType_ID = ot._ID
  join s_core.account_agg ac on oh.customer_ID = ac._ID
where oh._source = 'EBS'
and ol.bookedFlag = 'Y'
and not OL._DELETED
and not OH._DELETED
and not ot._DELETED
and ol.backOrderValue <> 0
and ol.requestDeliveryBy < current_date
and ol.orderStatusDetail not in ('Cancelled', 'Order Cancelled', 'Pending pre-billing acceptance', 'Closed')
and ac.customerType = 'External'
and ol.orderLineHoldType is  null -- tbd whether orders on hold should be incluced
and oh.orderHoldType is  null -- tbd whether orders on hold should be incluced
and ot.name in ('ASIA_Broken Case Order', 'ASIA_Broken Case Order Line', 'ASIA_Donation Order', 'ASIA_Donation Order Line', 'ASIA_Drop Shipment Order', 'ASIA_Drop Shipment Order Line', 'ASIA_Internal Line', 'ASIA_Internal Order', 'ASIA_Manual PriceAllowed Line', 'ASIA_Manual PriceAllowed Order', 'ASIA_Prepayment Order', 'ASIA_Prepayment Order Line', 'ASIA_Price related CN', 'ASIA_Price related DN', 'ASIA_Price related_CN Line', 'ASIA_Price related_CN Line1', 'ASIA_Price related_DN Line', 'ASIA_Return CN_No Inv Line', 'ASIA_Return CN_No Inv Line1', 'ASIA_Return DN_No Inv Line', 'ASIA_Return Order', 'ASIA_Return Order CN_No Inv', 'ASIA_Return Order DN_No Inv', 'ASIA_Return Order Line', 'ASIA_Return Order Line1', 'ASIA_Sample Drop Ship Order', 'ASIA_Sample Drop Shipment Line', 'ASIA_Sample Order Line', 'ASIA_Samples Order', 'ASIA_Service Order CN', 'ASIA_Service Order DN', 'ASIA_Service Order_CN Line', 'ASIA_Service Order_CN Line1', 'ASIA_Service Order_DN Line', 'ASIA_Standard Order', 'ASIA_Standard Order Line', 'IN_PP_Standard_Order', 'IN_PP_Drop Shipment Order')""")
  .select('salesOrderDetailId')
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


