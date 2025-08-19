# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_apac_sm.sm_sales_shipping

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_apac_sm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'deliveryDetailId') 
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'sm_sales_shipping')
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
  cutoff_value = get_cutoff_value(target_table, 's_supplychain.sales_shipping_lines_agg', prune_days)  
  sales_shipping_lines_agg = load_incremental_dataset('s_supplychain.sales_shipping_lines_agg', '_MODIFIED', cutoff_value)
  cutoff_value = get_cutoff_value(target_table, 's_supplychain.sales_order_headers_agg', prune_days)  
  sales_order_headers_agg = load_incremental_dataset('s_supplychain.sales_order_headers_agg', '_MODIFIED', cutoff_value)
  cutoff_value = get_cutoff_value(target_table, 's_supplychain.sales_order_lines_agg', prune_days)  
  sales_order_lines_agg = load_incremental_dataset('s_supplychain.sales_order_lines_agg', '_MODIFIED', cutoff_value)
else:
  sales_shipping_lines_agg = load_full_dataset('s_supplychain.sales_shipping_lines_agg')
  sales_order_headers_agg = load_full_dataset('s_supplychain.sales_order_headers_agg')
  sales_order_lines_agg = load_full_dataset('s_supplychain.sales_order_lines_agg')
  
sales_shipping_lines_agg.createOrReplaceTempView('sales_shipping_lines_agg')
sales_order_headers_agg.createOrReplaceTempView('sales_order_headers_agg')
sales_order_lines_agg.createOrReplaceTempView('sales_order_lines_agg')

# COMMAND ----------

# SAMPLING
if sampling:
  sales_shipping_lines_agg = sales_shipping_lines_agg.limit(10)
  sales_shipping_lines_agg.createOrReplaceTempView('sales_shipping_lines_agg')
  
  sales_order_headers_agg = sales_order_headers_agg.limit(10)
  sales_order_headers_agg.createOrReplaceTempView('sales_order_headers_agg')
  
  sales_order_lines_agg = sales_order_lines_agg.limit(10)
  sales_order_lines_agg.createOrReplaceTempView('sales_order_lines_agg')

# COMMAND ----------

main = spark.sql("""
select
  sales_order_headers_agg.ordernumber,
  sales_order_lines_agg.salesorderdetailid,
  sales_order_lines_agg.promisedOnDate,
  sales_order_lines_agg.requestDeliveryBy,
  sales_shipping_lines_agg.actualShipDate,
  sales_shipping_lines_agg.actualPickDate,
  int(date_format(sales_shipping_lines_agg.actualPickDate, 'yyyyMMdd')) pickDateId,
  sales_order_lines_agg.apacScheduledShipDate,
  sales_order_headers_agg.orderDate,
  sales_shipping_lines_agg.lineNumber,
  sales_shipping_lines_agg.sequenceNumber,
  sales_shipping_lines_agg.lotCount,
  sales_shipping_lines_agg.quantityOrdered * sales_shipping_lines_agg.primaryUomConv quantityOrderedPrimaryUom,
  sales_shipping_lines_agg.quantityShipped * sales_shipping_lines_agg.primaryUomConv quantityShippedPrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime1 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime1PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime2 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime2PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime3 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime3PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime4 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime4PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime5 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime5PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime6 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime6PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime7 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime7PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime8 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime8PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime9 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime9PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntime10 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime10PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate1 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate1PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate2 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate2PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate3 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate3PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate4 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate4PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate5 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate5PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate6 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate6PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate7 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate7PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate8 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate8PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate9 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate9PrimaryUom,
  sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate10 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate10PrimaryUom,
  sales_shipping_lines_agg.shippedOntime1Indicator,
  sales_shipping_lines_agg.shippedOntime2Indicator,
  sales_shipping_lines_agg.shippedOntime3Indicator,
  sales_shipping_lines_agg.shippedOntime4Indicator,
  sales_shipping_lines_agg.shippedOntime5Indicator,
  sales_shipping_lines_agg.shippedOntime6Indicator,
  sales_shipping_lines_agg.shippedOntime7Indicator,
  sales_shipping_lines_agg.shippedOntime8Indicator,
  sales_shipping_lines_agg.shippedOntime9Indicator,
  sales_shipping_lines_agg.shippedOntime10Indicator,
  sales_shipping_lines_agg.shippedOntime1IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime2IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime3IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime4IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime5IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime6IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime7IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime8IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime9IndicatorPromisedDate,
  sales_shipping_lines_agg.shippedOntime10IndicatorPromisedDate,
  sales_shipping_lines_agg.primaryUomConv,
  sales_shipping_lines_agg.inventoryWarehouse_ID,
  s_supplychain.sales_order_headers_agg.owningBusinessUnit_ID,
  sales_order_headers_agg.shipToAddress_ID,
  sales_order_headers_agg.orderType_ID,
  sales_order_headers_agg.customer_ID,
  sales_order_lines_agg.item_ID,
  sales_shipping_lines_agg.deliveryDetailId

from s_supplychain.sales_shipping_lines_agg
join s_supplychain.sales_order_headers_agg on sales_shipping_lines_agg.salesorder_id = sales_order_headers_agg._ID
join s_supplychain.sales_order_lines_agg on sales_shipping_lines_agg.salesOrderDetail_ID = sales_order_lines_agg._ID
where
  sales_order_headers_agg._source = 'EBS'
  and sales_shipping_lines_agg._source = 'EBS'
  and sales_order_lines_agg._source = 'EBS'
  and not sales_order_headers_agg._deleted
  and not sales_shipping_lines_agg._deleted
  and not sales_order_lines_agg._deleted
    """)
main.createOrReplaceTempView("main")

display(main)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("actualShipDate"))
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
  spark.sql("""Select deliveryDetailId
  from s_supplychain.sales_shipping_lines_agg
  join s_supplychain.sales_order_headers_agg on sales_shipping_lines_agg.salesorder_id = sales_order_headers_agg._ID
  join s_supplychain.sales_order_lines_agg on sales_shipping_lines_agg.salesOrderDetail_ID = sales_order_lines_agg._ID
where
  sales_order_headers_agg._source = 'EBS'
  and sales_shipping_lines_agg._source = 'EBS'
  and sales_order_lines_agg._source = 'EBS'
  and not sales_order_headers_agg._deleted
  and not sales_shipping_lines_agg._deleted
  and not sales_order_lines_agg._deleted
  """)
  .select('deliveryDetailId')
  )

apply_soft_delete(full_keys_f, table_name, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_shipping_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_shipping_lines_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_headers_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_headers_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_lines_agg')
  
