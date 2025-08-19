# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_apac_sm.sm_sales_invoices

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_apac_sm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'invoiceDetailId') 
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'sm_sales_invoices')
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
  cutoff_value = get_cutoff_value(target_table, 's_supplychain.sales_invoice_headers_agg', prune_days)  
  sales_invoice_headers_agg = load_incremental_dataset('s_supplychain.sales_invoice_headers_agg', '_MODIFIED', cutoff_value)
  cutoff_value = get_cutoff_value(target_table, 's_supplychain.sales_invoice_lines_agg', prune_days)  
  sales_invoice_lines_agg = load_incremental_dataset('s_supplychain.sales_invoice_lines_agg', '_MODIFIED', cutoff_value)
else:
  sales_invoice_headers_agg = load_full_dataset('s_supplychain.sales_invoice_headers_agg')
  sales_invoice_lines_agg = load_full_dataset('s_supplychain.sales_invoice_lines_agg')
  
  
sales_invoice_headers_agg.createOrReplaceTempView('sales_invoice_headers_agg')
sales_invoice_lines_agg.createOrReplaceTempView('sales_invoice_lines_agg')


# COMMAND ----------

# SAMPLING
if sampling:
  sales_invoice_headers_agg = sales_invoice_headers_agg.limit(10)
  sales_invoice_headers_agg.createOrReplaceTempView('sales_invoice_headers_agg')
  
  sales_invoice_lines_agg = sales_invoice_lines_agg.limit(10)
  sales_invoice_lines_agg.createOrReplaceTempView('sales_invoice_lines_agg')
  

# COMMAND ----------

main = spark.sql("""
select
  invh.owningBusinessUnit_ID,
  invh.customer_ID,
  invl.item_ID,
  invh.party_ID,
  invh.billtoAddress_ID,
  invh.shiptoAddress_ID,
  invh.transaction_ID,
  invl.orderNumber,
  invh.dateInvoiced,
  year(invh.dateInvoiced) yearInvoiced,
  year(invh.dateInvoiced) || ' / ' || month(invh.dateInvoiced) yearMonthInvoiced,
  invh.invoiceNumber,
  round(sum(invl.baseAmount),2) baseAmount,
  round(sum(invl.baseAmount * invh.exchangeRate),2) baseAmountLE,
  round(sum(invl.baseAmount / invh.exchangeSpotRate),2) baseAmountUsd,
  round(sum(invl.baseAmount + invl.returnAmount),2) grossAmount,
  round(sum((invl.baseAmount + invl.returnAmount) * invh.exchangeRate),2) grossAmountLE,
  round(sum((invl.baseAmount + invl.returnAmount) / invh.exchangeSpotRate),2) grossAmountUsd,
  round(sum(invl.quantityShipped * invl.caseUomConv),0) quantityShippedCase,
  round(sum(invl.quantityShipped * invl.ansStdUomConv),0) quantityShippedAnsStdUom,
  round(sum(invl.returnAmount),2) returnAmount,
  round(sum(invl.returnAmount * invh.exchangeRate),2) returnAmountLE,
  round(sum(invl.returnAmount / invh.exchangeSpotRate),2) returnAmountUsd,
  round(sum(invl.quantityInvoiced),2) quantityInvoiced ,
  round(sum(invl.quantityInvoiced * invl.caseUomConv),2) quantityInvoicedCase ,
  round(sum(invl.quantityInvoiced * invl.ansStdUomConv),2) quantityInvoicedAnsStdUom ,
  invl.customerPONumber,
  invh.baseCurrencyId,
  invh.transactionCurrencyId,
  invl.inventoryWarehouse_ID,
  invh.owningBusinessUnit_ID operatingUnit_ID,
  invl.salesorderdetail_ID,
  invl.salesOrderDetailid,
  invl.invoiceDetailId
  
from
  s_supplychain.sales_invoice_headers_agg invh
  join s_supplychain.sales_invoice_lines_agg invl on invh._ID = invl.invoice_ID
  join s_core.account_agg ac on invh.customer_ID = ac._ID
where
  not invh._deleted
  and not invl._deleted
  and invh._source = 'EBS'
  and invl._source = 'EBS'
  and ac.customerType = 'External'
  and salesOrderDetailid is not null
group by
  invh.owningBusinessUnit_ID,
  invh.customer_ID,
  invh.billtoAddress_ID,
  invh.shiptoAddress_ID,
  invh.dateInvoiced,
  invh.invoiceNumber,
  invl.customerPONumber,
  invh.baseCurrencyId,
  invl.item_ID,
  invh.party_ID,
  invh.transaction_ID,
  invh.transactionCurrencyId,
  invl.orderNumber,
  invl.inventoryWarehouse_ID,
  invl.salesorderdetail_ID,
  invl.salesOrderDetailid,
  invl.invoiceDetailId
    """)
main.createOrReplaceTempView("main")

display(main)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("dateInvoiced"))
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
  spark.sql("""Select invoiceDetailId
  from
  s_supplychain.sales_invoice_headers_agg invh
  join s_supplychain.sales_invoice_lines_agg invl on invh._ID = invl.invoice_ID
  join s_core.account_agg ac on invh.customer_ID = ac._ID
where
  not invh._deleted
  and not invl._deleted
  and invh._source = 'EBS'
  and invl._source = 'EBS'
  and ac.customerType = 'External'
  and salesOrderDetailid is not null
  """)
  .select('invoiceDetailId')
  )

apply_soft_delete(full_keys_f, table_name, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_headers_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_headers_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_lines_agg')


