# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.tm_sales_invoice_v

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = False
key_columns = get_input_param('key_columns', 'list', default_value = ['invoiceNumber'])
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'tm_sales_invoice_v')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/na_tm/full_data')

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

# EXTRACT
source_table = 's_supplychain.sales_invoice_headers_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_invoice_headers_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_invoice_headers_agg = load_full_dataset(source_table)
  
sales_invoice_headers_agg.createOrReplaceTempView('sales_invoice_headers_agg')
sales_invoice_headers_agg.display()

# COMMAND ----------

# SAMPLING
if sampling:
  sales_invoice_headers_agg = sales_invoice_headers_agg.limit(10)
  sales_invoice_headers_agg.createOrReplaceTempView('sales_invoice_headers_agg')

# COMMAND ----------

main_df = spark.sql("""
select
  invh.owningBusinessUnit_ID,
  invh.customer_ID,
  invh.party_ID,
  invh.shiptoAddress_ID,
  invh.dateInvoiced,
  invh.invoiceNumber,
  sum(invl.baseAmount) baseAmount,
  sum(invl.baseAmount / invh.exchangeRate) baseAmountLE,
  round(sum(invl.quantityShipped * caseUomConv),0) quantityShippedCase,
  invl.customerPONumber,
  invh.baseCurrencyId,
  invl.item_Id AS Product_ID
from
  s_supplychain.sales_invoice_headers_agg invh
  join s_supplychain.sales_invoice_lines_agg invl on invh._ID = invl.invoice_ID
where
  not invh._deleted
  and not invl._deleted
  and year(invh.dateInvoiced) >= 2019
  and invh._source = 'EBS'
  and invl._source = 'EBS'
  and invl.baseamount <> 0
group by
  invh.owningBusinessUnit_ID,
  invh.party_ID,
  invh.customer_ID,
  invh.billtoAddress_ID,
  invh.shiptoAddress_ID,
  invh.dateInvoiced,
  invh.invoiceNumber,
  invl.customerPONumber,
  invh.baseCurrencyId,
  invl.item_Id
""")

main_df.cache()
main_df.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("dateInvoiced"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)
main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
