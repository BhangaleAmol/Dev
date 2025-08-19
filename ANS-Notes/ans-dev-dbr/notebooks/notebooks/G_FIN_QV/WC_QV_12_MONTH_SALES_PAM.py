# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_12_month_sales_pam

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'INVOICE_MONTH,ACCOUNT_NUMBER,PRODUCT_NUMBER')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_12_month_sales_pam')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/fin_qv/full_data')

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
source_table = 's_supplychain.sales_invoice_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_invoice_lines_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_invoice_lines_agg = load_full_dataset(source_table)
  
sales_invoice_lines_agg.createOrReplaceTempView('sales_invoice_lines_agg')
sales_invoice_lines_agg.display()

# COMMAND ----------

main = spark.sql("""
SELECT ROW_NUMBER,
INVOICE_MONTH,CUSTOMER_NAME,ACCOUNT_NUMBER,PRODUCT_NUMBER,PRODUCT_NAME,
NET_UNITS,RETURNS,NET_AMT_USD, RETURN_AMT_USD
FROM (
select
  row_number() over (order by date_format(monthenddate,'yyyy/MM/01'),account.accountNumber,product.productcode ) ROW_NUMBER,
  date_format(monthenddate,'yyyy/MM/01') INVOICE_MONTH,
  account.name CUSTOMER_NAME,  
  account.accountNumber ACCOUNT_NUMBER,
  product.productcode PRODUCT_NUMBER,
  product.name PRODUCT_NAME,
  SUM(
    
      sales_invoice_lines.quantityInvoiced * sales_invoice_lines.ansStdUomConv * (
      case
        when product.ansStdUom = 'PR' then 2
        else 1
      end
    )
  ) NET_UNITS,
  sum(
    sales_invoice_lines.quantityReturned * sales_invoice_lines.ansStdUomConv * (
      case
        when product.ansStdUom = 'PR' then 2
        else 1
      end
    )
  ) RETURNS,
  round(
    sum(
      (
        CASE
          WHEN sales_invoice_lines.discountLineFlag = 'N'
          AND NOT tt_invh.transactiontypecode in ('CM', 'Credit Memo') THEN sales_invoice_lines.baseAmount
          ELSE 0
        END +(
          case
            when sales_invoice_lines.returnflag = 'Y'
            and sales_invoice_lines.discountLineFlag = 'N'
            and tt_invh.transactiontypecode in ('CM', 'Credit Memo') then sales_invoice_lines.baseAmount
            else 0
          end
        )
      ) / sales_invoice_headers.exchangeSpotRate
    ),
    2
  ) NET_AMT_USD,
  round(
    sum(
      sales_invoice_lines.returnAmount / sales_invoice_headers.exchangeSpotRate
    ),
    2
  )  RETURN_AMT_USD 
from
  s_supplychain.sales_invoice_lines_agg sales_invoice_lines
  join s_supplychain.sales_invoice_headers_agg sales_invoice_headers on sales_invoice_lines.invoice_ID = sales_invoice_headers._ID
  join s_core.account_agg account on sales_invoice_headers.customer_ID = account._ID
  join s_core.product_agg product on sales_invoice_lines.item_ID = product._ID
  join s_core.date date on date_format(sales_invoice_headers.dateInvoiced, 'yyyyMMdd') = date.dayid
  left join s_core.transaction_type_agg tt_invh on sales_invoice_headers.transaction_ID = tt_invh._id
  left join s_core.transaction_type_agg tt_invl on sales_invoice_lines.orderType_ID = tt_invl._id
where
  sales_invoice_headers._SOURCE = 'EBS'
  and sales_invoice_lines._SOURCE = 'EBS'
  and account.customerType = 'External'
  and product.productCode <> 'unknown'
  and product.itemtype not in ('EXPENSE')
  and date_format(sales_invoice_headers.dateInvoiced, 'yyyyMM') between date_format(add_months(current_timestamp(), -12), 'yyyyMM')
  and date_format(add_months(current_timestamp(), -1), 'yyyyMM')
  and NVL(sales_invoice_lines.quantityInvoiced, 0) <> 0 
GROUP BY
  account.name,
  account.accountNumber,
  product.productcode,
  product.name,
  date_format(monthenddate,'yyyy/MM/01')
)

  order by invoice_month, account_number, product_number
  
  """)
main.createOrReplaceTempView("main")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("INVOICE_MONTH"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
