# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.global_invoices

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = get_input_param('incremental', 'bool', default_value = True)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'global_invoices')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/na_tm/full_data')

# COMMAND ----------

overwrite - True

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

# EXTRACT INVOICES
source_table = 's_supplychain.sales_invoice_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_invoice_lines = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_invoice_lines = load_full_dataset(source_table)
  
sales_invoice_lines.createOrReplaceTempView('sales_invoice_lines_agg')
sales_invoice_lines.display()

# COMMAND ----------

# EXTRACT ACCRUALS
source_table = 's_trademanagement.trade_promotion_accruals_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  trade_promotion_accruals = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  trade_promotion_accruals = load_full_dataset(source_table)
  
trade_promotion_accruals.createOrReplaceTempView('trade_promotion_accruals_agg')
trade_promotion_accruals.display()

# COMMAND ----------

# SAMPLING
if sampling:  
  sales_invoice_lines = sales_invoice_lines.limit(10)
  sales_invoice_lines.createOrReplaceTempView('sales_invoice_lines_agg')

# COMMAND ----------

# SAMPLING
if sampling:  
  trade_promotion_accruals = trade_promotion_accruals.limit(10)
  trade_promotion_accruals.createOrReplaceTempView('trade_promotion_accruals_agg')

# COMMAND ----------

main_invoices_f=spark.sql("""
SELECT
  _ID,
  SOURCE,
  ACCOUNT_ID,
  ACCOUNT_NUMBER,
  ITEM_ID,
  ITEMID,
  INVOICED_DT_ID,
  LOAD_DATE,
  SUM(QTY_IN_CASES) QTY_IN_CASES,
  SUM(QTY_ANS_STD_UOM_CONV) QTY_ANS_STD_UOM_CONV,
  SUM(GROSS_SALES_AMOUNT) GROSS_SALES_AMOUNT,
  SUM(RETURNS_AMOUNT) RETURNS_AMOUNT,
  SUM(SETTLEMENT_DISCOUNT_AMOUNT) SETTLEMENT_DISCOUNT_AMOUNT,
  SUM(CO_OP_REBATE_AMOUNT) CO_OP_REBATE_AMOUNT,
  SUM(REBATES_AMOUNT) REBATES_AMOUNT,
  SUM(VOLUME_DISCOUNT_AMOUNT) VOLUME_DISCOUNT_AMOUNT,
  SUM(BOGO_AMOUNT) BOGO_AMOUNT,
  SUM(SLOTTING_AMOUNT) SLOTTING_AMOUNT,
  SUM(TPR_AMOUNT) TPR_AMOUNT,
  SUM(
    NVL(GROSS_SALES_AMOUNT, 0) - NVL(RETURNS_AMOUNT, 0) - NVL(SETTLEMENT_DISCOUNT_AMOUNT, 0) - NVL(CO_OP_REBATE_AMOUNT, 0) - NVL(REBATES_AMOUNT, 0) - NVL(VOLUME_DISCOUNT_AMOUNT, 0) - NVL(BOGO_AMOUNT, 0) - NVL(SLOTTING_AMOUNT, 0) - NVL(TPR_AMOUNT, 0)
  ) NET_SALES_AMOUNT,
  SUM(
    NVL(GROSS_SALES_AMOUNT, 0) - NVL(RETURNS_AMOUNT, 0) - NVL(SETTLEMENT_DISCOUNT_AMOUNT, 0) - NVL(REBATES_AMOUNT, 0) - NVL(VOLUME_DISCOUNT_AMOUNT, 0) - NVL(BOGO_AMOUNT, 0) - NVL(SLOTTING_AMOUNT, 0) - NVL(TPR_AMOUNT, 0)
  ) NET_SALES_REPORTED_AMOUNT
FROM
  (
    select
      sales_invoice_lines._id,
      'ITD' SOURCE,
      account._ID ACCOUNT_ID,
      account.accountNumber ACCOUNT_NUMBER,
      product_qv._id ITEM_ID,
      product.productcode ITEMID,
      date_format(sales_invoice_headers.dateInvoiced, 'yyyyMMdd') INVOICED_DT_ID,
      CURRENT_DATE() LOAD_DATE,
      SUM(
        case
          when sales_invoice_lines.discountlineFlag = 'N'
          and tt_invh.transactionTypeCode not in ('CM') then sales_invoice_lines.quantityInvoiced * sales_invoice_lines.caseUomConv
          else 0
        end + case
          when sales_invoice_lines.discountlineFlag = 'N'
          and sales_invoice_lines.returnFlag = 'Y'
          and tt_invh.transactionTypeCode in ('CM') then sales_invoice_lines.quantityInvoiced * sales_invoice_lines.caseUomConv
          else 0
        end
      ) as QTY_IN_CASES,
      SUM(
        case
          when sales_invoice_lines.discountlineFlag = 'N'
          and tt_invh.transactionTypeCode not in ('CM') then sales_invoice_lines.quantityInvoiced * sales_invoice_lines.ansStdUomConv
          else 0
        end + case
          when sales_invoice_lines.discountlineFlag = 'N'
          and sales_invoice_lines.returnFlag = 'Y'
          and tt_invh.transactionTypeCode in ('CM') then sales_invoice_lines.quantityInvoiced * sales_invoice_lines.ansStdUomConv
          else 0
        end
      ) as QTY_ANS_STD_UOM_CONV,
      SUM(
        CASE
          WHEN sales_invoice_lines.returnFlag = 'Y'
          and tt_invh.transactionTypeCode in ('CM') then 0
          WHEN sales_invoice_lines.returnFlag = 'Y'
          and tt_invh.transactionTypeCode not in ('CM') then sales_invoice_lines.baseAmount * sales_invoice_lines.exchangeRate
          WHEN sales_invoice_lines.returnFlag <> 'Y' then sales_invoice_lines.baseAmount * sales_invoice_lines.exchangeRate
        end
      ) as GROSS_SALES_AMOUNT,
      SUM(
        CASE
          WHEN sales_invoice_lines.returnFlag = 'Y' THEN - sales_invoice_lines.baseAmount * sales_invoice_lines.exchangeRate
          ELSE 0
        END
      ) RETURNS_AMOUNT,
      SUM(
        (
          sales_invoice_lines.settlementDiscountEarned + sales_invoice_lines.settlementDiscountUnEarned
        ) * sales_invoice_lines.exchangeRate
      ) as SETTLEMENT_DISCOUNT_AMOUNT,
      0 CO_OP_REBATE_AMOUNT,
      0 REBATES_AMOUNT,
      0 VOLUME_DISCOUNT_AMOUNT,
      0 BOGO_AMOUNT,
      0 SLOTTING_AMOUNT,
      0 TPR_AMOUNT
    from
      sales_invoice_lines_agg sales_invoice_lines
      join s_supplychain.sales_invoice_headers_agg sales_invoice_headers on sales_invoice_lines.invoice_ID = sales_invoice_headers._ID
      join s_core.organization_agg org on sales_invoice_headers.owningbusinessunit_id = org._ID
      join s_core.account_agg account on sales_invoice_headers.customer_ID = account._ID
      join s_core.product_agg product on sales_invoice_lines.item_ID = product._ID
      join s_core.customer_location_agg shipTo on sales_invoice_headers.shipToAddress_ID = shipto._ID
      join s_core.customer_location_agg billTo on sales_invoice_headers.billtoAddress_ID = billTo._ID
      join s_core.transaction_type_agg tt_invh on sales_invoice_headers.transaction_ID = tt_invh._id
      join s_core.transaction_type_agg tt_invl on sales_invoice_lines.orderType_ID = tt_invl._id
      join s_core.date date on date_format(sales_invoice_headers.dateInvoiced, 'yyyyMMdd') = date.dayid
      left join s_core.product_qv on product.productcode = product_qv.itemid
    where
      sales_invoice_headers._SOURCE = 'EBS'
      and sales_invoice_lines._SOURCE = 'EBS'
      and account.customerType = 'External' -- and sales_invoice_headers.invoiceNumber in ('18795', '21303652')
      --and date.currentPeriodCode in ('Current','Previous')
      and date_format(sales_invoice_headers.dateInvoiced, 'yyyyMM') >= '201801'
      and product.productCode <> 'unknown'
    GROUP BY
      account._ID,
      account.accountNumber,
      product.productcode,
      date_format(sales_invoice_headers.dateInvoiced, 'yyyyMMdd'),
      product_qv._id,
      sales_invoice_lines._id
    union
    select
      trade_promotion_accruals_agg._id,
      'ITD' SOURCE,
      account._ID ACCOUNT_ID,
      account.accountNumber ACCOUNT_NUMBER,
      product_qv._id ITEM_ID,
      product.productcode ITEMID,
      date_format(trade_promotion_accruals_agg.glDate, 'yyyyMMdd') INVOICED_DT_ID,
      CURRENT_DATE() LOAD_DATE,
      0 QTY_IN_CASES,
      0 QTY_ANS_STD_UOM_CONV,
      0 GROSS_SALES_AMOUNT,
      0 RETURNS_AMOUNT,
      0 SETTLEMENT_DISCOUNT_AMOUNT,
      sum(
        case
          when trade_promotion_accruals_agg.categoryId = 10005 then nvl(trade_promotion_accruals_agg.amountActual, 0) + nvl(trade_promotion_accruals_agg.amountRemaining, 0)
          else 0
        end
      ) CO_OP_REBATE_AMOUNT,
      sum(
        case
          when trade_promotion_accruals_agg.categoryId = 10000 then nvl(trade_promotion_accruals_agg.amountActual, 0) + nvl(trade_promotion_accruals_agg.amountRemaining, 0)
          else 0
        end
      ) REBATES_AMOUNT,
      sum(
        case
          when trade_promotion_accruals_agg.categoryId = 10001 then nvl(trade_promotion_accruals_agg.amountActual, 0) + nvl(trade_promotion_accruals_agg.amountRemaining, 0)
          else 0
        end
      ) VOLUME_DISCOUNT_AMOUNT,
      sum(
        case
          when trade_promotion_accruals_agg.categoryId = 10008 then nvl(trade_promotion_accruals_agg.amountActual, 0) + nvl(trade_promotion_accruals_agg.amountRemaining, 0)
          else 0
        end
      ) BOGO_AMOUNT,
      sum(
        case
          when trade_promotion_accruals_agg.categoryId = 10004 then nvl(trade_promotion_accruals_agg.amountActual, 0) + nvl(trade_promotion_accruals_agg.amountRemaining, 0)
          else 0
        end
      ) SLOTTING_AMOUNT,
      sum(
        case
          when trade_promotion_accruals_agg.categoryId = 10003 then nvl(trade_promotion_accruals_agg.amountActual, 0) + nvl(trade_promotion_accruals_agg.amountRemaining, 0)
          else 0
        end
      ) TPR_AMOUNT
    from
      trade_promotion_accruals_agg
      join s_core.organization_agg org on trade_promotion_accruals_agg.owningbusinessunit_id = org._ID
      join s_core.account_agg account on trade_promotion_accruals_agg.account_ID = account._ID
      left join s_core.product_agg product on trade_promotion_accruals_agg.item_ID = product._ID
      join s_core.date date on date_format(trade_promotion_accruals_agg.glDate, 'yyyyMMdd') = date.dayid
      left join s_core.product_qv on product.productcode = product_qv.itemid
    where
      trade_promotion_accruals_agg._SOURCE = 'EBS'
      and account.customerType = 'External' --and date.currentPeriodCode in ('Current','Previous')
    group by
      account._ID,
      account.accountNumber,
      product_qv._id,
      product.productcode,
      date_format(trade_promotion_accruals_agg.glDate, 'yyyyMMdd'),
      trade_promotion_accruals_agg._id
  )
GROUP BY
  _ID,
  SOURCE,
  ACCOUNT_ID,
  ACCOUNT_NUMBER,
  ITEM_ID,
  ITEMID,
  INVOICED_DT_ID,
  LOAD_DATE
  
  """)
main_invoices_f.createOrReplaceTempView('main_invoices_f')

# COMMAND ----------

main_df = (
  main_invoices_f
)
main_df.cache()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("LOAD_DATE"))
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

# COMMAND ----------

# HANDLE DELETE


full_inv_keys = spark.sql("""
  SELECT sales_invoice_lines._id from s_supplychain.sales_invoice_lines_agg sales_invoice_lines
  join s_supplychain.sales_invoice_headers_agg  sales_invoice_headers on sales_invoice_lines.invoice_ID = sales_invoice_headers._ID
  where sales_invoice_headers._SOURCE = 'EBS'
  and sales_invoice_lines._SOURCE = 'EBS'
 

""")

full_acc_keys = spark.sql("""
  SELECT _id from s_trademanagement.trade_promotion_accruals_agg accruals
  where accruals._SOURCE = 'EBS'
  and accruals._deleted = 'false'
 
""")

full_keys_f = (
  full_inv_keys
  .union(full_acc_keys)
  .select('_ID')
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:

  cutoff_value = get_max_value(trade_promotion_accruals, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_trademanagement.trade_promotion_accruals_agg')
  update_run_datetime(run_datetime, target_table, 's_trademanagement.trade_promotion_accruals_agg')

  cutoff_value = get_max_value(sales_invoice_lines, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_lines_agg')
