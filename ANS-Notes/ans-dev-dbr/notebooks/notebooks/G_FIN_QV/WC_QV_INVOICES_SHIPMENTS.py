# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_invoices_shipments

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'invoiceDetailId')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_invoices_shipments')
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

# EXTRACT ACCRUALS
source_table = 's_supplychain.sales_invoice_headers_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_invoice_headers_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_invoice_headers_agg = load_full_dataset(source_table)
  
sales_invoice_headers_agg.createOrReplaceTempView('sales_invoice_headers_agg')
# sales_invoice_headers_agg.display()

# COMMAND ----------

# EXTRACT INVOICES
source_table = 's_supplychain.sales_invoice_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_invoice_lines = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_invoice_lines = load_full_dataset(source_table)
  
sales_invoice_lines.createOrReplaceTempView('sales_invoice_lines_agg')
# sales_invoice_lines.display()

# COMMAND ----------

# %sql
# CREATE OR REPLACE VIEW g_fin_qv.wc_qv_invoices_shipments_v AS
main = spark.sql("""
select
  sales_invoice_lines.returnFlag,
  sales_invoice_lines.discountlineFlag,
  tt_invh.transactionTypeCode,
  org.organizationCode AS CMP_NUM_DETAIL,
  account.accountNumber AS CUSTOMER_ID,
  shipTo.partySiteNumber AS SHIPTO_DLY_LOC_ID,
  sales_invoice_lines.warehouseCode AS ITEM_BRANCH_KEY,
  sales_invoice_lines.orderNumber AS ORDER_NUMBER,
  sales_invoice_lines.orderLineNumber as ORDER_LINE_NUMBER,
  sales_invoice_headers.invoiceNumber as INVOICE_NUMBER,
  sales_invoice_lines.actualShipDate as SHIP_DATE,
  sales_invoice_headers.dateInvoiced as INVOICE_DATE,
  product.productCode ITEM_NUMBER,
  product.ansStdUom ANSELL_STD_UM,
  SUM(case when sales_invoice_lines.discountlineFlag = 'N' 
            and tt_invh.transactionTypeCode not in ('CM')
          then sales_invoice_lines.quantityInvoiced * sales_invoice_lines.ansStdUomConv
          else 0 
          end 
    + 
    case when sales_invoice_lines.discountlineFlag = 'N'
          and sales_invoice_lines.returnFlag = 'Y'
          and tt_invh.transactionTypeCode in ('CM')
     then sales_invoice_lines.quantityInvoiced * sales_invoice_lines.ansStdUomConv 
     else 0 end
  ) as SALES_QUANTITY,
  sales_invoice_headers.baseCurrencyId CURRENCY,
  SUM( CASE WHEN sales_invoice_lines.returnFlag = 'Y'
            and tt_invh.transactionTypeCode in ('CM') 
              then 0
            WHEN sales_invoice_lines.returnFlag = 'Y'
            and tt_invh.transactionTypeCode not in ('CM') 
              then sales_invoice_lines.baseAmount * sales_invoice_lines.exchangeRate
            WHEN sales_invoice_lines.returnFlag <> 'Y'
                 then sales_invoice_lines.baseAmount * sales_invoice_lines.exchangeRate 
            end
  ) as GROSS_SALES_AMOUNT_LC,
  SUM(
    CASE
      WHEN sales_invoice_lines.returnFlag = 'Y' THEN -sales_invoice_lines.baseAmount * sales_invoice_lines.exchangeRate
      ELSE 0
    END
  ) RETURNS_LC,
  SUM(
    (
      sales_invoice_lines.settlementDiscountEarned + sales_invoice_lines.settlementDiscountUnEarned
    ) * sales_invoice_lines.exchangeRate
  ) as SETTLEMENT_DISCOUNT_LC,
  NVL(
    SUM( (sales_invoice_lines.baseAmount  - sales_invoice_lines.settlementDiscountEarned) * sales_invoice_lines.exchangeRate)  ,    0  ) NET_SALES_LC,
  SUM(
    (
      CASE
        WHEN sales_invoice_lines.discountLineFlag = 'N' then sales_invoice_lines.seeThruCost
      end
    ) * sales_invoice_lines.exchangeRate
  ) as SALES_COST_AMOUNT_LC,
  CASE
    WHEN tt_invl.description LIKE '%Direct Shipment%' THEN 'Direct Shipment'
    WHEN tt_invl.description LIKE '%Consignment%' THEN 'Consignment'
    WHEN tt_invl.description LIKE '%Credit Memo%' THEN 'Credit Memo'
    WHEN tt_invl.description LIKE '%Drop Shipment%' THEN 'Drop Shipment'
    WHEN tt_invl.description LIKE '%Free Of Charge%' THEN 'Free Of Charge'
    WHEN tt_invl.description LIKE '%GTO%' THEN 'GTO'
    WHEN tt_invl.description LIKE '%Intercompany%' THEN 'Intercompany'
    WHEN tt_invl.description LIKE '%Replacement%' THEN 'Replacement'
    WHEN tt_invl.description LIKE '%Sample Return%' THEN 'Sample Return'
    WHEN tt_invl.description LIKE '%Return%' THEN 'Return'
    WHEN tt_invl.description LIKE '%Sample%' THEN 'Sample'
    WHEN tt_invl.description LIKE '%Service%' THEN 'Service'
    WHEN tt_invl.description LIKE '%Standard%' THEN 'Standard'
    WHEN tt_invl.description LIKE '%Customer_Safety_Stock%' THEN 'Safety Stock'
    ELSE tt_invl.description
  END as DOC_TYPE,
  tt_invh.transactionTypeCode as TRANS_TYPE,
  SUM(legalEntityCostLeCurrency) as SALES_COST_AMOUNT_LE,
  sales_invoice_headers.transactionCurrencyId DOC_CURR_CODE,
  SUM( CASE WHEN sales_invoice_lines.returnFlag = 'Y'
            and tt_invh.transactionTypeCode in ('CM') 
              then 0
            WHEN sales_invoice_lines.returnFlag = 'Y'
            and tt_invh.transactionTypeCode not in ('CM') 
              then sales_invoice_lines.baseAmount 
            WHEN sales_invoice_lines.returnFlag <> 'Y'
                 then sales_invoice_lines.baseAmount 
            end
  ) as GROSS_SALES_AMOUNT_DOC,        
  SUM(
    CASE
      WHEN sales_invoice_lines.returnFlag = 'Y' THEN -sales_invoice_lines.baseAmount
      ELSE 0
    END
  ) as RETURNS_DOC,
  SUM(
    (
      sales_invoice_lines.settlementDiscountEarned + sales_invoice_lines.settlementDiscountUnEarned
    )
  ) as SETTLEMENT_DISCOUNT_DOC,
  NVL(SUM(sales_invoice_lines.baseAmount - (sales_invoice_lines.settlementDiscountEarned + sales_invoice_lines.settlementDiscountUnEarned)), 0  ) NET_SALES_DOC,
  SUM(sales_invoice_lines.seeThruCost) as SALES_COST_AMOUNT_DOC,
  SUM(sales_invoice_lines.legalEntityCost) as SALES_COST_AMOUNT_LE_DOC,
  sales_invoice_lines.customerPONumber as CUST_PO_NUM,
  sales_invoice_lines.distributionChannel as SOURCE_NAME,
  sales_invoice_lines.invoiceDetailId
from
  s_supplychain.sales_invoice_lines_agg sales_invoice_lines
  join s_supplychain.sales_invoice_headers_agg sales_invoice_headers on sales_invoice_lines.invoice_ID = sales_invoice_headers._ID
  join s_core.organization_agg org on sales_invoice_headers.owningbusinessunit_id = org._ID
  join s_core.account_agg account on sales_invoice_headers.customer_ID = account._ID 
  join s_core.product_agg product on sales_invoice_lines.item_ID = product._ID
  join s_core.customer_location_agg shipTo on sales_invoice_headers.shipToAddress_ID = shipto._ID
  join s_core.transaction_type_agg tt_invh on sales_invoice_headers.transaction_ID = tt_invh._id
  join s_core.transaction_type_agg tt_invl on sales_invoice_lines.orderType_ID = tt_invl._id 
  join s_core.date date on date_format(sales_invoice_headers.dateInvoiced,'yyyyMMdd') = date.dayid 
 
where
  sales_invoice_headers._SOURCE = 'EBS'
  and sales_invoice_lines._SOURCE = 'EBS' 
  and account.customerType = 'External'
 -- and sales_invoice_headers.invoiceNumber in ('18795', '21303652')
  and date.currentPeriodCode in ('Current','Previous')
  and product.productCode <> 'unknown'
GROUP BY
  account.accountNumber,
  shipTo.partySiteNumber,
  sales_invoice_lines.warehouseCode,
  sales_invoice_lines.orderNumber,
  sales_invoice_lines.orderLineNumber,
  sales_invoice_headers.invoiceNumber,
  sales_invoice_lines.actualShipDate,
  sales_invoice_headers.dateInvoiced,
  product.productCode,
  product.ansStdUom,
  sales_invoice_headers.baseCurrencyId,
  sales_invoice_headers.transactionCurrencyId,
  sales_invoice_lines.customerPONumber,
  sales_invoice_lines.distributionChannel,
  tt_invh.transactionTypeCode,
  CASE
    WHEN tt_invl.description LIKE '%Direct Shipment%' THEN 'Direct Shipment'
    WHEN tt_invl.description LIKE '%Consignment%' THEN 'Consignment'
    WHEN tt_invl.description LIKE '%Credit Memo%' THEN 'Credit Memo'
    WHEN tt_invl.description LIKE '%Drop Shipment%' THEN 'Drop Shipment'
    WHEN tt_invl.description LIKE '%Free Of Charge%' THEN 'Free Of Charge'
    WHEN tt_invl.description LIKE '%GTO%' THEN 'GTO'
    WHEN tt_invl.description LIKE '%Intercompany%' THEN 'Intercompany'
    WHEN tt_invl.description LIKE '%Replacement%' THEN 'Replacement'
    WHEN tt_invl.description LIKE '%Sample Return%' THEN 'Sample Return'
    WHEN tt_invl.description LIKE '%Return%' THEN 'Return'
    WHEN tt_invl.description LIKE '%Sample%' THEN 'Sample'
    WHEN tt_invl.description LIKE '%Service%' THEN 'Service'
    WHEN tt_invl.description LIKE '%Standard%' THEN 'Standard'
    WHEN tt_invl.description LIKE '%Customer_Safety_Stock%' THEN 'Safety Stock'
    ELSE tt_invl.description
  END,
  org.organizationCode,
  sales_invoice_lines.returnFlag,
  sales_invoice_lines.discountlineFlag,
  tt_invh.transactionTypeCode,
  sales_invoice_lines.invoiceDetailId
  order by 
  product.productCode
  """)
main.createOrReplaceTempView('main')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("INVOICE_DATE"))
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

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_headers_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_headers_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_lines_agg')
