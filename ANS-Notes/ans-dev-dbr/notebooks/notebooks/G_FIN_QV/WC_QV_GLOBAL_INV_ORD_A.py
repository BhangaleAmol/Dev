# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_global_inv_ord_a

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'salesOrderDetailId')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_global_inv_ord_a')
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
source_table = 's_supplychain.sales_order_headers_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_order_headers_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_order_headers_agg = load_full_dataset(source_table)
  
sales_order_headers_agg.createOrReplaceTempView('sales_order_headers_agg')
# sales_order_headers_agg.display()

# COMMAND ----------

# EXTRACT INVOICES
source_table = 's_supplychain.sales_order_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_order_lines = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_order_lines = load_full_dataset(source_table)
  
sales_order_lines.createOrReplaceTempView('sales_order_lines_agg')
# sales_order_lines.display()

# COMMAND ----------

main = spark.sql(
"""select
   oh.customerPONumber CUST_PO_NUM,
   nvl(st.addressLine1,'') SHIP_TO_NAME,
   nvl(st.addressLine2, '') ADDRESS_1,
   nvl(st.addressLine3, '') ADDRESS_2,
   nvl(st.addressLine4, '') ADDRESS_3,
   nvl(st.state, '') AS STATE,
   nvl(st.postalCode,'') AS ZIP_CODE,
   inv.organizationCode AS COMP_NUM,
   oh.orderNumber AS ORDER_NUM,
   ol.lineNumber AS ORDER_LINE_NUM,
   ol.sequenceNumber AS ORDER_LINE_DETAIL_NUM,
   nvl(st.accountNumber, '') AS CUST_NUM,
   pr.productCode AS PROD_NUM,
   pr.name AS PROD_NAME,
   ol.quantityordered * ol.ansStdUomConv AS ORDER_QTY_STD_UOM,  
  round(ol.quantityordered * ol.primaryUomConv,0) ORDER_QTY_PRIM_UOM,
  ol.quantityshipped * ol.ansStdUomConv SHIP_QTY_STD_UOM,
  case
    when
      quantityordered =  quantityshipped
        then 'Y'
     else 'N'
    end  ORDER_COMPLETED,
    case when ol.orderLineHoldType is not null
      or oh.orderHoldType is not null
      then 'On Hold'
     else
      ol.orderStatusDetail
     end AS ORDER_STATUS,
    oh.baseCurrencyId AS LE_CURR,
    oh.transactionCurrencyId AS DOC_CURR,
    ol.orderUomCode AS ORDER_UOM,
    pr.ansStdUom AS STD_UOM,
    oh.orderDate AS ORDER_DT,
    oh.requestDeliveryBy AS REQUEST_DT,
    ol.actualShipDate AS SHIP_DT,
    ol.orderAmount/(ol.quantityordered * ol.ansStdUomConv) AS UNIT_PRICE_STD_UOM_DOC,
    (ol.orderAmount/(ol.quantityordered * ol.ansStdUomConv))/oh.exchangeRateUsd AS UNIT_PRICE_STD_UOM_USD,
    (ol.orderAmount/(ol.quantityordered * ol.ansStdUomConv))*oh.exchangeRate AS UNIT_PRICE_STD_UOM_LE,
    inv.organizationCode AS WAREHOUSE,
    ol.ansStdUomConv CONV_FACTOR,
    0 AS ORIG_CUST_NUM,
    '' AS END_CUST_PO,
    0 AS END_CUST,
    pr.productDivision AS ACCT_DIV,
    CASE
        WHEN
          tt.name like '%Direct Shipment%' OR tt.name LIKE '%Drop Shipment%'
        THEN 'DS'
      ELSE
        'WH'
    END DROP_SHIPMARK_CUST_ORD,
    ol.cetd AS CETD,
    ol.cetd AS REV_CETD,
    '' AS CUST_PROD,
    ol.pricePerUnit AS LIST_PRICE_DOC_CURR,
    CASE
          WHEN tt.name LIKE '%Direct Shipment%' THEN 'Direct Shipment'
          WHEN tt.name LIKE '%Consignment%' THEN 'Consignment'
          WHEN tt.name LIKE '%Credit Memo%' THEN 'Credit Memo'
          WHEN tt.name LIKE '%Drop Shipment%' THEN 'Drop Shipment'
          WHEN tt.name LIKE '%Free Of Charge%' THEN 'Free Of Charge'
          WHEN tt.name LIKE '%GTO%' THEN 'GTO'
          WHEN tt.name LIKE '%Intercompany%' THEN 'Intercompany'
          WHEN tt.name LIKE '%Replacement%' THEN 'Replacement'
          WHEN tt.name LIKE '%Sample Return%' THEN 'Sample Return'
          WHEN tt.name LIKE '%Return%' THEN 'Return'
          WHEN tt.name LIKE '%Sample%' THEN 'Sample'
          WHEN tt.name LIKE '%Service%' THEN 'Service'
          WHEN tt.name LIKE '%Standard%' THEN 'Standard'
          WHEN tt.name LIKE '%Customer_Safety_Stock%' THEN 'Safety Stock'
         ELSE tt.name
    END ORDER_TYPE,
    ol.orderAmount As LINE_VALUE_DOC_CURR,
    (ol.orderAmount/oh.exchangeRateUsd) AS LINE_VALUE_USD,
    0 AS LINE_COST_LE_CURR,
    pr.productBrand As BRAND,
    pr.productSubBrand AS SUBBRAND,  
    ol.salesOrderDetailId
    --  inv.organizationCode,
    --  ol.bookedFlag
from
  sales_order_headers_agg oh
  join sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
  join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
  join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
  join s_core.product_agg pr on ol.item_ID = pr._ID
--   join s_core.account_agg ac on oh.customer_ID = ac._ID
  join s_core.customer_location_agg st on ol.shipToAddress_ID = st._ID
  join s_core.transaction_type_agg tt on oh.orderType_ID = tt._ID
where 1=1
    AND tt.name NOT LIKE '%Safety Stock%'
    AND tt.name NOT LIKE '%Return%'
    AND tt.name NOT LIKE '%Sample%'
    AND tt.name NOT LIKE '%Consignment%'
    AND tt.name NOT LIKE '%Credit Memo%'
    AND tt.name NOT LIKE '%Free Of Charge%'
    AND tt.name NOT LIKE '%Replacement%'
    AND tt.name NOT LIKE '%Sample Return%'
    AND tt.name NOT LIKE '%Service%'
    AND ol.quantityordered !=0
    AND inv.organizationCode IN ('508', '509', '511','532','800','805','325','811','832','600','601','605','401','802','819','602','724', '826', '827')
    AND ol.orderStatusDetail !='Cancelled'
    AND date_format(oh.orderDate, 'yyyyMM') between date_format(add_months(current_date, -11), 'yyyyMM') and date_format(current_date, 'yyyyMM')
    and ol.bookedFlag = 'Y'
    and not ol._deleted
    and not oh._deleted
    """
)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("ORDER_DT"))
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
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_headers_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_headers_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_lines_agg')
