# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_gross_cbo

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'salesOrderDetailId')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_gross_cbo')
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
sales_order_headers_agg.display()

# COMMAND ----------

# EXTRACT 
source_table = 's_supplychain.sales_order_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_order_lines_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_order_lines_agg = load_full_dataset(source_table)
  
sales_order_lines_agg.createOrReplaceTempView('sales_order_lines_agg')
sales_order_lines_agg.display()

# COMMAND ----------

main = spark.sql("""
select 
  'Oracle' SOURCE,
  LAST_DAY(current_date - 1) MONTH_END_DATE,
  pr.productCode ITEM_NUMBER,
  pr.name ITEM_DESCRIPTION,
  inv.organizationCode INVENTORY_ORG_CODE,
  inv.name INVENTORY_ORG_NAME,
  org.name OPERATING_UNIT,
  --org.organizationcode,
  ol.ansStdUom STD_UOM,
  ol.primaryUomCode PRIMARY_UOM,
  ol.quantityBackOrdered * ol.ansStdUomConv  CBO_QTY,
  ol.quantityBackOrdered * ol.primaryUomConv CBO_QTY_PRIMARY_UOM,
  --ol.quantitycancelled,
  --ol.quantityshipped,
  round(ol.backOrderValue / nvl(oh.exchangeRateUsd,1), 2) CBO_VALUE_USD,
  tt.transactionTypeGroup ORDER_TYPE,
  tt.dropShipmentFlag DIRECT_SHIP_FLG,
  st.siteCategory SHIP_TO_REGION,
  oh.orderNumber SALES_ORDER,
  date_format(oh.orderDate, 'dd-MMM-yy') ORDERED_DATE,
  date_format(ol.requestDeliveryBy, 'dd-MMM-yy') REQUEST_DATE,
  date_format(ol.scheduledShipDate, 'dd-MMM-yy') SCHEDULED_DATE,
  ac.accountNumber CUSTOMER_NUMBER,
  ac.name CUSTOMER_NAME,
  case 
    when upper(ol.orderStatusDetail) in ('CLOSED', 'CANCELLED')  
    then ol.orderStatusDetail
    when oh.orderHoldType is not null or ol.orderLineHoldType is not null
    then  'On Hold'
    else ol.orderStatusDetail
  end STATUS_NAME,
  origin.originid ORIGIN_CODE ,
  origin.originName ORIGIN_DESCRIPTION ,
  tt.description XACT_SUBTYPE_CODE,
  ol.salesOrderDetailId
from
  s_supplychain.sales_order_headers_agg oh
  join s_supplychain.sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
  join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
  join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
  join s_core.product_agg pr on ol.item_ID = pr._ID
  join s_core.account_agg ac on oh.customer_ID = ac._ID
  join s_core.customer_location_agg st on ol.shipToAddress_ID = st._ID
  join s_core.transaction_type_agg tt on oh.orderType_Id = tt._Id
  left join s_core.payment_methods_agg pm on oh.paymentMethod_ID = pm._id
  join s_core.payment_terms_agg pt on oh.paymentTerm_ID = pt._id
  left join s_core.origin_agg origin on origin._id = pr.origin_ID
where
  oh._source = 'EBS'
  and not oh._deleted
  and not ol._deleted
  and upper(tt.description) not like '%SAMPLE%'
  and upper(tt.description) not like '%SAFETY STOCK%'
  and upper(tt.description) not like '%RETURN%'
  and upper(tt.description) not like '%CREDIT MEMO%'
  and upper(tt.description) not like '%REPLACEMENT%'
  and upper(tt.description) not like '%SERVICE%'
  and upper(tt.description) not like '%INTERCOMPANY%'
  and upper(tt.description) not like '%RMA%'
  and ac.customertype = 'External'
  and ol.bookedflag = 'Y'
  and ol.requestDeliveryBy < current_date 
  and upper(nvl(oh.orderHeaderStatus, 'CLOSED')) not in ('CLOSED', 'CANCELLED', 'ENTERED')
  and upper(nvl(ol.orderLineStatus, 'CLOSED')) not in ('BLOCKED', 'CLOSED', 'CANCELLED', 'ENTERED')
  and org.organizationcode not in ('2190')
  and not ol._deleted
  and not oh._deleted
  and ol.quantityBackOrdered <> 0
  and (oh.orderHoldType is  null or oh.orderHoldType in (select KEY_VALUE from smartsheets.edm_control_table where Table_Id = 'INCLUDE_HOLD_CODES' ))
  and (ol.orderLineHoldType is  null or ol.orderLineHoldType in (select KEY_VALUE from smartsheets.edm_control_table where Table_Id = 'INCLUDE_HOLD_CODES' ))
  and ol.orderLineHoldType is  null
  """)
main.createOrReplaceTempView('main')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("MONTH_END_DATE"))
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
