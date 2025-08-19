# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_order_data_full

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'KEY')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_order_data_full')
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

# EXTRACT 
source_table = 's_supplychain.sales_order_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_order_lines_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_order_lines_agg = load_full_dataset(source_table)
  
sales_order_lines_agg.createOrReplaceTempView('sales_order_lines_agg')
# sales_order_lines_agg.display()

# COMMAND ----------

main = spark.sql("""
  select
   ol.salesorderDetailId KEY,
   org.organizationCode Company_Number_Detail,
   pr.productCode PROD_NUM,
   oh.orderNumber ORDER_NUM,
   ol.lineNumber ORDER_LINE_NUM,
   ol.sequenceNumber ORDER_LINE_DETAIL_NUM,
   oh.orderDate ORDER_DT,
   ol.requestDeliveryBy REQUEST_DT,
   ol.scheduledShipDate SCHEDULE_DT,
   ol.actualShipDate SHIP_DT,
   case when upper(ol.orderLineStatus) in ('CLOSED', 'CANCELLED')  
     then ol.orderLineStatus
     when  (oh.orderHoldType is not null or ol.orderLineHoldType is not null)  
     then 'Blocked'
    else ol.orderLineStatus
   end ORDER_STATUS,
   ol.quantityOrdered * ol.ansStdUomConv ORDER_QTY_STD_UOM,
   ol.orderAmount ORDERED_AMOUNT_DOC_CURR,
   oh.baseCurrencyId LE_CURRENCY,
   ac.accountNumber CUSTUMER_ID,
   st.partySiteNumber SHIP_TO_DELIVERY_LOCATION_ID,
   tt.description ORDER_TYPE,
   oh.customerPONumber CUST_PO_NUM,
   case when upper(ol.orderStatusDetail) in ('CLOSED', 'CANCELLED')  
     then ol.orderStatusDetail
     when (oh.orderHoldType is not null or ol.orderLineHoldType is not null)
     then  'On Hold'
    else ol.orderStatusDetail 
   end SOURCE_ORDER_STATUS,
    inv.organizationCode ORGANIZATION_CODE,
    greatest(ol.modifiedOn, oh.modifiedOn, ol._MODIFIED, oh._MODIFIED) W_UPDATE_DT,
    oh.transactionCurrencyId DOC_CURR_CODE,
    ac.accountNumber DISTRIBUTOR_ID,
    ac.name DISTRIBUTOR_NAME,
    ol.deliveryNoteId X_DELIVERY_NOTE_ID,
   case when upper(ol.orderStatusDetail) not in ('CLOSED', 'CANCELLED')
      then coalesce(oh.orderHoldType, ol.orderLineHoldType)
    else ''
   end X_ORDER_HOLD_TYPE,
   ol.quantityReserved X_RESERVATION_VALUE,
   ol.intransitTime X_INTRANSIT_TIME,
   ol.shipDate945 X_SHIPDATE_945,
   oh.freightTerms X_FREIGHT_TERMS,
   ol.createdOn X_CREATION_DATE,
   ol.bookedDate X_BOOKED_DATE,
   ol.quantityShipped X_SHIPPED_QUANTITY,
   ol.shippingQuantityUom X_SHIPPING_QUANTITY_UOM,
   ol.needByDate     X_NEED_BY_DATE,
   ol.retd           X_RETD,
   ol.promiseDate    X_PROMISED_DATE,
   ol.CETD           X_CETD,
   oh.orderDate      ORDER_DATE_TIME,
   ol.promisedOnDate SO_PROMISED_DATE,
   quantityCancelled * ol.ansStdUomConv           CANCELLED_QTY_STD_UOM,
   ol.quantityReserved * ol.ansStdUomConv X_RESERVATION_QTY,
   ol.orderedItem ORDERED_ITEM,
   date_format(ol.deliverNoteDate, 'MM/dd/yyyy') DELIVERY_NOTE_DATE,
   ol.customerLineNumber CUSTOMER_LINE_NUMBER,
   oh.distributionChannel SOURCE_NAME,
   oh.robomotionNumberOfSubscriptions RM_NUM_OF_SUBSCRIPTIONS,
   oh.robomotionSubscription RM_SUBSCRIPTION
 from
   sales_order_headers_agg oh
   join sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
   join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
   join s_core.product_agg pr on ol.item_ID = pr._ID
   join s_core.account_agg ac on oh.customer_ID = ac._ID
   join s_core.customer_location_agg st on ol.shipToAddress_ID = st._ID
   join s_core.transaction_type_agg tt on oh.orderType_ID = tt._ID
 where
   oh._source = 'EBS'
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
  --     and (ol.modifiedOn > current_date - 7
  --     or oh.modifiedOn > current_date - 7)
  """)

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
