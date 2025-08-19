# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_global_inventory_ship_a

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'order_num,order_line_num,order_line_detail_num,invoice_number, salesorderDetailId,SHIPPED_DT')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_global_inventory_ship')
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
source_table = 's_supplychain.sales_invoice_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_invoice_lines_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_invoice_lines_agg = load_full_dataset(source_table)
  
sales_invoice_lines_agg.createOrReplaceTempView('sales_invoice_lines_agg')
# sales_invoice_lines_agg.display()

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

# EXTRACT 
source_table = 's_supplychain.sales_shipping_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_shipping_lines_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_shipping_lines_agg = load_full_dataset(source_table)
  
sales_shipping_lines_agg.createOrReplaceTempView('sales_shipping_lines_agg')
# sales_shipping_lines_agg.display()

# COMMAND ----------

orderlines_lkp = spark.sql("""
SELECT
      org.name operating_unit,
      oh.orderNumber,
      ol.salesorderDetailId,
      oh.salesOrderId,
      ol.orderAmount /(ol.quantityordered * ol.ansStdUomConv) AS UNIT_PRICE,
      ol.quantityordered * ol.ansStdUomConv qty,
      pr.productcode,
      pr.name AS PROD_NAME,
      pr.productBrand,
      pr.productSubBrand,
      pr.ansStdUom,
      ac.accountNumber,
      ac.name
    from
      sales_order_lines_agg ol
      join s_supplychain.sales_order_headers_agg oh on oh._ID = ol.salesOrder_ID
      join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
      join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
      join s_core.product_agg pr on ol.item_ID = pr._ID
      join s_core.date date on date_format(ol.actualShipDate, 'yyyyMMdd') = date.dayid
      join s_core.transaction_type_agg tt on oh.orderType_Id = tt._id
      join s_core.account_agg ac on oh.customer_ID = ac._ID
    where
    oh._SOURCE = 'EBS' and
      --date_format(ol.actualShipDate,'yyyyMM') = '202103'
      date.currentPeriodCode in ('Current', 'Previous')
      and CASE
        WHEN tt.description LIKE '%Direct Shipment%' THEN 'Direct Shipment'
        WHEN tt.description LIKE '%Consignment%' THEN 'Consignment'
        WHEN tt.description LIKE '%Credit Memo%' THEN 'Credit Memo'
        WHEN tt.description LIKE '%Drop Shipment%' THEN 'Drop Shipment'
        WHEN tt.description LIKE '%Free Of Charge%' THEN 'Free Of Charge'
        WHEN tt.description LIKE '%GTO%' THEN 'GTO'
        WHEN tt.description LIKE '%Intercompany%' THEN 'Intercompany'
        WHEN tt.description LIKE '%Replacement%' THEN 'Replacement'
        WHEN tt.description LIKE '%Sample Return%' THEN 'Sample Return'
        WHEN tt.description LIKE '%Return%' THEN 'Return'
        WHEN tt.description LIKE '%Sample%' THEN 'Sample'
        WHEN tt.description LIKE '%Service%' THEN 'Service'
        WHEN tt.description LIKE '%Standard%' THEN 'Standard'
        WHEN tt.description LIKE '%Customer_Safety_Stock%' THEN 'Safety Stock'
        ELSE tt.description
      end NOT IN (
        'Safety Stock',
        'Return',
        'Sample',
        'Consignment',
        'Credit Memo',
        'Free Of Charge',
        'Replacement',
        'Sample Return',
        'Service'
      )
""")
orderlines_lkp.createOrReplaceTempView("orderlines_lkp")

# COMMAND ----------

picklines_lkp = spark.sql("""
SELECT
      ship.salesorderDetailId,
      ship.salesOrderId,
      inv.organizationCode,
      ship.lineNumber,
      ship.actualPickDate,
      pr.productcode,
      sequenceNumber orderItemDetailNumber,
      sum(ship.quantityShipped * ship.ansStdUomConv) Qty
    from
      sales_shipping_lines_agg ship
      join s_core.organization_agg inv on ship.inventoryWarehouse_ID = inv._ID
      join s_core.product_agg pr on ship.item_ID = pr._ID
      join s_core.date date on date_format(ship.actualPickDate, 'yyyyMMdd') = date.dayid
    where
    ship._SOURCE = 'EBS'
      --date_format(ship.actualShipDate,'yyyyMM') =  '202103'
     AND date.currentPeriodCode in ('Current', 'Previous')
      AND inv.organizationCode IN (
        '508',
        '509',
        '511',
        '532',
        '800',
        '805',
        '325',
        '811',
        '832',
        '600',
        '601',
        '605',
        '401',
        '802',
        '819',
        '602',
        '724',
        '826',
        '827'
      )
    group by
      ship.salesorderDetailId,
      ship.salesOrderId,
      inv.organizationCode,
      ship.lineNumber,
      ship.actualPickDate,
      pr.productcode,
      ship.sequenceNumber
""")
picklines_lkp.createOrReplaceTempView("picklines_lkp")

# COMMAND ----------

invoicelines_lkp = spark.sql("""
select
      int(float(invl.salesorderDetailId)) salesorderDetailId,
      invh.invoiceNumber,
      invh.dateInvoiced,
      date.fiscalYearId fiscal_year,
      date.periodName fiscal_period,
      date.yearId calendar_year,
      shipto.partysitenumber
    from
      sales_invoice_lines_agg invl
      join s_supplychain.sales_invoice_headers_agg invh on invl.invoice_ID = invh._ID
      join s_core.customer_location_agg shipTo on invl.shipToAddress_ID = shipto._ID
      join s_core.date date on date_format(invl.actualShipDate, 'yyyyMMdd') = date.dayid
    where
      invh._SOURCE = 'EBS'
      and invl._SOURCE = 'EBS' -- and date_format(invl.actualShipDate,'yyyyMM') =   '202103'
      AND date.currentPeriodCode in ('Current', 'Previous')
      and invl.salesorderDetailId is not null
""")
invoicelines_lkp.createOrReplaceTempView("invoicelines_lkp")

# COMMAND ----------

main = spark.sql("""
SELECT DISTINCT
  orderline_details.operating_unit OPERATING_UNIT,
  orderline_details.orderNumber ORDER_NUM,
  pickline_details.lineNumber ORDER_LINE_NUM,
  pickline_details.orderItemDetailNumber ORDER_LINE_DETAIL_NUM,
  invoice_details.invoiceNumber INVOICE_NUMBER,
  pickline_details.organizationCode CIE,
  ROUND(pickline_details.qty, 2) QTY_SHIPPED,
  pickline_details.actualPickDate SHIPPED_DT,
  orderline_details.accountNumber CUST_NUM,
  orderline_details.productcode PROD_NUM,
  orderline_details.PROD_NAME PROD_NAME,
  orderline_details.productbrand BRAND,
  orderline_details.productsubbrand SUBBRAND,
  ROUND(orderline_details.unit_price, 5) UNIT_PRICE,
  invoice_details.dateinvoiced INVOICE_DT,
  invoice_details.fiscal_year FISCAL_YEAR,
  invoice_details.fiscal_period FISCAL_PERIOD,
  invoice_details.calendar_year CALENDER_YEAR,
  invoice_details.partysitenumber SHIPTO_SITE,
  orderline_details.ansstduom STD_UOM,
  CASE
    WHEN orderline_details.accountNumber in ('11682', '11683') then 'IC'
    WHEN invoice_details.partysitenumber IN (
      '10266',
      '10382',
      '10200',
      '10633',
      '10881',
      '11634',
      '11974',
      '595133',
      '368690',
      '402281',
      '497486',
      '368697',
      '368689'
    ) THEN 'DS'
  END DIRECT_SHIPMENT
 ,orderline_details.salesorderDetailId
from
  (
    --orderlines
    SELECT
      operating_unit,
      orderNumber,
      salesorderDetailId,
      salesOrderId,
      UNIT_PRICE,
      qty,
      productcode,
      PROD_NAME,
      productBrand,
      productSubBrand,
      ansStdUom,
      accountNumber,
      name
    from
      orderlines_lkp
  ) orderline_details
  join (
    --picklines
    SELECT
      salesorderDetailId,
      salesOrderId,
      organizationCode,
      lineNumber,
      actualPickDate,
      productcode,
      orderItemDetailNumber,
      Qty
    from
      picklines_lkp
  ) pickline_details on pickline_details.salesorderDetailId = orderline_details.salesorderDetailId
                        and pickline_details.salesOrderId = orderline_details.salesOrderId
  left join (
    --invoicelines
    select
      salesorderDetailId,
      invoiceNumber,
      dateInvoiced,
      fiscal_year,
      fiscal_period,
      calendar_year,
      partysitenumber
    from
     invoicelines_lkp
  ) invoice_details on invoice_details.salesorderDetailId = orderline_details.salesorderDetailId
  --where orderline_details.orderNumber='1488287'
  """)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("INVOICE_DT"))#,"SHIPPED_DT"
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()
  
main_f.createOrReplaceTempView('main_f')

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_lines_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_lines_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_shipping_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_shipping_lines_agg')
  
