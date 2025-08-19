# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_inventory_extract

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'item_ID,inventoryWarehouse_ID,owningBusinessUnit_ID,MONTH_END_DATE')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_inventory_extract')
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

# EXTRACT POINT_OF_SALES
source_table = 's_supplychain.inventory_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  inventory_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  inventory_agg = load_full_dataset(source_table)
  
inventory_agg.createOrReplaceTempView('inventory_agg')
inventory_agg.display()

# COMMAND ----------

# EXTRACT ACCRUALS
source_table = 's_supplychain.sales_invoice_headers_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  sales_invoice_headers_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_invoice_headers_agg = load_full_dataset(source_table)
  
sales_invoice_headers_agg.createOrReplaceTempView('sales_invoice_headers_agg')
sales_invoice_headers_agg.display()

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

# SAMPLING
if sampling:
  inventory_agg = inventory_agg.limit(10)
  inventory_agg.createOrReplaceTempView('inventory_agg')
  
  sales_invoice_headers_agg = sales_invoice_headers_agg.limit(10)
  sales_invoice_headers_agg.createOrReplaceTempView('sales_invoice_headers_agg')
  
  sales_invoice_lines = sales_invoice_lines.limit(10)
  sales_invoice_lines.createOrReplaceTempView('sales_invoice_lines_agg')

# COMMAND ----------

W_EXCH_RATE_G = spark.sql("""
  SELECT * FROM (
  SELECT 
    GL_DAILY_RATES.FROM_CURRENCY, 
    GL_DAILY_RATES.TO_CURRENCY, 
    GL_DAILY_RATES.CONVERSION_TYPE, 
    GL_DAILY_RATES.FROM_CURRENCY FROM_CURCY_CD,
    GL_DAILY_RATES.TO_CURRENCY TO_CURCY_CD,
    GL_DAILY_RATES.CONVERSION_TYPE RATE_TYPE,
    GL_DAILY_RATES.CONVERSION_RATE EXCH_RATE,
    MAX(GL_DAILY_RATES.CONVERSION_DATE) CONVERSION_DATE
  FROM 
    EBS.GL_DAILY_RATES
    where upper(CONVERSION_TYPE) = 'CORPORATE'
    and TO_CURRENCY = 'USD'
    --and from_currency = 'CAD'
    GROUP BY
          GL_DAILY_RATES.FROM_CURRENCY, 
    GL_DAILY_RATES.TO_CURRENCY, 
    GL_DAILY_RATES.CONVERSION_TYPE, 
    GL_DAILY_RATES.FROM_CURRENCY ,
    GL_DAILY_RATES.TO_CURRENCY ,
    GL_DAILY_RATES.CONVERSION_TYPE ,
    GL_DAILY_RATES.CONVERSION_RATE )
    WHERE  current_date <= conversion_date
""")
W_EXCH_RATE_G.createOrReplaceTempView("W_EXCH_RATE_G")

# COMMAND ----------

inv_qty = spark.sql("""
select 
  inventory_agg.item_ID,
  inventory_agg.inventoryWarehouse_ID,
  inventory_agg.owningBusinessUnit_ID,
  inventory_agg.productCode,
  -- inventory_agg.ansStdUomCode,
  product_agg.ansStdUom ansStdUomCode,
  inventory_agg.inventoryDate,
  sum(CASE WHEN SUBINVENTORYCODE NOT IN ('IN TRANSIT') THEN inventory_agg.ansStdQty END) x_onhand_qty,
  sum(CASE WHEN SUBINVENTORYCODE NOT IN ('IN TRANSIT') THEN inventory_agg.primaryQty END  * CASE WHEN SUBINVENTORYCODE NOT IN ('IN TRANSIT') THEN inventory_agg.stdCostPerUnitPrimary END * inventory_agg.exchangeRate) on_hand_see_through_cost,
  sum(CASE WHEN SUBINVENTORYCODE IN ('IN TRANSIT') THEN inventory_agg.ansStdQty END )  x_intransit_qty, 
  sum(CASE WHEN SUBINVENTORYCODE  IN ('IN TRANSIT') THEN inventory_agg.ansStdQty END * CASE WHEN SUBINVENTORYCODE  IN ('IN TRANSIT') THEN 
      inventory_agg.stdCostPerUnitPrimary END )  intransit_see_through_cost   ,
  sum(CASE WHEN SUBINVENTORYCODE NOT IN ('IN TRANSIT') THEN (inventory_agg.totalcost/inventory_agg.secondaryQty) END)  unit_cost_std_uom  
from 
  s_supplychain.inventory_agg
    join s_core.product_agg on inventory_agg.item_id = product_agg._id
where  inventory_agg._source = 'EBS'
and inventory_agg.inventoryDate = (select max(inventory_agg.inventoryDate) from  s_supplychain.inventory_agg
    where inventory_agg._source = 'EBS' ) 
  and  not inventory_agg._DELETED
group by
  inventory_agg.item_ID,
  inventory_agg.inventoryWarehouse_ID,
  inventory_agg.owningBusinessUnit_ID,
  inventory_agg.inventoryDate,
   -- inventory_agg.ansStdUomCode,
  product_agg.ansStdUom,
  inventory_agg.productCode
""")
inv_qty.createOrReplaceTempView("inv_qty")

# COMMAND ----------

int_qty = spark.sql("""
--intransit_qty
select
  INVENTORY_ORG,
  ITEM_NUMBER,
  prod.itemid,
  inv.organizationId,
  HR_OPERATING_UNITS.ORGANIZATION_ID owningBusinessUnitId,
  INV._ID inventoryWarehouse_ID,
  PROD._ID item_id,
  OU._ID owningBusinessUnit_ID,
  sum(IN_TRANSIT_QTY) IN_TRANSIT_QTY,
  sum(IN_TRANSIT_QTY_STD_UOM) IN_TRANSIT_QTY_STD_UOM,
  sum(COALESCE(GIC.ACCTG_COST, 0)) COST,
  sum(COALESCE(GIC.ACCTG_COST, 0) * IN_TRANSIT_QTY) INTRANSIT_SEE_THROUGH_COST
from
  g_fin_qv.wc_intransit_extract_fs int
  join s_core.organization_ebs inv on int.inventory_org = inv.organizationCode
  and inv.organizationType = 'INV'
  join s_core.product_ebs prod on int.item_number = prod.productcode
  LEFT JOIN EBS.GL_ITEM_CST GIC ON inv.organizationId = GIC.ORGANIZATION_ID
  AND prod.itemid = GIC.INVENTORY_ITEM_ID
  AND CURRENT_DATE -1 BETWEEN GIC.START_DATE
  AND GIC.END_DATE
  join ebs.HR_ORGANIZATION_INFORMATION HOI on HOI.ORGANIZATION_ID = inv.ORGANIZATIONID
  join EBS.HR_OPERATING_UNITS on HOI.ORG_INFORMATION3 = HR_OPERATING_UNITS.ORGANIZATION_ID
  join s_core.organization_ebs ou on HR_OPERATING_UNITS.ORGANIZATION_ID = ou.organizationId
  and ou.organizationType = 'OPERATING_UNIT'
where
  TRANSACTION_DATE in (
    select
      max(TRANSACTION_DATE)
    from
      g_fin_qv.wc_intransit_extract_fs
  )
  and not prod._deleted
  and not inv._deleted
  and int.ORDER_TYPE_TEXT = 'IN TRANSIT'
group by
  INVENTORY_ORG,
  ITEM_NUMBER,
  prod.itemid,
  inv.organizationId,
  HR_OPERATING_UNITS.ORGANIZATION_ID,
  INV._ID,
  PROD._ID,
  OU._ID
  """)
int_qty.createOrReplaceTempView("int_qty")

# COMMAND ----------

tmp_current_month = spark.sql("""
select 
  --invoiceDetailId, 
  item_id, 
  inventoryWarehouse_ID, 
  owningBusinessUnit_ID,
 -- ordTypeTransTypeCode,
 -- invTypeTransTypeCode,
 -- returnFlag,
 -- discountLineFlag,
  Sum(invoiced_qty) invoiced_qty
from (
select 
  invoiceDetailId,
  ordertype.transactionTypeCode ordTypeTransTypeCode,
  invoicetype.transactionTypeCode invTypeTransTypeCode,
  sales_invoice_lines_agg.inventoryWarehouse_ID,
  sales_invoice_lines_agg.owningBusinessUnit_ID,
  sales_invoice_lines_agg.item_id ,
  sales_invoice_lines_agg.returnFlag,
  sales_invoice_lines_agg.discountLineFlag,
  sales_invoice_lines_agg.quantityInvoiced *  sales_invoice_lines_agg.primaryUomConv    invoiced_qty
from
  s_supplychain.sales_invoice_headers_agg
  join s_supplychain.sales_invoice_lines_agg 
    on sales_invoice_lines_agg.Invoice_ID = sales_invoice_headers_agg._ID
  join s_core.account_agg 
    on sales_invoice_headers_agg.customer_ID = account_agg._ID
  join s_core.product_agg 
    on sales_invoice_lines_agg.item_id = product_agg._ID
  join s_core.transaction_type_agg  ordertype
    on sales_invoice_lines_agg.orderType_ID  = ordertype._ID
  join s_core.transaction_type_agg  invoicetype
    on sales_invoice_headers_agg.transaction_ID   = invoicetype._ID
where 1=1
   and date_Format(dateInvoiced, 'yyyyMM') = date_Format(add_months(current_date, 0), 'yyyyMM')
   and sales_invoice_headers_agg._source = 'EBS'
   and sales_invoice_lines_agg._source = 'EBS'
   and account_agg._source = 'EBS'
   and account_agg.customerType = 'External'
   and not sales_invoice_headers_agg._deleted
   and not sales_invoice_lines_agg._deleted
   and product_agg.itemType = 'FINISHED GOODS'
   and ordertype.description NOT LIKE '%Drop Shipment%' 
   and ordertype.description NOT LIKE '%Direct Shipment%'
  and sales_invoice_lines_agg.inventoryWarehouseID is not null
   ) AS A
GROUP BY 
  --invoiceDetailId, 
  item_id, 
  inventoryWarehouse_ID, 
  owningBusinessUnit_ID
  --ordTypeTransTypeCode,
  --invTypeTransTypeCode,
  --returnFlag,
  --discountLineFlag
""")

tmp_current_month.createOrReplaceTempView("current_month")

# COMMAND ----------

tmp_prev_mth = spark.sql("""
select  
  item_id, 
  inventoryWarehouse_ID, 
  owningBusinessUnit_ID,
  Sum(invoiced_qty) invoiced_qty
from (
select 
  invoiceDetailId,
  ordertype.transactionTypeCode ordTypeTransTypeCode,
  invoicetype.transactionTypeCode invTypeTransTypeCode,
  sales_invoice_lines_agg.inventoryWarehouse_ID,
  sales_invoice_lines_agg.owningBusinessUnit_ID,
  sales_invoice_lines_agg.item_id ,
  sales_invoice_lines_agg.returnFlag,
  sales_invoice_lines_agg.discountLineFlag,
  sales_invoice_lines_agg.quantityInvoiced *  sales_invoice_lines_agg.primaryUomConv    invoiced_qty
from
  s_supplychain.sales_invoice_headers_agg
  join s_supplychain.sales_invoice_lines_agg 
    on sales_invoice_lines_agg.Invoice_ID = sales_invoice_headers_agg._ID
  join s_core.account_agg 
    on sales_invoice_headers_agg.customer_ID = account_agg._ID
  join s_core.product_agg 
    on sales_invoice_lines_agg.item_id = product_agg._ID
  join s_core.transaction_type_agg  ordertype
    on sales_invoice_lines_agg.orderType_ID  = ordertype._ID
  join s_core.transaction_type_agg  invoicetype
    on sales_invoice_headers_agg.transaction_ID   = invoicetype._ID
where 1=1
   and date_Format(dateInvoiced, 'yyyyMM') = date_Format(add_months(current_date, -1), 'yyyyMM')
   and sales_invoice_headers_agg._source = 'EBS'
   and sales_invoice_lines_agg._source = 'EBS'
   and account_agg._source = 'EBS'
   and account_agg.customerType = 'External'
   and not sales_invoice_headers_agg._deleted
   and not sales_invoice_lines_agg._deleted
   and product_agg.itemType = 'FINISHED GOODS'
   and ordertype.description NOT LIKE '%Drop Shipment%' 
   and ordertype.description NOT LIKE '%Direct Shipment%'
   ) AS A
GROUP BY 
  item_id, 
  inventoryWarehouse_ID, 
  owningBusinessUnit_ID
""")

tmp_prev_mth.createOrReplaceTempView("prev_mth")

# COMMAND ----------

tmp_prev_2mth = spark.sql("""
select  
  item_id, 
  inventoryWarehouse_ID, 
  owningBusinessUnit_ID,
  Sum(invoiced_qty) invoiced_qty
from (
select 
  invoiceDetailId,
  ordertype.transactionTypeCode ordTypeTransTypeCode,
  invoicetype.transactionTypeCode invTypeTransTypeCode,
  sales_invoice_lines_agg.inventoryWarehouse_ID,
  sales_invoice_lines_agg.owningBusinessUnit_ID,
  sales_invoice_lines_agg.item_id ,
  sales_invoice_lines_agg.returnFlag,
  sales_invoice_lines_agg.discountLineFlag,
  sales_invoice_lines_agg.quantityInvoiced *  sales_invoice_lines_agg.primaryUomConv    invoiced_qty
from
  s_supplychain.sales_invoice_headers_agg
  join s_supplychain.sales_invoice_lines_agg 
    on sales_invoice_lines_agg.Invoice_ID = sales_invoice_headers_agg._ID
  join s_core.account_agg 
    on sales_invoice_headers_agg.customer_ID = account_agg._ID
  join s_core.product_agg 
    on sales_invoice_lines_agg.item_id = product_agg._ID
  join s_core.transaction_type_agg  ordertype
    on sales_invoice_lines_agg.orderType_ID  = ordertype._ID
  join s_core.transaction_type_agg  invoicetype
    on sales_invoice_headers_agg.transaction_ID   = invoicetype._ID
where 1=1
   and date_Format(dateInvoiced, 'yyyyMM') = date_Format(add_months(current_date, -2), 'yyyyMM')
   and sales_invoice_headers_agg._source = 'EBS'
   and sales_invoice_lines_agg._source = 'EBS'
   and account_agg._source = 'EBS'
   and account_agg.customerType = 'External'
   and not sales_invoice_headers_agg._deleted
   and not sales_invoice_lines_agg._deleted
   and product_agg.itemType = 'FINISHED GOODS'
   and ordertype.description NOT LIKE '%Drop Shipment%' 
   and ordertype.description NOT LIKE '%Direct Shipment%'
   ) AS A
GROUP BY 
  item_id, 
  inventoryWarehouse_ID, 
  owningBusinessUnit_ID
""")

tmp_prev_2mth.createOrReplaceTempView("prev_2mth")

# COMMAND ----------

tmp_prev_3mth = spark.sql("""
select 
  item_id, 
  inventoryWarehouse_ID, 
  owningBusinessUnit_ID,
  Sum(invoiced_qty) invoiced_qty
from (
select 
  invoiceDetailId,
  ordertype.transactionTypeCode ordTypeTransTypeCode,
  invoicetype.transactionTypeCode invTypeTransTypeCode,
  sales_invoice_lines_agg.inventoryWarehouse_ID,
  sales_invoice_lines_agg.owningBusinessUnit_ID,
  sales_invoice_lines_agg.item_id ,
  sales_invoice_lines_agg.returnFlag,
  sales_invoice_lines_agg.discountLineFlag,
  sales_invoice_lines_agg.quantityInvoiced *  sales_invoice_lines_agg.primaryUomConv    invoiced_qty
from
  s_supplychain.sales_invoice_headers_agg
  join s_supplychain.sales_invoice_lines_agg 
    on sales_invoice_lines_agg.Invoice_ID = sales_invoice_headers_agg._ID
  join s_core.account_agg 
    on sales_invoice_headers_agg.customer_ID = account_agg._ID
  join s_core.product_agg 
    on sales_invoice_lines_agg.item_id = product_agg._ID
  join s_core.transaction_type_agg  ordertype
    on sales_invoice_lines_agg.orderType_ID  = ordertype._ID
  join s_core.transaction_type_agg  invoicetype
    on sales_invoice_headers_agg.transaction_ID   = invoicetype._ID
where 1=1
   and date_Format(dateInvoiced, 'yyyyMM') = date_Format(add_months(current_date, -3), 'yyyyMM')
   and sales_invoice_headers_agg._source = 'EBS'
   and sales_invoice_lines_agg._source = 'EBS'
   and account_agg._source = 'EBS'
   and account_agg.customerType = 'External'
   and not sales_invoice_headers_agg._deleted
   and not sales_invoice_lines_agg._deleted
   and product_agg.itemType = 'FINISHED GOODS'
   and ordertype.description NOT LIKE '%Drop Shipment%' 
   and ordertype.description NOT LIKE '%Direct Shipment%'
   ) AS A
GROUP BY 
  item_id, 
  inventoryWarehouse_ID, 
  owningBusinessUnit_ID
""")

tmp_prev_3mth.createOrReplaceTempView("prev_3mth")

# COMMAND ----------

tmp_q1 = spark.sql("""
select distinct item_ID  ,inventoryWarehouse_ID  ,owningBusinessUnit_ID from (
      select DISTINCT item_ID  ,inventoryWarehouse_ID  ,owningBusinessUnit_ID  from inv_qty
union select DISTINCT item_ID  ,inventoryWarehouse_ID  ,owningBusinessUnit_ID  from current_month
Union select DISTINCT item_ID  ,inventoryWarehouse_ID  ,owningBusinessUnit_ID  from prev_mth
Union select DISTINCT item_ID  ,inventoryWarehouse_ID  ,owningBusinessUnit_ID  from prev_2mth
Union select DISTINCT item_ID  ,inventoryWarehouse_ID  ,owningBusinessUnit_ID  from prev_3mth
union select DISTINCT item_ID  ,inventoryWarehouse_ID  ,owningBusinessUnit_ID  from int_qty )
AS A
""")
tmp_q1.createOrReplaceTempView("tmp_q1")

# COMMAND ----------

main_inventory_extract = spark.sql("""Select 
    A.item_ID
  , A.inventoryWarehouse_ID
  , A.owningBusinessUnit_ID
  , A.MONTH_END_DATE
  , Case When warehouse.organizationCode In ('502', '503', '504', '511', '513', '517', '532') 
  Then '8' || Substr(warehouse.organizationCode, 2, 2) Else warehouse.organizationCode End  INVENTORY_ORG
  , warehouse.name INVENTORY_ORG_NAME
  , Case When warehouse.organizationCode In ('502', '503', '504', '511', '513', '517', '532') 
  Then 'ANSELL HEALTHCARE PRODUCTS LLC' Else operatingUnit.name End OPERATING_UNIT
  , product.productCode ITEM_NUMBER
  , product.name ITEM_DESCRIPTION
  , product.productDivision DIVISION
  , product.legacyAspn LEGACY_ASPN
  , product.launchDate LAUNCH_DATE
  , A.STD_UOM
  , A.ON_HAND_QTY
  , A.INTRANSIT_QTY
  , (A.INV_M0_QTY * product.ansStduomConv) INV_M0_QTY
  , (A.INV_M1_QTY * product.ansStduomConv) INV_M1_QTY
  , (A.INV_M2_QTY * product.ansStduomConv) INV_M2_QTY
  , (A.INV_M3_QTY * product.ansStduomConv) INV_M3_QTY
  , A.ONHAND_SEE_THROUGH_COST
  , A.INTRANSIT_SEE_THROUGH_COST
  , Date_format(A.Month_End_Date,"yyyyMMdd") || '~' || warehouse.organizationCode || '~' || product.productCode || '~' || operatingUnit.organizationCode INTEGRATION_ID
  , 999 as DATASOURCE_NUM_ID	
  , 0 ETL_PROC_WID 
  , W_EXCH_RATE_G.EXCH_RATE 
  ,(a.UNIT_COST_STD_UOM*nvl(W_EXCH_RATE_G.EXCH_RATE,1)) UNIT_COST_STD_UOM
  
from (
      select 
        last_day(current_date) MONTH_END_DATE
        ,tmp_q1.item_ID
        ,tmp_q1.inventoryWarehouse_ID
        ,tmp_q1.owningBusinessUnit_ID
        ,inv_qty.productCode
        ,inv_qty.ansStdUomCode STD_UOM
        ,inv_qty.inventoryDate
        ,inv_qty.x_onhand_qty ON_HAND_QTY
        ,inv_qty.on_hand_see_through_cost ONHAND_SEE_THROUGH_COST
        ,int_qty.IN_TRANSIT_QTY INTRANSIT_QTY
        ,int_qty.intransit_see_through_cost INTRANSIT_SEE_THROUGH_COST
        ,current_month.invoiced_qty INV_M0_QTY
        ,prev_mth.invoiced_qty INV_M1_QTY
        ,prev_2mth.invoiced_qty INV_M2_QTY
        ,prev_3mth.invoiced_qty INV_M3_QTY
        ,inv_qty.unit_cost_std_uom
      
      from tmp_q1
      left join inv_qty 
        on tmp_q1.item_id = inv_qty.item_id
          and tmp_q1.inventoryWarehouse_ID = inv_qty.inventoryWarehouse_ID
          and tmp_q1.owningBusinessUnit_ID = inv_qty.owningBusinessUnit_ID
      left join current_month
        on  tmp_q1.item_id = current_month.item_id
          and tmp_q1.inventoryWarehouse_ID = current_month.inventoryWarehouse_ID
          and tmp_q1.owningBusinessUnit_ID = current_month.owningBusinessUnit_ID
      left join prev_mth
        on  tmp_q1.item_id = prev_mth.item_id
          and tmp_q1.inventoryWarehouse_ID = prev_mth.inventoryWarehouse_ID
          and tmp_q1.owningBusinessUnit_ID = prev_mth.owningBusinessUnit_ID
      left join prev_2mth
        on  tmp_q1.item_id = prev_2mth.item_id
          and tmp_q1.inventoryWarehouse_ID = prev_2mth.inventoryWarehouse_ID
          and tmp_q1.owningBusinessUnit_ID = prev_2mth.owningBusinessUnit_ID
      left join prev_3mth
        on  tmp_q1.item_id = prev_3mth.item_id
          and tmp_q1.inventoryWarehouse_ID = prev_3mth.inventoryWarehouse_ID
          and tmp_q1.owningBusinessUnit_ID = prev_3mth.owningBusinessUnit_ID
      left join int_qty 
  on tmp_q1.item_id = int_qty.item_id
          and tmp_q1.inventoryWarehouse_ID = int_qty.inventoryWarehouse_ID
          and tmp_q1.owningBusinessUnit_ID = int_qty.owningBusinessUnit_ID
  
    ) AS A
Join s_core.product_agg product
    on A.item_id = product._id
join s_core.organization_agg warehouse
    on A.inventoryWarehouse_ID = warehouse._ID
    and warehouse.organizationType = 'INV' 
join s_core.organization_agg operatingUnit
    on A.owningBusinessUnit_ID = operatingUnit._ID
    and operatingUnit.organizationType = 'OPERATING_UNIT' 
left join W_EXCH_RATE_G on warehouse.currency = W_EXCH_RATE_G.FROM_CURCY_CD
    where product.itemtype='FINISHED GOODS'
    
Order By 
      product.productCode 
    , warehouse.organizationCode
    , operatingUnit.organizationCode
    """)
main_inventory_extract.createOrReplaceTempView("main_inventory_extract")

# COMMAND ----------

main_inventory_extract.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_inventory_extract
   .transform(attach_partition_column("MONTH_END_DATE"))
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

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.inventory_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.inventory_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_headers_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_headers_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_invoice_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_invoice_lines_agg')
