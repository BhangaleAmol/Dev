# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.order_intake

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = True)
key_columns = get_input_param('key_columns', 'string', default_value = '_ID')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'order_intake')
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
  oh.createdOn Creation_Date,
  oh.orderDate Order_Date,
  ol.salesorderDetailId Line_ID,
  ol._ID _ID,
  ol.orderLineStatus Order_Status,
  ac.accountNumber Customer_Number,
  ac.name Customer_Name,
  pr.productCode Product,
  pr.Name Product_Name,
  pr.productStyle Style,
  pr.productBrand Brand,
  pr.productSbu SBU,
  country.region Region,
  country.subRegion Sub_Region,
  oh.distributionChannel Channel,
  inv.organizationCode Warehouse,
  origin.originId Origin_Code,
  origin.originName Origin_Name,
  oh.baseCurrencyId Ordered_Currency,
  ol.quantityOrdered * ol.ansStdUomConv Ordered_Qty_Ans_Std_Uom,
  ol.orderAmount Ordered_Amount_Doc_Currency,
  ol.orderAmount / oh.exchangeSpotRate Ordered_Amount_USD
 
 from
   sales_order_headers_agg oh
   join s_supplychain.sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
   join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
   join s_core.product_agg pr on ol.item_ID = pr._ID
   join s_core.account_agg ac on oh.customer_ID = ac._ID
   join s_core.customer_location_agg st on ol.shipToAddress_ID = st._ID
   join s_core.transaction_type_agg tt on oh.orderType_ID = tt._ID
   left join s_core.origin_agg origin on pr.origin_id = origin._id
   left join s_core.country_agg country on st.countryCode_ID = country._ID
   join s_core.date on date_format(oh.orderDate,'yyyyMMdd') = date.dayid 
 where
   oh._source in ('COL', 'EBS', 'KGD', 'SAP' 'TOT')
   and not OL._DELETED
   and not OH._DELETED
   and date.fiscalYearId >='2021'
   and ol.bookedflag = 'Y'
    """)
   

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("Creation_Date"))
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


full_order_keys = spark.sql("""
  SELECT sales_order_lines._id from s_supplychain.sales_order_lines_agg sales_order_lines
""")

full_keys_f = (
 full_order_keys
  .select('_ID')
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_headers_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_headers_agg')

  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_lines_agg')
