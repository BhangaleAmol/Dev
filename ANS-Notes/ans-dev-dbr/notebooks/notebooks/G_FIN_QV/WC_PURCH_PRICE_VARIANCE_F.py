# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_purch_price_variance_f

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'INVENTORY_ITEM_ID,ORGANIZATION_ID,TRANSACTION_ID')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_purch_price_variance_f')
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
source_table = 'ebs.po_headers_all'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  po_headers_all = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  po_headers_all = load_full_dataset(source_table)
  
po_headers_all.createOrReplaceTempView('po_headers_all')
po_headers_all.display()

# COMMAND ----------

std_cost=spark.sql("""
SELECT
  dtl.inventory_item_id inventory_item_id,
  dtl.organization_id organization_id,
  dtl.period_id period_id,
  date_format(gmf.start_date, 'yyyyMMdd') start_date,
  date_format(gmf.end_date, 'yyyyMMdd') end_date,
  SUM(
    case
      when COST_CMPNTCLS_DESC not in ('Admistrative Overhead', 'Markup') then NVL(cmpnt_cost, 0)
      else 0
    end
  ) standard_cost,
  SUM(
    case
      when COST_CMPNTCLS_DESC in ('Admistrative Overhead', 'Markup') then NVL(cmpnt_cost, 0)
      else 0
    end
  ) markup,
  SUM(NVL(cmpnt_cost, 0)) total_cost,
  pol.base_currency_code base_curr
FROM
  ebs.cm_cmpt_mst_tl mst,
  ebs.cm_cmpt_dtl dtl,
  ebs.gmf_period_statuses gmf,
  ebs.gmf_fiscal_policies pol
WHERE
  mst.cost_cmpntcls_id = dtl.cost_cmpntcls_id
  AND dtl.period_id = gmf.period_id --and mst.usage_ind = 1 /* Removed as part of ticket 333 */
  AND POL.LEGAL_ENTITY_ID = GMF.LEGAL_ENTITY_ID
  and cost_cmpntcls_desc in (
    'Purchased Finished Good from US Supplier',
    'Asian/HK Factory Standard',
    'US Factory Std',
    'Admistrative Overhead',
    'Markup',
    'Raw Material',
    'Purchased Finished Good',
    'Purchased Condom Strips'
  )
  AND gmf.cost_type_id = 1000
  and mst.language = 'US'
GROUP BY
  dtl.inventory_item_id,
  dtl.organization_id,
  dtl.period_id,
  date_format(gmf.start_date, 'yyyyMMdd'),
  date_format(gmf.end_date, 'yyyyMMdd'),
  pol.base_currency_code
   """)
std_cost.createOrReplaceTempView('std_cost')

# COMMAND ----------

conv_from_item=spark.sql("""
SELECT
  inventory_item_id,
  unit_of_measure,
  SUM(NVL(conversion_rate, 0)) conversion_rate
FROM
  ebs.MTL_UOM_CONVERSIONS
GROUP BY
  unit_of_measure,
  inventory_item_id
  """)
conv_from_item .createOrReplaceTempView('conv_from_item')

# COMMAND ----------

conv_from=spark.sql("""
SELECT
  unit_of_measure,
  SUM(conversion_rate) conversion_rate
FROM
  ebs.MTL_UOM_CONVERSIONS
WHERE
  inventory_item_id = 0
GROUP BY
  unit_of_measure
    """)
conv_from.createOrReplaceTempView('conv_from')

# COMMAND ----------

conv_to_item=spark.sql("""
SELECT
  inventory_item_id,
  unit_of_measure,
  SUM(conversion_rate) conversion_rate
FROM
  ebs.MTL_UOM_CONVERSIONS
GROUP BY
  unit_of_measure,
  inventory_item_id
  """)
conv_to_item.createOrReplaceTempView('conv_to_item')

# COMMAND ----------

get_currency=spark.sql("""
SELECT
  DISTINCT transaction_id,
  date_format(transaction_date, 'yyyyMMdd') transaction_date,
  rcv_transactions.currency_code po_currency,
  pol.base_currency_code base_currency,
  CASE
    WHEN RCV_TRANSACTIONS.SOURCE_DOC_QUANTITY IS NOT NULL THEN source_doc_unit_of_measure
    ELSE RCV_TRANSACTIONS.UNIT_OF_MEASURE
  END AS UOM,
  po_lines_all.item_id,
  RCV_TRANSACTIONS.ORGANIZATION_ID
FROM
  ebs.RCV_TRANSACTIONS,
  ebs.po_lines_all,
  ebs.cm_cmpt_dtl dtl,
  ebs.gmf_period_statuses gmf,
  ebs.gmf_fiscal_policies pol
WHERE
  po_lines_all.po_line_id = rcv_transactions.po_line_id
  AND po_lines_all.item_id = dtl.inventory_item_id
  AND rcv_transactions.organization_id = dtl.organization_id
  AND dtl.period_id = gmf.period_id
  AND rcv_transactions.transaction_date BETWEEN gmf.start_date
  AND gmf.end_date
  AND gmf.legal_entity_id = pol.legal_entity_id
   """)
get_currency.createOrReplaceTempView('get_currency')

# COMMAND ----------

conv_to=spark.sql("""
SELECT
  unit_of_measure,
  SUM(conversion_rate) conversion_rate
FROM
  ebs.MTL_UOM_CONVERSIONS
WHERE
  inventory_item_id = 0
GROUP BY
  unit_of_measure
  """)
conv_to.createOrReplaceTempView('conv_to')

# COMMAND ----------

curr_conv=spark.sql("""
SELECT
  from_currency,
  to_currency,
  date_format(conversion_date, 'yyyyMMdd') conversion_date,
  conversion_rate
FROM
 ebs.GL_DAILY_RATES
WHERE
  upper(conversion_type) = '1002'
  """)
curr_conv.createOrReplaceTempView('curr_conv')

# COMMAND ----------

employee=spark.sql("""
select
  person_id person_id,
  max(full_name) full_name
from
  ebs.xx_per_all_people_f
group by
  person_id
""")
employee.createOrReplaceTempView('employee')

# COMMAND ----------

ebs_division_reference=spark.sql("""
select 
   mtl_system_items_b.inventory_item_id inventory_item_id,
   mtl_categories_b.segment1 productDivision,
   mtl_categories_b.segment2 TransferPriceCategory
   from 
    ebs.mtl_category_sets_tl,
    ebs.mtl_category_sets_b,
    ebs.mtl_item_categories,
    ebs.mtl_categories_b,
    ebs.mtl_system_items_b
   where mtl_category_sets_tl.language = 'US'
   and mtl_category_sets_tl.category_set_name = 'Division and Transfer Price'
   and mtl_category_sets_tl.category_set_id = mtl_category_sets_b.category_set_id
   and mtl_item_categories.category_set_id =  mtl_category_sets_b.category_set_id
   and mtl_item_categories.category_id = mtl_categories_b.category_id
   and mtl_system_items_b.organization_id = 124
   and mtl_item_categories.organization_id = 124
   and mtl_system_items_b.inventory_item_id = mtl_item_categories.inventory_item_id
 """)
ebs_division_reference.createOrReplaceTempView('ebs_division_reference')

# COMMAND ----------

ebs_brand_reference=spark.sql("""
select 
   mtl_system_items_b.inventory_item_id inventory_item_id,
   mtl_categories_b.segment1 brandStrategy,
   mtl_categories_b.segment2 masterBrand,
   mtl_categories_b.segment3 brand,
   mtl_categories_b.segment4 subBrand,
   mtl_categories_b.segment5 style
   from 
    ebs.mtl_category_sets_tl,
    ebs.mtl_category_sets_b,
    ebs.mtl_item_categories,
    ebs.mtl_categories_b,
    ebs.mtl_system_items_b
   where mtl_category_sets_tl.language = 'US'
   and mtl_category_sets_tl.category_set_name = 'Brand'
   and mtl_category_sets_tl.category_set_id = mtl_category_sets_b.category_set_id
   and mtl_item_categories.category_set_id =  mtl_category_sets_b.category_set_id
   and mtl_item_categories.category_id = mtl_categories_b.category_id
   and mtl_system_items_b.organization_id = 124
   and mtl_item_categories.organization_id = 124
   and mtl_system_items_b.inventory_item_id = mtl_item_categories.inventory_item_id
   --and mtl_system_items_b.segment1 = '838060'
 """)
ebs_brand_reference.createOrReplaceTempView('ebs_brand_reference')

# COMMAND ----------

ebs_productcat_reference=spark.sql("""
select 
   mtl_system_items_b.inventory_item_id inventory_item_id,
   mtl_categories_b.segment1 productCategory,
   mtl_categories_b.segment2 productSubCategory
   from 
    ebs.mtl_category_sets_tl,
    ebs.mtl_category_sets_b,
    ebs.mtl_item_categories,
    ebs.mtl_categories_b,
    ebs.mtl_system_items_b
   where mtl_category_sets_tl.language = 'US'
   and mtl_category_sets_tl.category_set_name = 'Inventory'
   and mtl_category_sets_tl.category_set_id = mtl_category_sets_b.category_set_id
   and mtl_item_categories.category_set_id =  mtl_category_sets_b.category_set_id
   and mtl_item_categories.category_id = mtl_categories_b.category_id
   and mtl_system_items_b.organization_id = 124
   and mtl_item_categories.organization_id = 124
   and mtl_system_items_b.inventory_item_id = mtl_item_categories.inventory_item_id
   --and mtl_system_items_b.segment1 = '838060'
 """)
ebs_productcat_reference.createOrReplaceTempView('ebs_productcat_reference')

# COMMAND ----------

xla_details=spark.sql("""
SELECT
  DISTINCT po_lines.po_header_id po_header_id,
  po_lines.po_line_id po_line_id,
  xla_lines.transaction_id,
  xla_lines.amount,
  PARENT_TRANSACTION_ID
from
  (
    select
      geh.transaction_id,
      nvl(al.accounted_cr, al.accounted_dr) amount
    FROM
      ebs.xla_ae_lines al,
      ebs.xla_ae_headers ah,
      ebs.xla_distribution_links dl,
      ebs.gmf_xla_extract_headers geh,
      ebs.gmf_xla_extract_lines gel,
      ebs.gl_code_combinations glcc
    WHERE
      al.ae_header_id = dl.ae_header_id
      AND ah.ae_header_id = al.ae_header_id
      AND ah.ae_header_id = dl.ae_header_id
      AND ah.application_id = 555 
      AND al.ae_line_num = dl.ae_line_num
      AND dl.event_id = geh.event_id
      AND dl.application_id = 555 
      AND dl.source_distribution_type = geh.entity_code
      AND dl.source_distribution_id_num_1 = gel.line_id
      AND geh.header_id = gel.header_id
      AND geh.event_id = gel.event_id
      AND glcc.code_combination_id = al.code_combination_id 
      AND al.accounting_class_code = 'RECEIVING_INSPECTION'
      AND geh.entity_code = 'PURCHASING'
      AND geh.event_class_code = 'DELIVER'
  ) xla_lines,
  
  (
    SELECT
      mmt.transaction_id transaction_id,
      PLA.PO_HEADER_ID,
      pla.po_line_id,
      RT.PARENT_TRANSACTION_ID
    FROM
      ebs.rcv_transactions rt,
      ebs.rcv_shipment_headers rsh,
      ebs.rcv_shipment_lines rsl,
      ebs.po_lines_all pla,
      ebs.po_headers_all pha,
      ebs.po_line_locations_all plla,
      ebs.po_distributions_all pda,
      ebs.mtl_material_transactions mmt
    WHERE
      1 = 1 --and rt.po_header_id = 151400
      AND rt.shipment_header_id = rsh.shipment_header_id
      AND rt.transaction_type = 'DELIVER'
      AND rsl.shipment_header_id = rsh.shipment_header_id
      AND rt.po_line_id = rsl.po_line_id
      AND rt.po_line_id = pla.po_line_id
      AND pla.po_header_id = pha.po_header_id
      AND pla.po_line_id = plla.po_line_id
      AND plla.line_location_id = pda.line_location_id
      AND rt.transaction_id = mmt.rcv_transaction_id 
  ) po_lines
where
  xla_lines.transaction_id = po_lines.transaction_id 
  """)
xla_details.createOrReplaceTempView('xla_details')

# COMMAND ----------

main_ppv = spark.sql("""
SELECT
  date_format(
    add_months(RCV_TRANSACTIONS.TRANSACTION_DATE, 6),
    'yyyy-MM'
  ) FISCAL_PERIOD,
  date_format(
    add_months(RCV_TRANSACTIONS.TRANSACTION_DATE, 6),
    'yyyy'
  ) FISCAL_YEAR,
  mtl_parameters.organization_code INVENTORY_ORG_CODE,
  haou2.name INVENTORY_ORG_NAME,
  haou1.name OPERATING_UNIT,
  gl_codes.segment2 AS GL_ACCOUNT,
  PO_HEADERS_ALL.VENDOR_ID VENDOR_NUMBER,
  suppliers.vendor_name VENDOR_NAME,
  MTL_SYSTEM_ITEMS_B.segment1 AS PRODUCT_NUMBER,
  MTL_SYSTEM_ITEMS_B.DESCRIPTION PRODUCT_NAME,
  MTL_SYSTEM_ITEMS_B.item_type PRODUCT_TYPE,
  MTL_SYSTEM_ITEMS_B.primary_unit_of_measure PRIMARY_UOM,
  PO_DISTRIBUTIONS_ALL.CREATION_DATE AS PO_DATE,
  po_headers_all.segment1 AS PO_NUMBER,
  PO_LINES_ALL.LINE_NUM AS PO_LINE_NUMBER,
  RCV_SHIPMENT_HEADERS.RECEIPT_NUM AS RECEIVING_NUMBER,
  RCV_TRANSACTIONS.TRANSACTION_DATE AS TRANSACTION_DATE,
  RCV_TRANSACTIONS.CURRENCY_CODE AS PURCHASE_CURRENCY,
  CASE
    WHEN RCV_TRANSACTIONS.SOURCE_DOC_QUANTITY IS NOT NULL THEN source_doc_unit_of_measure
    ELSE RCV_TRANSACTIONS.UNIT_OF_MEASURE
  END AS PURCHASE_UOM,
  NVL(
    RCV_TRANSACTIONS.SOURCE_DOC_QUANTITY,
    RCV_TRANSACTIONS.QUANTITY
  ) AS UNITS_RECEIVED_PURCH_UOM,
  std_cost.standard_cost AS STD_COST,
  std_cost.markup AS MARKUP,
  std_cost.total_cost AS TOTAL_COST,
  std_cost.base_curr BASE_CURR,
  RCV_TRANSACTIONS.UNIT_OF_MEASURE,
  RCV_TRANSACTIONS.PO_UNIT_PRICE AS PO_COST_UNIT,
  --ACTUAL_COST_UNIT,
  CASE
    WHEN XLA.AMOUNT IS NULL THEN (
      NVL(
        RCV_TRANSACTIONS.SOURCE_DOC_QUANTITY,
        RCV_TRANSACTIONS.QUANTITY
      ) * RCV_TRANSACTIONS.PO_UNIT_PRICE
    )
    ELSE (XLA.AMOUNT / NVL(curr_conv.conversion_rate, 1))
  END AS TRANSACTION_AMT_TR_CURR,
  CASE
    WHEN XLA.AMOUNT IS NULL THEN (
      NVL(
        RCV_TRANSACTIONS.SOURCE_DOC_QUANTITY,
        RCV_TRANSACTIONS.QUANTITY
      ) * RCV_TRANSACTIONS.PO_UNIT_PRICE * NVL(curr_conv.conversion_rate, 1)
    )
    ELSE XLA.AMOUNT
  END AS TRANSACTION_AMT_LE_CURR,
  RCV_TRANSACTIONS.PRIMARY_QUANTITY AS UNITS_RCV_PRIMARY_UOM,
  RCV_TRANSACTIONS.primary_quantity * std_cost.total_cost AS EXT_STD_COST,
  employee.full_name EMPLOYEE_NAME,
  MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID,
  RCV_TRANSACTIONS.TRANSACTION_ID,
  RCV_TRANSACTIONS.LAST_UPDATE_DATE,
  RCV_TRANSACTIONS.LAST_UPDATED_BY,
  RCV_TRANSACTIONS.CREATION_DATE,
  RCV_TRANSACTIONS.CREATED_BY,
  RCV_TRANSACTIONS.ORGANIZATION_ID,
  NVL(curr_conv.conversion_rate, 1) CONVERSION_RATE,
  PO_LINES_ALL.PO_HEADER_ID,
  PO_LINES_ALL.PO_LINE_ID,
  gl_codes.segment1 GL_COMPANY,
  gl_codes.segment3 GL_COST_CENTRE,
  gl_codes.segment4 GL_DIVISION,
  gl_codes.segment5 GL_REGION,
  gl_codes.segment6 GL_IC,
  gl_codes.segment7 GL_FUTURE,
  PO_DISTRIBUTIONS_ALL.code_combination_id CODE_COMBINATION_ID,
  ebs_division_reference.productDivision PRODUCT_DIVISION_DH,
  ebs_division_reference.TransferPriceCategory TRANSFER_PRICE_CATEGORY_DH,
  ebs_brand_reference.brand BRAND_DH,
  ebs_brand_reference.brandStrategy BRAND_TYPE,
  ebs_brand_reference.masterBrand MASTER_BRAND,
  ebs_productcat_reference.productCategory PRODUCT_FAMILY,
  EXCHANGE_RATE_AGG.exchangeRate EXCHANGERATE_USD,
  ebs_brand_reference.subBrand SUB_BRAND_DH
  
  
FROM
  ebs.PO_HEADERS_ALL
  join ebs.PO_LINES_ALL on PO_HEADERS_ALL.PO_HEADER_ID = PO_LINES_ALL.PO_HEADER_ID
  left join ebs.RCV_TRANSACTIONS on PO_LINES_ALL.PO_HEADER_ID = RCV_TRANSACTIONS.PO_HEADER_ID
  AND PO_LINES_ALL.PO_LINE_ID = RCV_TRANSACTIONS.PO_LINE_ID
  join ebs.MTL_SYSTEM_ITEMS_B on PO_LINES_ALL.item_id = MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID
  join ebs.PO_DISTRIBUTIONS_ALL on PO_DISTRIBUTIONS_ALL.po_distribution_id = RCV_TRANSACTIONS.PO_DISTRIBUTION_ID
  join ebs.gl_code_combinations gl_codes on gl_codes.code_combination_id = PO_DISTRIBUTIONS_ALL.code_combination_id
  join ebs.hr_all_organization_units haou1 on haou1.organization_id = PO_HEADERS_ALL.org_id
  join ebs.hr_all_organization_units haou2 on haou2.organization_id = RCV_TRANSACTIONS.ORGANIZATION_ID
  join ebs.RCV_SHIPMENT_HEADERS on RCV_TRANSACTIONS.SHIPMENT_HEADER_ID = RCV_SHIPMENT_HEADERS.SHIPMENT_HEADER_ID
  left join conv_to_item on MTL_SYSTEM_ITEMS_B.inventory_item_id = conv_to_item.inventory_item_id
  AND MTL_SYSTEM_ITEMS_B.PRIMARY_UNIT_OF_MEASURE = CONV_TO_ITEM.UNIT_OF_MEASURE
  left join conv_to on conv_to.unit_of_measure = MTL_SYSTEM_ITEMS_B.primary_unit_of_measure
  join get_currency on rcv_transactions.transaction_id = get_currency.transaction_id
  left join conv_from_item on get_currency.item_id = conv_from_item.inventory_item_id
  AND get_currency.UOM = conv_from_item.unit_of_measure
  join conv_from on get_currency.UOM = conv_from.unit_of_measure
  left join std_cost on get_currency.item_id = std_cost.inventory_item_id
  AND get_currency.ORGANIZATION_ID = std_cost.organization_id
  AND get_currency.TRANSACTION_DATE BETWEEN std_cost.start_date
  AND std_cost.end_date
  left join curr_conv on get_currency.po_currency = curr_conv.from_currency
  AND get_currency.base_currency = curr_conv.to_currency
  AND get_currency.TRANSACTION_DATE = curr_conv.conversion_date
  join employee on PO_HEADERS_ALL.AGENT_ID = EMPLOYEE.PERSON_ID
  join ebs.ap_suppliers suppliers on SUPPLIERS.VENDOR_ID = PO_HEADERS_ALL.VENDOR_ID
  join ebs.mtl_parameters on RCV_TRANSACTIONS.ORGANIZATION_ID = MTL_PARAMETERS.ORGANIZATION_ID
  left join xla_details xla on xla.PARENT_TRANSACTION_ID = RCV_TRANSACTIONS.TRANSACTION_ID
  and PO_LINES_ALL.PO_HEADER_ID = xla.po_header_id
  and PO_LINES_ALL.PO_LINE_ID = xla.po_line_id
  left join ebs_division_reference on MTL_SYSTEM_ITEMS_B.inventory_item_id = ebs_division_reference.inventory_item_id
  left join ebs_brand_reference on MTL_SYSTEM_ITEMS_B.inventory_item_id = ebs_brand_reference.inventory_item_id
  left join ebs_productcat_reference on MTL_SYSTEM_ITEMS_B.inventory_item_id = ebs_productcat_reference.inventory_item_id
  left join S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(RCV_TRANSACTIONS.CURRENCY_CODE)) = 0 then 'USD' else  trim(RCV_TRANSACTIONS.CURRENCY_CODE) end || '-USD-' || date_format(RCV_TRANSACTIONS.TRANSACTION_DATE, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
WHERE
  MTL_SYSTEM_ITEMS_B.ORGANIZATION_ID = 124
  AND RCV_TRANSACTIONS.TRANSACTION_TYPE IN (
    'RECEIVE',
    'RETURN TO VENDOR',
    'CORRECT',
    'MATCH'
  )
  and not (
    RCV_TRANSACTIONS.TRANSACTION_TYPE = 'CORRECT'
    and RCV_TRANSACTIONS.DESTINATION_TYPE_CODE = 'INVENTORY'
  )
  
  """)
main_ppv.createOrReplaceTempView("main_ppv")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_ppv
   .transform(attach_partition_column("FISCAL_PERIOD"))
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
