# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'lom_aa_do_l1')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/g_tembo/full_data')

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
source_table = 's_supplychain.sales_order_lines_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)
  sales_order_lines_df = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  sales_order_lines_df = load_full_dataset(source_table)

sales_order_lines_df.createOrReplaceTempView('sales_order_lines_df')
sales_order_lines_df.display()

# COMMAND ----------

# SAMPLING
if sampling:
  sales_order_lines_df = fact_customer_activity_df.limit(10)
  sales_order_lines_df.createOrReplaceTempView('sales_order_lines_df')

# COMMAND ----------

main_current_1 = spark.sql("""
select
  acc.accountnumber,
  pr.productCode || '-' || coalesce(
    ol.e2eDistributionCentre,
    inv.commonOrganizationCode,
    inv.organizationcode
  ) || '-' || orderType.dropShipmentFlag key,
  pr.productCode,
  pr.productStatus,
  case
    when nvl(acc.gbu, 'X') = 'NV&AC' then 'I'
    else left(nvl(acc.gbu, acc_org.salesoffice), 1)
  end customerGbu,
  coalesce(
    acc_org.forecastGroup,
    acc.forecastGroup,
    '#Error7' || '(' || oh.orderNumber || ')'
  ) forecastGroup,
  billtoTerritory.region Forecast_Planner,
  case
    when nvl(billto.siteCategory, 'X') = 'LA' then 'OLAC'
    else nvl(billtoTerritory.subRegion, sapSoldTo.subRegion)
  end subRegion,
  coalesce(
    ol.e2eDistributionCentre,
    inv.commonOrganizationCode,
    inv.organizationcode
  ) DC,
  orderType.dropShipmentFlag Channel,
  case
    when nvl(acc.gbu, 'X') = 'NV&AC' then 'I'
    else left(nvl(acc.gbu, acc_org.salesoffice), 1)
  end || '_' || coalesce(
    acc_org.forecastGroup,
    acc.forecastGroup,
    'Other'
  ) Forecast_3_ID,
  billtoTerritory.territoryCode,
  billtoTerritory.subRegionGis,
  case
    when nvl(billto.siteCategory, 'X') = 'LA' then 'LAC'
    else nvl(billtoTerritory.Region, sapSoldTo.Region)
  end Region,
  case
    when nvl(billto.siteCategory, 'X') = 'LA' then 'OLAC'
    else billtoTerritory.forecastArea
  end forecastArea,
  ol._SOURCE,
  oh.orderNumber,
  case
    when pr_org.mtsMtoFlag is null then '#Error20' || '(' || oh.orderNumber || ')'
    when pr_org.mtsMtoFlag not in ('MTS', 'MTO') then '#Error21' || '(' || oh.orderNumber || ')'
    else pr_org.mtsMtoFlag
  end mtsMtoFlag,
  sum(ol.quantityOrdered * ol.ansStdUomConv) quantityOrdered,
  sum(ol.orderAmount / nvl(oh.exchangeRateUsd, 1)) orderAmountUsd,
  year(ol.requestDeliveryBy) yearRequestDeliveryBy,
  month(ol.requestDeliveryBy) monthRequestDeliveryBy
from
  sales_order_lines_df ol
  join s_supplychain.sales_order_headers_agg oh on ol.salesorder_id = oh._ID
  join s_core.product_agg pr on ol.item_ID = pr._ID
  join s_core.account_agg acc on oh.customer_ID = acc._ID
  left join s_core.customer_location_agg billto on oh.billtoAddress_ID = billto._ID
  join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
  join s_core.transaction_type_agg orderType on oh.orderType_ID = orderType._id
  left join s_core.territory_agg billtoTerritory on billto.territory_ID = billtoTerritory._id
  left join s_core.account_organization_agg acc_org on oh.customerOrganization_ID = acc_org._id --oh.customerId = acc_org.accountId -- to be replaced with join of surrogate key
  --and oh.customerDivision = acc_org.customerDivision
  --and oh.salesOrganization = acc_org.salesOrganization
  --and oh.distributionChannel = acc_org.distributionChannel
  -- temp solution until SAP sold to records will be loaded
  left join s_core.territory_agg sapSoldTo on acc.address1Country = sapSoldTo.territorycode
  left join s_core.product_org_agg pr_org on ol.item_ID = pr_org.item_ID
  and ol.inventoryWarehouse_ID = pr_org.organization_ID
where
  not ol._deleted
  and not oh._deleted
  and oh._source in ('SAP', 'EBS')
  and coalesce(
    ol.e2eDistributionCentre,
    inv.commonOrganizationCode,
    inv.organizationcode
  ) not in ('325', '355')
  and year(ol.requestDeliveryBy) * 100 + month(ol.requestDeliveryBy) >= year(current_date) * 100 + month(current_date)
  and (
    nvl(orderType.e2eFlag, true)
    or ol.e2edistributionCentre = 'DC827'
  )
  and pr.itemType not in ('Service')
  and (
    ol.quantityOrdered <> 0
    or cancelReason like '%COVID-19%'
    or cancelReason like '%PLANT DIRECT BILLING%'
    or cancelReason like '%Plant Direct Billing Cancellation%'
  )
  and nvl(pr.productdivision, 'Include') not in ('SH&WB')
  and nvl(acc.customerType, 'External') not in ('Internal')
  and oh.customerId is not null
  and pr.productCode not like 'PALLET%'
  and pr.itemType in ('FINISHED GOODS', 'ACCESSORIES', 'ZPRF')
  and orderType.name not like 'AIT_DIRECT SHIPMENT%'
  and ol.bookedFlag = 'Y' -- and pr.productcode = '103653'
group by
  acc.accountnumber,
  pr.productCode || '-' || coalesce(
    ol.e2eDistributionCentre,
    inv.commonOrganizationCode,
    inv.organizationcode
  ) || '-' || dropShipmentFlag,
  pr.productCode,
  billtoTerritory.region,
  pr.productStatus,
  case
    when nvl(acc.gbu, 'X') = 'NV&AC' then 'I'
    else left(nvl(acc.gbu, acc_org.salesoffice), 1)
  end,
  coalesce(
    acc_org.forecastGroup,
    acc.forecastGroup,
    '#Error7' || '(' || oh.orderNumber || ')'
  ),
  case
    when nvl(billto.siteCategory, 'X') = 'LA' then 'OLAC'
    else nvl(billtoTerritory.subRegion, sapSoldTo.subRegion)
  end,
  coalesce(
    ol.e2eDistributionCentre,
    inv.commonOrganizationCode,
    inv.organizationcode
  ),
  orderType.dropShipmentFlag,
  case
    when nvl(acc.gbu, 'X') = 'NV&AC' then 'I'
    else left(nvl(acc.gbu, acc_org.salesoffice), 1)
  end || '_' || coalesce(
    acc_org.forecastGroup,
    acc.forecastGroup,
    'Other'
  ),
  billtoTerritory.territoryCode,
  billtoTerritory.subRegionGis,
  case
    when nvl(billto.siteCategory, 'X') = 'LA' then 'LAC'
    else nvl(billtoTerritory.Region, sapSoldTo.Region)
  end,
  case
    when nvl(billto.siteCategory, 'X') = 'LA' then 'OLAC'
    else billtoTerritory.forecastArea
  end,
  ol._SOURCE,
  oh.orderNumber,
  case
    when pr_org.mtsMtoFlag is null then '#Error20' || '(' || oh.orderNumber || ')'
    when pr_org.mtsMtoFlag not in ('MTS', 'MTO') then '#Error21' || '(' || oh.orderNumber || ')'
    else pr_org.mtsMtoFlag
  end,
  year(ol.requestDeliveryBy),
  month(ol.requestDeliveryBy)
""")
  
main_current_1.createOrReplaceTempView('main_current_1')
main_current_1.display()

# COMMAND ----------

gtc_replacement = spark.sql("""
  select ORACLE_PRODUCT_ID_OR_GTC_ID, min(item) item 
  from pdh.master_records 
  where nvl(STYLE_ITEM_FLAG, 'N') = 'N' 
    and ORACLE_PRODUCT_ID_OR_GTC_ID not in (select item from pdh.master_records where nvl(STYLE_ITEM_FLAG, 'N') = 'N' and ITEM_STATUS not in ('Inactive', 'Discontinue'))
    and ITEM_STATUS not in ('Inactive', 'Discontinue')
    and ORACLE_PRODUCT_ID_OR_GTC_ID not in ('GTC')
    and ORACLE_PRODUCT_ID_OR_GTC_ID not like 'Rule generated%'
  group by 
  ORACLE_PRODUCT_ID_OR_GTC_ID
""")
  
gtc_replacement.createOrReplaceTempView('gtc_replacement')
gtc_replacement.display()

# COMMAND ----------

main_2a = spark.sql("""
select 
  main_2._SOURCE,
  nvl(gtc_replacement.item,main_2.productCode) productCode,
  --main_2.predecessorCode,
  main_2.subRegion,
  case when main_2.subRegion = 'UK-E' and main_2.DC = 'DCNLT1' then main_2.DC else 'DCANV1' end DC,
  main_2.channel,
  main_2.region,
  main_2.subRegionGis,
  main_2.customerGbu,
  main_2.forecastGroup,
  main_2.Forecast_Planner,
  main_2.forecastArea,
  main_2.orderNumber,
  main_2.mtsMtoFlag,
  main_2.quantityOrdered,
  main_2.orderAmountUsd,
  main_2.yearRequestDeliveryBy,
  main_2.monthRequestDeliveryBy,
  --main_2.customerTier,
  case when gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID is null then false else true end gtcReplacementFlag
from
  main_current_1 main_2
  left join gtc_replacement on main_2.productCode = gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID
""")
main_2a.createOrReplaceTempView('main_2a')

# COMMAND ----------

main_f = spark.sql("""
select 
  main._SOURCE,
  main.productCode,
  --main.predecessorCode,
  main.subRegion,
  main.DC,
  main.channel,
  main.region,
  main.subRegionGis,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.forecastArea,
  --main.orderNumber,
  main.mtsMtoFlag,
  main.quantityOrdered,
  main.orderAmountUsd,
  main.yearRequestDeliveryBy,
  main.monthRequestDeliveryBy,
  case when left(pdhProduct.productStyle,1) = '0' then substr(pdhProduct.productStyle, 2, 50) else pdhProduct.productStyle end productStylePdh,
  case when pdhProduct.productCode is null then 'Missing' end pdhProductFlag,
  left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36) description,
  
  Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
  Cast(pdhProduct.caseNetWeight as numeric(9,4)) Unit_weight,
  left(pdhProduct.sizeDescription, 40) sizeDescription,
  left(pdhProduct.gbu, 40) productGbu,
  left(pdhProduct.productSbu, 40) productSbu,
  left(pdhProduct.productBrand, 40) productBrand,
  left(pdhProduct.productSubBrand, 40) productSubBrand,
  left(pdhProduct.productM4Group, 40) productM4Group,
  left(pdhProduct.productM4Family, 40) productM4Family,
  left(pdhProduct.productM4Category, 40) productM4Category,
  left(pdhProduct.productStatus, 40) productStatus,
  left(pdhProduct.productStyle, 40) productStyle,
  left(pdhProduct.name, 40) name,
  left(pdhProduct.marketingCode, 40) marketingCode,
  left(erpProduct.productStatus, 40) regionalProductStatus,
  pdhProduct.originId as originCode,
  pdhProduct.originDescription,
  pdhProduct.piecesInCarton,
  pdhProduct.gtinInner,
  pdhProduct.gtinouter,
  pdhProduct.ansStdUom
from
  main_current_1 main
  left join (select * from s_core.product_agg where _source = 'PDH') pdhProduct on main.productCode = pdhProduct.productCode 
  left join s_core.product_agg erpProduct on main.productCode = erpProduct.productCode and main._SOURCE = erpProduct._SOURCE
where
  1=1
""")
  
main_f.createOrReplaceTempView('main_f')

# COMMAND ----------

# LOAD
register_hive_table(main_f, target_table, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, target_table, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_supplychain.sales_order_lines_agg')
  update_run_datetime(run_datetime, target_table, 's_supplychain.sales_order_lines_agg')
