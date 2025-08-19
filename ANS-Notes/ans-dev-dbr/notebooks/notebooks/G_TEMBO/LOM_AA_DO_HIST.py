# Databricks notebook source
# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)

# COMMAND ----------

ENV_NAME = os.getenv('ENV_NAME')
ENV_NAME = ENV_NAME.upper()
ENV_NAME

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
#key_columns = get_input_param('key_columns', 'string', default_value = 'item_ID,inventoryWarehouse_ID,owningBusinessUnit_ID')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'TMP_LOM_AA_DO_L1_HIST')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# DBTITLE 1,0.1 Determine current runPeriod
runperiodsql= "select nvl(case when cvalue = '*CURRENT' then year(current_date) * 100 + month(current_date) else   cast(cvalue as integer)  end, year(current_date) * 100 + month(current_date)) runPeriod from smartsheets.edm_control_table where table_id = 'TEMBO_CONTROLS'   and key_value = '{0}_CURRENT_PERIOD'   and active_flg is true   and current_date between valid_from and valid_to".format(ENV_NAME)

run_period  = spark.sql(runperiodsql)
run_period.createOrReplaceTempView("run_period")
run_period.display()

# COMMAND ----------

# DBTITLE 1,0.2 Determine current startPeriod
startperiodsql= "select nvl(case when cvalue = '*CURRENT' then year(current_date) * 100 + month(current_date) else   cast(cvalue as integer)  end, year(current_date) * 100 + month(current_date)) startPeriod from smartsheets.edm_control_table where table_id = 'TEMBO_CONTROLS'   and key_value = '{0}_START_PERIOD'   and active_flg is true   and current_date between valid_from and valid_to".format(ENV_NAME)

start_period  = spark.sql(startperiodsql)
start_period.createOrReplaceTempView("start_period")
start_period.display()

# COMMAND ----------

# DBTITLE 1,0.3 Determine active source systems
source_systemssql= "select distinct cvalue sourceSystem from smartsheets.edm_control_table where table_id = 'TEMBO_CONTROLS'   and key_value = '{0}_SOURCE_SYSTEM'   and active_flg is true   and current_date between valid_from and valid_to".format(ENV_NAME)

source_systems  = spark.sql(source_systemssql)
source_systems.createOrReplaceTempView("source_systems")
source_systems.display()

# COMMAND ----------

# DBTITLE 1,0.4. Build temp substitution table - Historical
HistoricalProductSubstitution = spark.sql("""
SELECT
  PROD_CD || '-' ||  DC || '-' || DS_WH key,
  PROD_CD,
  PROD_DESCRP,
  CHAN_ID,
  Channel,
  DC,
  DS_WH,
  Subregion,
  Intro_date,
  final_Successor,
  to_date(valid_from, 'dd/MM/yyyy') valid_from,
  Predecessor_of_Final_successor
FROM
  tmb.dbo_E2EHistoricalProductSubstitution
where 1=1
  and year(to_date(valid_from, 'dd/MM/yyyy')) * 100 + month(to_date(valid_from, 'dd/MM/yyyy')) <= (select runperiod from run_period)
  and not _deleted
""")

HistoricalProductSubstitution.createOrReplaceTempView("HistoricalProductSubstitution")

# COMMAND ----------

# DBTITLE 1,0.5 Get PDH product master records
pdhProduct = spark.sql("""
select 
  * 
from 
  s_core.product_agg 
where _source = 'PDH'
  and not _deleted
""")
pdhProduct.createOrReplaceTempView('pdhProduct')
pdhProduct.display()

# COMMAND ----------

# DBTITLE 1,0.6 Determine organizations to exclude
exclude_organizations = spark.sql("""
select 
  distinct key_value organization
from smartsheets.edm_control_table
where table_id = 'E2E_EXCLUDE_ORGANIZATION'
  and active_flg is true
  and current_date between valid_from and valid_to
""")

exclude_organizations.createOrReplaceTempView("exclude_organizations")

# COMMAND ----------

# DBTITLE 1,0.7 Get product details for replaced item numbers
regionalproductstatus = spark.sql("""
select 
  pr_org.organization_id, 
  pr_org.item_id,
  pr.e2eProductCode,
  pr_org.productstatus,
  pr_org.mtsMtoFlag
from s_core.product_org_agg pr_org
  join s_core.product_agg pr on 
     pr_org.item_id = pr._ID
where pr.productcode = pr.e2eproductcode
  and not pr._deleted
  and not pr_org._deleted

""")
regionalproductstatus.createOrReplaceTempView("regionalproductstatus")

# COMMAND ----------

# DBTITLE 1,0.8 Get sub region override table
sub_region_override = spark.sql("""
select distinct 
  replace(key_value, '.0', '')  partySiteNumber,
  cvalue subRegion,
  nvalue country_ID
from smartsheets.edm_control_table
where table_id = 'E2E_SUB_REGION_OVERRIDE'
  and active_flg is true
  and current_date between valid_from and valid_to
""")

sub_region_override.createOrReplaceTempView("sub_region_override")
sub_region_override.display()

# COMMAND ----------

# packaging_code = spark.sql("""
# select item,pack_type,base_product from pdh.master_records
# """)
# packaging_code.createOrReplaceTempView('packaging_code')
# # packaging_code.display()

# COMMAND ----------

# DBTITLE 1,1. Get base selection for LOM AA DO - History
main_1 = spark.sql("""
WITH MAIN_1 as
(
select 
  acc.accountnumber,
  pr.e2eProductCode || '-' || coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) || '-' ||  orderType.dropShipmentFlag key,
  pr.e2eProductCode productCode,
  pr.productStatus,
  case when nvl(acc.gbu,'X') = 'NV&AC' then 'I' else  left(nvl(acc.gbu, acc_org.salesoffice), 1) end customerGbu,
  case when acc_org._id = 0 then 
     nvl(acc.forecastGroup, 'Other')
  else
     nvl(acc_org.forecastGroup, 'Other')
  end forecastGroup,
  soldtoTerritory.region Forecast_Planner,
  case 
    when sub_region_override.subRegion is not null then sub_region_override.subRegion 
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion not in ('MX') and oh._source = 'EBS' then 'OLAC' 
    else  nvl(soldtoTerritory.subRegion, '#Error8-' || soldtoTerritory.territoryCode) 
  end subRegion, 
  coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, 'DC' || inv.organizationcode)  DC ,
  orderType.dropShipmentFlag Channel,
  soldtoTerritory.territoryCode,
  soldtoTerritory.subRegionGis,
  case when nvl(soldto.siteCategory,'X') = 'LA' then 'LAC' else nvl(soldtoTerritory.Region, '#Error14') end Region,
  case when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.forecastArea not in ('MX', 'CO') and oh._source = 'EBS' then 'OLAC' else soldtoTerritory.forecastArea end forecastArea,
  ol._SOURCE,
  oh.orderNumber,
  case 
    when pr_org.mtsMtoFlag is null 
      then '#Error20' 
    when pr_org.mtsMtoFlag not in ('MTS', 'MTO') 
      then '#Error21' 
    else 
      pr_org.mtsMtoFlag
  end mtsMtoFlag,
   case when ol.orderlinestatus like '%CANCELLED%'
    then (ol.quantitycancelled  * ol.ansStdUomConv)
  else (ol.quantityOrdered  * ol.ansStdUomConv)
  end quantityOrdered,
  case when ol.orderlinestatus like '%CANCELLED%'
    then ol.cancelledAmount / nvl(oh.exchangeRateUsd,1)
  else
    (ol.orderAmount / nvl(oh.exchangeRateUsd,1) )
  end orderAmountUsd,
  nvl(acc.customerTier, 'No Tier') customerTier,
  year(ol.requestDeliveryBy) yearRequestDeliveryBy,
  month(ol.requestDeliveryBy) monthRequestDeliveryBy,
  1 orderLines,
  pr_org.productStatus regionalProductStatus,
  inv.region orgRegion
from 
  s_supplychain.sales_order_lines_agg ol
  join s_supplychain.sales_order_headers_agg oh on ol.salesorder_id = oh._ID
  join s_core.product_agg pr on ol.item_ID = pr._ID
  join s_core.account_agg acc on oh.customer_ID = acc._ID
  left join s_core.customer_location_agg soldto on oh.soldtoAddress_ID = soldto._ID
  join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
  join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
  join s_core.transaction_type_agg orderType on oh.orderType_ID = orderType._id
  left join s_core.account_organization_agg acc_org on oh.customerOrganization_ID = acc_org._id
  --left join s_core.product_org_agg pr_org on ol.item_ID = pr_org.item_ID 
    --  and ol.inventoryWarehouse_ID = pr_org.organization_ID
  left join regionalproductstatus  pr_org on pr.e2eProductCode = pr_org.e2eProductCode
                                    and ol.inventoryWarehouse_ID = pr_org.organization_ID
  left join sub_region_override on nvl(soldto.partySiteNumber, soldto.addressid) = sub_region_override.partySiteNumber
  left join s_core.territory_agg soldtoTerritory on nvl(sub_region_override.country_ID, soldto.territory_ID) = soldtoTerritory._id  
where
  not ol._deleted
  and not oh._deleted
  and oh._source  in (select sourceSystem from source_systems)
  and inv.isActive
  and coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) not in ('DC325', '000', '250','325', '355', '811', '821', '802', '824', '825', '832', '834')
  and year(ol.requestDeliveryBy) * 100 + month(ol.requestDeliveryBy) >=   (select startPeriod from start_period)
  and year(ol.requestDeliveryBy) * 100 + month(ol.requestDeliveryBy) < (select runPeriod from run_period)
  and (nvl(orderType.e2eFlag, true)
         or ol.e2eDistributionCentre = 'DC827')
  and nvl(pr.itemType, 'X') not in  ('Service')
  and (nvl(ol.orderlinestatus, 'Include') not like ('%CANCELLED%')
    or cancelReason like '%COVID-19%' 
      or cancelReason like '%PLANT DIRECT BILLING%'
      or cancelReason like '%Plant Direct Billing Cancellation%')
  and nvl(pr.productdivision, 'Include') not in ('SH&WB')
  and nvl(acc.customerType, 'External') not in ('Internal')
  and oh.customerId is not null
  and pr.productCode not like 'PALLET%'
  and pr.itemType in ('FINISHED GOODS', 'ACCESSORIES','ZPRF')
  and orderType.name not like 'AIT_DIRECT SHIPMENT%'
  and ol.bookedFlag = 'Y'
  and OH._SOURCE || '-' || upper(nvl(pr_org.productStatus,'X')) not in ('EBS-INACTIVE', 'EBS-OBSOLETE', 'KGD-INACTIVE', 'KGD-OBSOLETE','TOT-INACTIVE', 'TOT-OBSOLETE','COL-INACTIVE', 'COL-OBSOLETE','SAP-INACTIVE')
  and org.organizationcode not in (select organization from exclude_organizations)
  and ol.reasonForRejection is null
)
select 
  accountnumber,
  key,
  productCode,
  productStatus,
  case when customerGbu in ('I', 'M') then customerGbu else 'I' end customerGbu,
  forecastGroup,
  Forecast_Planner,
  subRegion, 
  DC ,
  Channel,
  territoryCode,
  subRegionGis,
  Region,
  forecastArea,
  _SOURCE,
  orderNumber,
  mtsMtoFlag,
  sum(quantityOrdered) quantityOrdered,
  sum(orderAmountUsd) orderAmountUsd,
  customerTier,
  yearRequestDeliveryBy,
  monthRequestDeliveryBy,
  sum(orderlines) orderlines,
  regionalProductStatus,
  orgRegion
from MAIN_1
group by
  accountnumber,
  key,
  productCode,
  productStatus,
  case when customerGbu in ('I', 'M') then customerGbu else 'I' end,
  forecastGroup,
  Forecast_Planner,
  subRegion, 
  DC,
  Channel,
  territoryCode,
  subRegionGis,
  Region,
  forecastArea,
  _SOURCE,
  orderNumber,
  mtsMtoFlag,
  customerTier,
  yearRequestDeliveryBy,
  monthRequestDeliveryBy,
  regionalProductStatus,
  orgRegion
""")
  
main_1.createOrReplaceTempView('main_1')

# COMMAND ----------

# DBTITLE 1,2. Find substitution if available
main_2 = spark.sql("""
select 
  main._SOURCE,
  case when nvl(HistoricalProductSubstitution.final_successor, main.productCode) = ''
    then '#Error37'
  else nvl(HistoricalProductSubstitution.final_successor, main.productCode) end productCode,
  case when HistoricalProductSubstitution.final_successor is not null
    then Predecessor_of_Final_successor
  end predecessorCode,
  main._SOURCE,
  nvl(main.subRegion, '#Error8-' || main.territoryCode) subRegion,
  nvl(case when main.DC in ('DC813') then 'DC822' else main.DC end, '#Error9') DC,
  nvl(main.channel, '#Error10') channel,
  nvl(main.region, '#Error14') region,
  nvl(main.subRegionGis, '#Warning15') subRegionGis,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.forecastArea,
  main.orderNumber,
  main.mtsMtoFlag,
  main.quantityOrdered,
  main.orderAmountUsd,
  main.yearRequestDeliveryBy,
  main.monthRequestDeliveryBy,
  main.orderlines,
  main.customerTier,
  main.key,
  regionalProductStatus,
  orgRegion
from 
   main_1 main
   left join HistoricalProductSubstitution on main.key =   HistoricalProductSubstitution.key
where nvl(HistoricalProductSubstitution.final_successor, 'Include') not in ('dis')
""")
  
main_2.createOrReplaceTempView('main_2')

# COMMAND ----------

gtc_replacement = spark.sql("""
  select 
    ORACLE_PRODUCT_ID_OR_GTC_ID, 
    ITEM_STATUS,
    min(item) item 
  from pdh.master_records 
  where nvl(STYLE_ITEM_FLAG, 'N') = 'N' 
    and ORACLE_PRODUCT_ID_OR_GTC_ID not in (select item from pdh.master_records where nvl(STYLE_ITEM_FLAG, 'N') = 'N' and ITEM_STATUS not in ('Inactive', 'Discontinue'))
    and ORACLE_PRODUCT_ID_OR_GTC_ID not in ('GTC')
    and ORACLE_PRODUCT_ID_OR_GTC_ID not like 'Rule generated%'
  group by 
    ORACLE_PRODUCT_ID_OR_GTC_ID,
    ITEM_STATUS
""")
  
gtc_replacement.createOrReplaceTempView('gtc_replacement')

# COMMAND ----------

# DBTITLE 1,2.a. replace GTC product code with PDH product Code
main_2a = spark.sql("""
select 
  main_2._SOURCE,
  nvl(gtc_replacement.item,main_2.productCode) productCode,
  main_2.predecessorCode,
  main_2.subRegion,
  case
    when main_2.subRegion = 'UK-E' and main_2.DC = 'DCNLT1'
        then main_2.DC
    when main_2.subRegion = 'UK-E' and main_2.DC <> 'DCNLT1'  and _source = 'SAP' then main_2.DC
    when main_2.subRegion <> 'UK-E' and main_2.DC = 'DCNLT1'  and _source = 'SAP' then 'DCANV1'
    else main_2.DC
  end DC,
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
  main_2.customerTier,
  case when gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID is null then false else true end gtcReplacementFlag,
  gtc_replacement.ITEM_STATUS,
  main_2.orderlines,
  main_2.regionalProductStatus,
  main_2.orgRegion
from
  main_2
  left join gtc_replacement on main_2.productCode = gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID
where 
  nvl(gtc_replacement.ITEM_STATUS, 'Active') not in ('Inactive')
""")
main_2a.createOrReplaceTempView('main_2a')
# main_2a.display()


# COMMAND ----------

# DBTITLE 1,3. Add Product data from source systems and PDH data - History
main_3 = spark.sql("""
select 
  main_2._SOURCE,
  main_2.productCode,
  main_2.predecessorCode,
  main_2.subRegion,
  main_2.DC,
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
  case when left(pdhProduct.productStyle,1) = '0' then substr(pdhProduct.productStyle, 2, 50) else pdhProduct.productStyle end productStylePdh,
  case when pdhProduct.productCode is null then '#Error1' end pdhProductFlag,
  left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36) description,
  Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
  Cast(pdhProduct.caseNetWeight as numeric(9,4)) Unit_weight,
  nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  sizeDescription,
  nvl(left(pdhProduct.gbu, 40), '#Error5' ) productGbu,
  nvl(left(pdhProduct.productSbu, 40), '#Error6') productSbu,
  left(pdhProduct.productBrand, 40) productBrand,
  left(pdhProduct.productSubBrand, 40) productSubBrand,
  left(pdhProduct.productM4Group, 40) productM4Group,
  left(pdhProduct.productM4Family, 40) productM4Family,
  left(nvl(pdhProduct.productM4Category, '#Error22'), 40) productM4Category,
  left(pdhProduct.productStatus, 40) productStatus,
  nvl(left(pdhProduct.productStyle, 40), '#Error13') productStyle,
  nvl(left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36), '#Error12') name,
  left(pdhProduct.marketingCode, 40) marketingCode,
  --left(erpProduct.productStatus, 40) regionalProductStatus,
  main_2.regionalProductStatus,
  pdhProduct.originId,
  pdhProduct.originDescription,
  pdhProduct.piecesInCarton,
  nvl(pdhProduct.gtinInner, 'No gtinInner') gtinInner,
  nvl(pdhProduct.gtinouter, 'No gtinOuter') gtinouter,
  case 
    when pdhProduct.ansStdUom is null then '#Error18' 
    when pdhProduct.ansStdUom not in ('PAIR', 'PIECE') then '#Error19' 
    else pdhProduct.ansStdUom
  end ansStdUom,
  main_2.gtcReplacementFlag,
  main_2.customerTier,
  pdhProduct.ansStdUomConv,
  main_2.orderlines,
  main_2.orgRegion
from
  main_2a main_2
  left join (select * from s_core.product_agg where _source = 'PDH' and not _deleted) pdhProduct on main_2.productCode = pdhProduct.productCode 
where
  1=1
  --and nvl(pdhProduct.itemClass,'X') not in ('Ancillary Products')
""")
  
main_3.createOrReplaceTempView('main_3')

# COMMAND ----------

# DBTITLE 1,4.Add Forecast Planner L1-2 and L3-6 - history
main_4 = spark.sql("""
select
  main._SOURCE,
  main.productCode,
  main.predecessorCode,
  main.subRegion,
  main.DC,
  main.channel,
  main.region,
  main.subRegionGis,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.forecastArea,
  main.orderNumber,
  main.mtsMtoFlag,
  main.quantityOrdered,
  main.orderAmountUsd,
  main.yearRequestDeliveryBy,
  main.monthRequestDeliveryBy,
  main.productStylePdh,
  main.pdhProductFlag,
  main.description,
  main.Unit_cube,
  main.Unit_weight,
  main.sizeDescription,
  main.productGbu,
  main.productSbu,
  main.productBrand,
  main.productSubBrand,
  main.productM4Group,
  main.productM4Family,
  main.productM4Category,
  main.productStatus,
  main.productStyle,
  main.name,
  main.marketingCode,
  main.regionalProductStatus,
  main.originId,
  main.originDescription,
  main.piecesInCarton,
  main.gtinInner,
  main.gtinouter,
  main.ansStdUom,
  main.gtcReplacementFlag,
  main.customerTier,
  main.ansStdUomConv,
  case
    when main.customerGbu = 'M' then 'M_*'
    when main.customerGbu in ('I', 'N')
      and main.Region = 'NA'
      and main.productGbu not in ('MEDICAL')
      and main.forecastGroup not like 'I_GRAINGER%' then 'I_* & <> I_GRAINGER*'
    when upper(main.customerGbu) in ('I', 'N', 'O', 'U') then 'I_*'
    when main.forecastGroup like 'I_GRAINGER%' then 'I_GRAINGER*'
  end key,
  nvl(plannerCodeL1.forecastplannerid, '#Error2') forecastL1plannerid,
  nvl(plannerCodeL3.forecastplannerid, '#Error3') forecastL3plannerid,
  main.orderlines,
  main.orgRegion
from
  main_3 main
  left join (
    select
      customerforecastgroup,
      forecastplannerid,
      pdhgbu,
      region,
      sbu
    from
      smartsheets.e2eforecastplanner
    where
      level = 'L1-2'
      and not _deleted
  ) plannerCodeL1 on main.region = plannercodeL1.region
  and main.productSbu = plannercodeL1.SBU
  and main.productGbu = plannercodeL1.pdhgbu
  and    case
    when main.customerGbu = 'M' then 'M_*'
    when main.customerGbu in ('I', 'N')
      and main.Region = 'NA'
      and main.productGbu not in ('MEDICAL')
      and main.forecastGroup not like 'I_GRAINGER%' then 'I_* & <> I_GRAINGER*'
    when upper(main.customerGbu) in ('I', 'N', 'O', 'U') then 'I_*'
    when main.forecastGroup like 'I_GRAINGER%' then 'I_GRAINGER*'
  end = plannercodeL1.CustomerForecastGroup
   left join (
    select
      forecastplannerid,
      pdhgbu,
      region,
      sbu
    from
      smartsheets.e2eforecastplanner
    where
      level = 'L3-6'
      and not _deleted
  ) plannerCodeL3 on main.region = plannerCodeL3.region
  and main.productSbu = plannerCodeL3.SBU
  and main.productGbu = plannerCodeL3.pdhgbu
""")
  
main_4.createOrReplaceTempView('main_4')

# COMMAND ----------

# DBTITLE 1,5. Add average cost and sales price if available - History
main_5 = spark.sql("""
select 
  main._SOURCE,
  main.productCode,
  main.predecessorCode,
  main.subRegion,
  main.DC,
  main.channel,
  main.region,
  main.subRegionGis,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.forecastArea,
  main.orderNumber,
  main.mtsMtoFlag,
  main.quantityOrdered,
  main.orderAmountUsd,
  main.yearRequestDeliveryBy,
  main.monthRequestDeliveryBy,
  main.productStylePdh,
  main.pdhProductFlag,
  main.description,
  main.Unit_cube,
  main.Unit_weight,
  main.sizeDescription,
  main.productGbu,
  main.productSbu,
  main.productBrand,
  main.productSubBrand,
  main.productM4Group,
  main.productM4Family,
  main.productM4Category,
  main.productStatus,
  main.productStyle productStyle,
  main.name,
  main.marketingCode,
  main.regionalProductStatus,
  main.originId,
  main.originDescription,
  main.piecesInCarton,
  main.gtinInner,
  main.gtinOuter,
  main.ansStdUom,
  main.gtcReplacementFlag,
  main.customerTier,
  main.ansStdUomConv,
  main.productGbu,
  main.productSbu,
  main.forecastL1plannerid,
  main.forecastL3plannerid,
  main.orderlines,
  nvl(qvunitpricePdh.AVG_UNIT_SELLING_PRICE,0) Unit_price,
  nvl(qvunitpricePdh.AVG_UNIT_COST_PRICE,0) Unit_cost,
  pdhProduct.packagingCode,
  pdhProduct.baseProduct,
  main.orgRegion
from main_4 main
  left join sap.qvunitprice qvunitpricePdh on  main.productStylePdh  = qvunitpricePdh.StyleCode
    and main.subRegionGis = qvunitpricePdh.subRegion
    left join pdhProduct on main.productCode = pdhProduct.productCode


""")
  
main_5.createOrReplaceTempView('main_5')

# COMMAND ----------

# DBTITLE 1,5.1.Product LOM AA DO (L1) - History
LOM_AA_DO_L1 = spark.sql("""
select distinct
    main._SOURCE,
    main.pdhProductFlag _PDHPRODUCTFLAG,
    '1' Pyramid_Level,
    left(main.productCode,40) Forecast_1_ID,
    main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
    left(main.customerGbu || '_' || main.forecastGroup, 40) Forecast_3_ID,
    'AA' Record_Type,
    name Description,
    'Y' Forecast_calculate_indicator,
    main.Unit_price,
    main.Unit_cost,
    main.Unit_cube,
    main.Unit_Weight,
    'N' Product_group_conversion_option,
    1 Product_group_conversion_factor,
    main.forecastL1plannerid Forecast_Planner,
    main.productStyle || '_' || main.sizeDescription User_Data_84,
    main.productStyle  User_Data_01,
    main.sizeDescription User_Data_02,
    main.productStyle || '_' || main.sizeDescription User_Data_03,
    main.productGbu User_Data_04,
    main.productSbu User_Data_05,
    main.productBrand User_Data_06,
    main.productSubBrand User_Data_07,
    main.baseProduct User_Data_08,
    main.region User_Data_09,
    main.forecastArea User_Data_10,
    main.subRegion User_Data_11,
    main.channel User_Data_12,
    main.subRegion || '_' || main.channel User_Data_13,
    main.marketingCode User_Data_14,
    main.productStatus User_Data_15,
    main.regionalProductStatus User_Data_16,
    main.packagingCode User_Data_17,
    '' User_Data_18,
    '' User_Data_19,
    main.predecessorCode User_Data_20,
    '' User_Data_21,
    '' User_Data_22,
    main.productM4Category User_Data_23,
    'Undefined' User_Data_24,
    'Undefined' User_Data_25,
    'Undefined' User_Data_26,
    'Undefined' User_Data_27,
    main.originId  User_Data_28,
    main.originDescription User_Data_29,
    main.productM4Group User_Data_30,
    main.productM4Family User_Data_31,
    main.productM4Category User_Data_32,
    main.customerGbu || '_' || main.forecastGroup User_Data_33,
    'M' Cube_unit_of_measure,
    'KG' Weight_unit_of_measure,
    ansStdUom Unit_of_measure,
    case when ansStdUom = 'PIECE' then main.piecesInCarton else main.piecesInCarton / 2 end Case_quantity,
    main.gtcReplacementFlag,
    orderNumber,
    current_date creationDate
  from main_5 main

""")

LOM_AA_DO_L1.createOrReplaceTempView('LOM_AA_DO_L1')


# COMMAND ----------

# DBTITLE 1,5.2.Product LOM AA DO (L4) - History
LOM_AA_DO_L4 = spark.sql("""
select distinct
    _source,
    pdhProductFlag _PDHPRODUCTFLAG,
    '4' Pyramid_Level,
    main.productStyle || '_' ||  main.sizeDescription Forecast_1_ID,
    main.region Forecast_2_ID,
    left(main.channel,40) Forecast_3_ID,
    'AA' Record_Type,
    main.productStyle Description,
    main.forecastL3plannerid Forecast_Planner,
    main.orderNumber
  from main_5 main 
""")

LOM_AA_DO_L4.createOrReplaceTempView('LOM_AA_DO_L4')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_AA_DO_L4_HIST', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AA_DO_L4.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,5.3.Product LOM AU - History
LOM_AU_L1 = spark.sql("""
select distinct
_source,
pdhProductFlag _PDHPRODUCTFLAG,
    '1' Pyramid_Level,
    left(main.productCode,40) Forecast_1_ID,
    main.subRegion  || '_' || main.DC || '_' || main.channel Forecast_2_ID,
    left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
    'AU' Record_Type,
    main.forecastGroup User_Data_34,
    main.customerGbu User_Data_35,
    main.gtinInner User_Data_38,
    main.gtinouter User_Data_39,
    main.orgRegion User_Data_41,
    main.customerTier User_Data_42,
    main.DC User_Data_43,
    main.subRegion || '_' || main.DC || '_' || main.channel User_Data_44,
    main.mtsMtoFlag User_Data_45,
    '' User_Data_46,
    '' User_Data_47,
    main.ordernumber,
    current_date User_Data_48
  from main_5 main
""")
LOM_AU_L1.createOrReplaceTempView('LOM_AU_L1')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_AU_DO_L1_HIST', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AU_L1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,5.4. Product LOM BB L2 - de-prioritized
# MAGIC %sql
# MAGIC --%python
# MAGIC --LOM_BB_DO_L2= spark.sql("""
# MAGIC -- select 
# MAGIC --    2 Pyramid_Level,
# MAGIC --    left(main.productCode,40) Forecast_1_ID,
# MAGIC --    main.subRegion || '_' || main.channel Forecase_2_ID,
# MAGIC --    main.customerGbu || '_' || main.forecastGroup Forecast_3_ID,
# MAGIC --    'BB' Record_Type,
# MAGIC --    cast(2022 as decimal (4,0)) Year,
# MAGIC --    cast(02 as decimal (2,0)) Period ,
# MAGIC --    cast(0 as decimal (9,0))  Budget_units,
# MAGIC --    '' Budget_unit_sign,
# MAGIC --    cast(0 as decimal(9,0)) Budget_value ,
# MAGIC --    '' Budget_value_sign
# MAGIC -- from 
# MAGIC --   main_5 main
# MAGIC  
# MAGIC
# MAGIC --""")
# MAGIC   
# MAGIC --main_2.createOrReplaceTempView('main_2')

# COMMAND ----------

# DBTITLE 1,20.1.Get base selection for LOM HH Shipments
main_20 = spark.sql("""
WITH MAIN_20 AS (
select 
  acc.accountnumber,
  pr.e2eProductCode || '-' || coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) || '-' ||  dropShipmentFlag key,
  pr.e2eProductCode productCode,
  pr.productStatus,
  case when nvl(acc.gbu,'X') = 'NV&AC' then 'I' else  left(nvl(acc.gbu, acc_org.salesoffice), 1) end customerGbu,
  case when acc_org._id = 0 then 
     nvl(acc.forecastGroup, 'Other')
  else
     nvl(acc_org.forecastGroup, 'Other')
  end forecastGroup,
  soldtoTerritory.region Forecast_Planner,
  case 
    when sub_region_override.subRegion is not null then sub_region_override.subRegion 
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion not in ('MX') and oh._source = 'EBS' then 'OLAC' 
    else  nvl(soldtoTerritory.subRegion, '#Error8-' || soldtoTerritory.territoryCode) 
  end subRegion, 
  coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode)  DC ,
  orderType.dropShipmentFlag Channel,
  soldtoTerritory.territoryCode,
  soldtoTerritory.subRegionGis,
  case when nvl(soldto.siteCategory,'X') = 'LA' then 'LAC' else nvl(soldtoTerritory.Region, '#Error14') end Region,
  case when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion not in ('MX', 'CO') and oh._source = 'EBS' then 'OLAC' else soldtoTerritory.forecastArea end forecastArea,
  ol._SOURCE,
  oh.orderNumber,
  (sl.quantityShipped  * nvl(sl.ansStdUomConv,1)) quantityShipped,
  (sl.shippingAmount / nvl(sl.exchangeRateUsd,1) ) shippingAmountUsd,
  year(sl.actualShipDate) yearShipped,
  month(sl.actualShipDate) monthShipped,
  pr_org.productStatus regionalProductStatus 
from 
  s_supplychain.sales_order_lines_agg ol -- 2.228.919
   join s_supplychain.sales_shipping_lines_agg sl on ol._ID = sl.salesOrderDetail_ID -- 2.514.227
   join s_supplychain.sales_order_headers_agg oh on ol.salesorder_id = oh._ID 
   join s_core.product_agg pr on ol.item_ID = pr._ID -- 2.514.227
   join s_core.account_agg acc on oh.customer_ID = acc._ID -- 2.514.222 
   left join s_core.customer_location_agg soldto on oh.soldtoAddress_ID = soldto._ID -- 2514222
   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID -- 2514169
   join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
   left join s_core.transaction_type_agg orderType on oh.orderType_ID = orderType._id  
   left join s_core.account_organization_agg acc_org on oh.customerOrganization_ID = acc_org._id -- 2514169
   left join regionalproductstatus  pr_org on pr.e2eProductCode = pr_org.e2eProductCode
                                    and ol.inventoryWarehouse_ID = pr_org.organization_ID
  left join sub_region_override on nvl(soldto.partySiteNumber, soldto.addressid) = sub_region_override.partySiteNumber
  left join s_core.territory_agg soldtoTerritory on nvl(sub_region_override.country_ID, soldto.territory_ID) = soldtoTerritory._id  
where
  not ol._deleted
  and not oh._deleted
  and not acc._deleted
  and not pr._deleted
  and not sl._deleted
  and oh._source  in (select sourceSystem from source_systems)
  and not orderType._deleted
  and inv.isActive 
  and year(sl.actualShipDate) * 100 + month(sl.actualShipDate) >=   (select startPeriod from start_period) --201901 --year(add_months(current_date, -40)) * 100 + month(add_months(current_date, -40)) --and year(current_date) * 100 + month(current_date) 
  and year(sl.actualShipDate) * 100 + month(sl.actualShipDate) < (select runPeriod from run_period)
  and coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) not in ('DC325', '000', '250','325', '355', '811', '821', '802', '824', '825', '832', '834')
  and (nvl(orderType.e2eFlag, true)
         or ol.e2edistributionCentre = 'DC827')
  and upper(pr.itemType) not in  ('SERVICE')
  and (ol.quantityOrdered <> 0
     or cancelReason like '%COVID-19%' 
       or cancelReason like '%PLANT DIRECT BILLING%'
       or cancelReason like '%Plant Direct Billing Cancellation%')
  and nvl(pr.productdivision, 'Include') not in ('SH&WB')
  and nvl(acc.customerType, 'External') not in ('Internal')
  and oh.customerId is not null
  and pr.productCode not like 'PALLET%'
  and orderType.name not like 'AIT_DIRECT SHIPMENT%'
  and ol.bookedFlag = 'Y'
  --and upper(nvl(pr_org.productStatus,'X')) not in ('INACTIVE', 'OBSOLETE', 'OBSOLETE (OLD VALUE)')
  and OH._SOURCE || '-' || upper(nvl(pr_org.productStatus,'X')) not in ('EBS-INACTIVE', 'EBS-OBSOLETE', 'KGD-INACTIVE', 'KGD-OBSOLETE','TOT-INACTIVE', 'TOT-OBSOLETE','COL-INACTIVE', 'COL-OBSOLETE','SAP-INACTIVE')
  and org.organizationcode not in (select organization from exclude_organizations)
 )
select 
  accountnumber,
  key,
  productCode,
  productStatus,
  case when customerGbu in ('I', 'M') then customerGbu else 'I' end customerGbu,
  forecastGroup,
  Forecast_Planner,
  subRegion, 
  DC ,
  Channel,
  territoryCode,
  subRegionGis,
  Region,
  forecastArea,
  _SOURCE,
  orderNumber,
  sum(quantityShipped) quantityShipped,
  sum(shippingAmountUsd) shippingAmountUsd,
  yearShipped,
  monthShipped,
  regionalProductStatus 
from 
  MAIN_20
group by
  accountnumber,
  key,
  productCode,
  productStatus,
  case when customerGbu in ('I', 'M') then customerGbu else 'I' end,
  forecastGroup,
  Forecast_Planner,
  subRegion, 
  DC ,
  Channel,
  territoryCode,
  subRegionGis,
  Region,
  forecastArea,
  _SOURCE,
  orderNumber,
  yearShipped,
  monthShipped,
  regionalProductStatus 
  
""")
  
main_20.createOrReplaceTempView('main_20')

# COMMAND ----------

# DBTITLE 1,20.2. Find substitution if available
main_20_2 = spark.sql("""
select 
  main._SOURCE,
  nvl(HistoricalProductSubstitution.final_successor, main.productCode) productCode,
  main._SOURCE,
  main.subRegion,
  main.DC,
  main.channel,
  main.region,
  main.subRegionGis,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.forecastArea,
  main.orderNumber,
  main.quantityShipped,
  main.shippingAmountUsd,
  main.yearShipped,
  main.monthShipped,
  regionalProductStatus 
from 
   main_20 main
   left join HistoricalProductSubstitution on main.key =   HistoricalProductSubstitution.key
where nvl(HistoricalProductSubstitution.final_successor, 'Include') not in ('dis')
""")
  
main_20_2.createOrReplaceTempView('main_20_2')

# COMMAND ----------

# DBTITLE 1,20.2.0 - GTC Substitution
main_2a = spark.sql("""
select 
  main_2._SOURCE,
  nvl(gtc_replacement.item,main_2.productCode) productCode,
  main_2.subRegion,
  case
    when main_2.subRegion = 'UK-E' and main_2.DC = 'DCNLT1'
        then main_2.DC
    when main_2.subRegion = 'UK-E' and main_2.DC <> 'DCNLT1'  and _source = 'SAP' then main_2.DC
    when main_2.subRegion <> 'UK-E' and main_2.DC = 'DCNLT1'  and _source = 'SAP' then 'DCANV1'
    else main_2.DC
  end DC,
  main_2.channel,
  main_2.region,
  main_2.subRegionGis,
  main_2.customerGbu,
  main_2.forecastGroup,
  main_2.Forecast_Planner,
  main_2.forecastArea,
  main_2.orderNumber,
  main_2.quantityShipped,
  main_2.shippingAmountUsd,
  main_2.yearShipped,
  main_2.monthShipped,
  case when gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID is null then false else true end gtcReplacementFlag,
  gtc_replacement.ITEM_STATUS,
  main_2.regionalProductStatus 
from
  main_20_2 main_2
  left join gtc_replacement on main_2.productCode = gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID
where 
  nvl(gtc_replacement.ITEM_STATUS, 'Active') not in ('Inactive')
""")
main_2a.createOrReplaceTempView('main_20_2a')

# COMMAND ----------

# DBTITLE 1,20.3. Add Product data from source systems and PDH data
main_20_3 = spark.sql("""
select 
  main._SOURCE,
  case when pdhProduct.productCode is null then '#Error1' end pdhProductFlag,
  main.productCode,
  main.subRegion,
  main.DC,
  main.channel,
  main.region,
  main.subRegionGis,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.forecastArea,
  null as orderNumber, -- changes in edm
  main.quantityShipped,
  main.shippingAmountUsd,
  main.yearShipped,
  main.monthShipped,
  case when left(pdhProduct.productStyle,1) = '0' then substr(pdhProduct.productStyle, 2, 50) else pdhProduct.productStyle end productStylePdh,
  
  nvl(left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36), '#Error12') name,
  Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
  Cast(pdhProduct.caseNetWeight as numeric(9,4)) Unit_weight,
  nvl(left(pdhProduct.productStyle, 40), '#Error13') productStyle,
  nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  sizeDescription,
  nvl(left(pdhProduct.gbu, 40), '#Error5' ) productGbu,
  nvl(left(pdhProduct.productSbu, 40), '#Error6' ) productSbu,
  left(pdhProduct.productBrand, 40) productBrand,
  left(pdhProduct.productSubBrand, 40) productSubBrand,
  left(pdhProduct.marketingCode, 40) marketingCode,
  left(pdhProduct.productStatus, 40) productStatus,
  main.regionalProductStatus,
  left(nvl(pdhProduct.productM4Category, '#Error22'), 40) productM4Category,
  pdhProduct.originId,
  pdhProduct.originDescription,
  left(pdhProduct.productM4Group, 40) productM4Group,
  left(pdhProduct.productM4Family, 40) productM4Family,
  case 
    when pdhProduct.ansStdUom is null then '#Error18' 
    when pdhProduct.ansStdUom not in ('PAIR', 'PIECE') then '#Error19' 
    else pdhProduct.ansStdUom
  end ansStdUom,
  pdhProduct.piecesInCarton,
  main.gtcReplacementFlag  
  
from
  main_20_2a main
  left join (select * from s_core.product_agg where _source = 'PDH' and not _deleted) pdhProduct on main.productCode = pdhProduct.productCode 
where
  1=1
  and nvl(pdhProduct.itemClass,'X') not in ('Ancillary Products')
""")
  
main_20_3.createOrReplaceTempView('main_20_3')

# COMMAND ----------

# edm

main_20_4_ss = spark.sql("""
select 
  main._SOURCE,
  pdhProductFlag _PDHPRODUCTFLAG,
  '1' Pyramid_Level,
  left(main.productCode,40) Forecast_1_ID,
  main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
  left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
  'AA' Record_Type,
  name Description,  
 'Y' Forecast_calculate_indicator,
  nvl(qvunitpricePdh.AVG_UNIT_SELLING_PRICE,0) Unit_price,
  nvl(qvunitpricePdh.AVG_UNIT_COST_PRICE,0) Unit_cost,
  main.Unit_cube,
  main.Unit_Weight,
  'N' Product_group_conversion_option,
  1 Product_group_conversion_factor,
  main.Forecast_Planner,
  main.productStyle || '_' || main.sizeDescription User_Data_84,
  main.productStyle  User_Data_01,
  main.sizeDescription User_Data_02,
  main.productStyle || '_' || main.sizeDescription User_Data_03,
  main.productGbu User_Data_04,
  main.productSbu User_Data_05,
  main.productBrand User_Data_06,
  main.productSubBrand User_Data_07,
  main.productStyle User_Data_08,
  main.region User_Data_09,
  main.forecastArea User_Data_10,
  main.subRegion User_Data_11,
  main.channel User_Data_12,
  main.subRegion || '_' || main.channel User_Data_13,
  main.marketingCode User_Data_14,
  main.productStatus User_Data_15,
  main.regionalProductStatus User_Data_16,
  '' User_Data_17,
  '' User_Data_18,
  '' User_Data_19,
  '' User_Data_20,
  '' User_Data_21,
  '' User_Data_22,
  main.productM4Category User_Data_23,
  'Undefined' User_Data_24,
  'Undefined' User_Data_25,
  'Undefined' User_Data_26,
  'Undefined' User_Data_27,
  main.originId  User_Data_28,
  main.originDescription User_Data_29,
  main.productM4Group User_Data_30,
  main.productM4Family User_Data_31,
  main.productM4Category User_Data_32,
  main.customerGbu || '_' || main.forecastGroup User_Data_33,
  'M' Cube_unit_of_measure,
  'KG' Weight_unit_of_measure,
  main.ansStdUom Unit_of_measure,
  case when main.ansStdUom = 'PIECE' then main.piecesInCarton else main.piecesInCarton / 2 end Case_quantity,
  main.gtcReplacementFlag,
  main.orderNumber,
  current_date creationDate  
from
  main_20_3 main 
  left join sap.qvunitprice qvunitpricePdh on  main.productStylePdh  = qvunitpricePdh.StyleCode
    and main.subRegionGis = qvunitpricePdh.subRegion
""")
  
main_20_4_ss.createOrReplaceTempView('main_20_4_ss')

# COMMAND ----------

# DBTITLE 1,20.4.Build interface layout for Shipments
main_HH2 = spark.sql("""

select
    main._SOURCE,
    main.pdhProductFlag _PDHPRODUCTFLAG,
    '1' Pyramid_Level,
    left(main.productCode,40) Forecast_1_ID,
    main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
    left(main.customerGbu || '_' || main.forecastGroup, 40) Forecast_3_ID,
    'HH' Record_Type,
    main.yearShipped Year,
    main.monthShipped Period,
    0 Demand,
    '' Demand_sign,
    quantityShipped  Historical_ADS1_data,
    '' Historical_ADS1_sign,
    0 Value_history_in_currency,
    '' Value_history_sign,
    shippingAmountUsd Historical_ADS2_data,
    '' Historical_ADS2_sign

from  main_20_3 main

""")
 
main_HH2.createOrReplaceTempView('main_HH2')


# COMMAND ----------

# DBTITLE 1,20.5 Build interface layout for Orders
main_HH1 = spark.sql("""
select
  main._SOURCE,
  main.pdhProductFlag _PDHPRODUCTFLAG,
  '1' Pyramid_Level,
  left(main.productCode,40) Forecast_1_ID,
  main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
  left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
  'HH' Record_Type,
  main.yearRequestDeliveryBy Year,
  main.monthRequestDeliveryBy Period,
  quantityOrdered Demand,
  '' Demand_sign,
  0 Historical_ADS1_data,
  '' Historical_ADS1_sign,
  main.orderAmountUsd Value_history_in_currency,
  ''  Value_history_sign,
  0 Historical_ADS2_data,
  '' Historical_ADS2_sign
 
from  main_5 main
where yearRequestDeliveryBy * 100 + monthRequestDeliveryBy < (select runPeriod from run_period) --year(current_date) * 100 + month(current_date)

""")
 
main_HH1.createOrReplaceTempView('main_HH1')


# COMMAND ----------

# DBTITLE 1,20.6 Union orders and shipments
main_HH = (
  main_HH1.select('*')
  .union(main_HH2.select('*'))
)

main_HH.createOrReplaceTempView('main_HH')

# COMMAND ----------

# DBTITLE 1,20.7 Build LOM_HH data set
LOM_HH_L1 = spark.sql("""
WITH LOM_HH AS
(select
  _source,
  _pdhproductflag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_Type,
  year,
  period,
  sum(demand) demand,
  sum(historical_ads1_data) historical_ads1_data ,
  sum(value_history_in_currency) value_history_in_currency,
  sum(Historical_ADS2_data) Historical_ADS2_data
from main_HH
  where 1=1
group by
   _source,
  _pdhproductflag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_Type,
  year,
  period
) select
  _source,
  _pdhproductflag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_Type,
  year,
  period,
  abs(demand) demand,
  case when demand < 0 then '-' else '' end demand_sign,
  abs(historical_ads1_data) historical_ads1_data,
  case when historical_ads1_data < 0 then '-' else '' end historical_ads1_sign,
  abs(value_history_in_currency) value_history_in_currency ,
  case when value_history_in_currency < 0 then '-' else '' end value_history_sign,
  abs(Historical_ADS2_data) Historical_ADS2_data,
  case when Historical_ADS2_data < 0 then '-' else '' end Historical_ADS2_sign
  from
    LOM_HH
""")
 
LOM_HH_L1.createOrReplaceTempView('LOM_HH_L1')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_HH_DO_L1_HIST', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_HH_L1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,40.1.INV Get base selection for LOM XX Invoices
main_40_1_inv = spark.sql("""
WITH MAIN_40_1_INV AS
(select 
  acc.accountnumber,
  pr.e2eProductCode || '-' || coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) || '-' ||  dropShipmentFlag key,
  pr.e2eProductCode productCode,
  pr.productStatus,
  case when nvl(acc.gbu,'X') = 'NV&AC' then 'I' else  left(nvl(acc.gbu, acc_org.salesoffice), 1) end customerGbu,
  case when acc_org._id = 0 then 
     nvl(acc.forecastGroup, 'Other')
  else
     nvl(acc_org.forecastGroup, 'Other')
  end forecastGroup,
  soldtoTerritory.region Forecast_Planner,
  case 
    when sub_region_override.subRegion is not null then sub_region_override.subRegion 
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion not in ('MX') and oh._source = 'EBS' then 'OLAC' 
    else  nvl(soldtoTerritory.subRegion, '#Error8-' || soldtoTerritory.territoryCode) 
  end subRegion, 
  coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode)  DC ,
  orderType.dropShipmentFlag Channel,
  soldtoTerritory.territoryCode,
  soldtoTerritory.subRegionGis,
  case when nvl(soldto.siteCategory,'X') = 'LA' then 'LAC' else nvl(soldtoTerritory.Region, '#Error14') end Region,
  case when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.forecastArea not in ('MX', 'CO') and oh._source = 'EBS' then 'OLAC' else soldtoTerritory.forecastArea end forecastArea,
  ol._SOURCE,
  oh.orderNumber,
  (il.quantityInvoiced  * nvl(il.ansStdUomConv,1)) quantityInvoiced,
  year(ih.dateInvoiced) yearInvoiced,
  month(ih.dateInvoiced) monthInvoiced,
 pr_org.productStatus regionalProductStatus 
from 
  s_supplychain.sales_invoice_lines_agg il
  join s_supplychain.sales_invoice_headers_agg ih on il.invoice_id = ih._ID
  join s_supplychain.sales_order_lines_agg ol on il.salesOrderDetail_ID = ol._ID
  join s_supplychain.sales_order_headers_agg oh on ol.salesorder_id = oh._ID
  join s_core.product_agg pr on il.item_ID = pr._ID
  join s_core.account_agg acc on ih.customer_ID = acc._ID
  left join s_core.customer_location_agg soldto on oh.soldtoAddress_ID = soldto._ID
  join s_core.organization_agg inv on il.inventoryWarehouse_ID = inv._ID
  join s_core.organization_agg org on ih.owningBusinessUnit_ID = org._ID
  join s_core.transaction_type_agg orderType on oh.orderType_ID = orderType._id
  left join s_core.account_organization_agg acc_org on oh.customerOrganization_ID = acc_org._id
  --left join s_core.product_org_agg pr_org on il.item_ID = pr_org.item_ID 
    --    and il.inventoryWarehouse_ID = pr_org.organization_ID
 left join regionalproductstatus  pr_org on pr.e2eProductCode = pr_org.e2eProductCode
                                    and il.inventoryWarehouse_ID = pr_org.organization_ID
  left join sub_region_override on nvl(soldto.partySiteNumber, soldto.addressid) = sub_region_override.partySiteNumber
  left join s_core.territory_agg soldtoTerritory on nvl(sub_region_override.country_ID, soldto.territory_ID) = soldtoTerritory._id  
where
  not il._deleted
  and not ih._deleted
  and inv.isActive
  and ih._source  in  (select sourceSystem from source_systems) 
  and coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) not in ('DC325', '000', '355', '811', '821', '802', '824', '825', '832', '834')
  and year(ih.dateInvoiced) * 100 + month(ih.dateInvoiced) >=  (select startPeriod from start_period) -- year(add_months(current_date, -40)) * 100 + month(add_months(current_date, -40)) 
  and year(ih.dateInvoiced) * 100 + month(ih.dateInvoiced) < (select runPeriod from run_period)
  and (nvl(orderType.e2eFlag, true)
         or ol.e2edistributionCentre = 'DC827')
  and pr.itemType not in  ('Service')
  and nvl(pr.productdivision, 'Include') not in ('SH&WB')
  and nvl(acc.customerType, 'External') not in ('Internal')
  and oh.customerId is not null
  and pr.productCode not like 'PALLET%'
  and orderType.name not like 'AIT_DIRECT SHIPMENT%'
  --and upper(nvl(pr_org.productStatus,'X')) not in ('INACTIVE', 'OBSOLETE', 'OBSOLETE (OLD VALUE)')
  and IH._SOURCE || '-' || upper(nvl(pr_org.productStatus,'X')) not in ('EBS-INACTIVE', 'EBS-OBSOLETE', 'KGD-INACTIVE', 'KGD-OBSOLETE','TOT-INACTIVE', 'TOT-OBSOLETE','COL-INACTIVE', 'COL-OBSOLETE','SAP-INACTIVE')
  and org.organizationcode not in (select organization from exclude_organizations)
)
  select 
  accountnumber,
  key,
  productCode,
  productStatus,
  case when customerGbu in ('I', 'M') then customerGbu else 'I' end customerGbu,
  forecastGroup,
  Forecast_Planner,
  subRegion, 
  DC ,
  Channel,
  territoryCode,
  subRegionGis,
  Region,
  forecastArea,
  _SOURCE,
  orderNumber,
  sum(quantityInvoiced) quantityInvoiced,
  yearInvoiced,
  monthInvoiced,
  regionalProductStatus 
from 
  MAIN_40_1_INV
group by
  accountnumber,
  key,
  productCode,
  productStatus,
  case when customerGbu in ('I', 'M') then customerGbu else 'I' end,
  forecastGroup,
  Forecast_Planner,
  subRegion, 
  DC ,
  Channel,
  territoryCode,
  subRegionGis,
  Region,
  forecastArea,
  _SOURCE,
  orderNumber,
  yearInvoiced,
  monthInvoiced,
  regionalProductStatus 
""")
  
main_40_1_inv.createOrReplaceTempView('main_40_1_inv')

# COMMAND ----------

# DBTITLE 1,40.2.INV Find substitution if available for LOM XX Invoices
main_40_2_inv = spark.sql("""
select 
  main.accountnumber,
  nvl(HistoricalProductSubstitution.final_successor, main.productCode) productCode,
  main.productStatus,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.subRegion, 
  main.DC ,
  main.Channel,
  main.territoryCode,
  main.subRegionGis,
  main.Region,
  main.forecastArea,
  main._SOURCE,
  main.orderNumber,
  main.quantityInvoiced,
  main.yearInvoiced,
  main.monthInvoiced,
  main.regionalProductStatus
from 
   main_40_1_inv main
   left join HistoricalProductSubstitution on main.key =   HistoricalProductSubstitution.key
where nvl(HistoricalProductSubstitution.final_successor, 'Include') not in ('dis')
""")
  
main_40_2_inv.createOrReplaceTempView('main_40_2_inv')

# COMMAND ----------

# DBTITLE 1,40.2.a. replace GTC product code with PDH product Code
main_40_2a = spark.sql("""
select 
  main.accountnumber,
  nvl(gtc_replacement.item,main.productCode) productCode, 
  main.productStatus,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.subRegion, 
  case
    when main.subRegion = 'UK-E' and main.DC = 'DCNLT1'
        then main.DC
    when main.subRegion = 'UK-E' and main.DC <> 'DCNLT1'  and _source = 'SAP' then main.DC 
    when main.subRegion <> 'UK-E' and main.DC = 'DCNLT1'  and _source = 'SAP' then 'DCANV1'
    else main.DC
  end DC,
  main.Channel,
  main.territoryCode,
  main.subRegionGis,
  main.Region,
  main.forecastArea,
  main._SOURCE,
  main.orderNumber,
  main.quantityInvoiced,
  main.yearInvoiced,
  main.monthInvoiced,
  case when gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID is null then false else true end gtcReplacementFlag,
  main.regionalProductStatus
from
  main_40_2_inv main
  left join gtc_replacement on main.productCode = gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID
where 
  nvl(gtc_replacement.ITEM_STATUS, 'Active') not in ('Inactive')
""")
main_40_2a.createOrReplaceTempView('main_40_2a')

# COMMAND ----------

# DBTITLE 1,40.3.INV Check if available in PDH
main_40_3_inv = spark.sql("""
select 
  case when pdhProduct.productCode is null then '#Error1' end pdhProductFlag,
  main.accountnumber,
  main.productCode,
  main.productStatus,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.subRegion, 
  main.DC ,
  main.Channel,
  main.territoryCode,
  main.subRegionGis,
  main.Region,
  main.forecastArea,
  main._SOURCE,
  main.orderNumber,
  main.quantityInvoiced,
  main.yearInvoiced,
  main.monthInvoiced,
    
  case when left(pdhProduct.productStyle,1) = '0' then substr(pdhProduct.productStyle, 2, 50) else pdhProduct.productStyle end productStylePdh,
  nvl(left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36), '#Error12') name,
  Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
  Cast(pdhProduct.caseNetWeight as numeric(9,4)) Unit_weight,
  nvl(left(pdhProduct.productStyle, 40), '#Error13') productStyle,
  nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  sizeDescription,
  nvl(left(pdhProduct.gbu, 40), '#Error5' ) productGbu,
  nvl(left(pdhProduct.productSbu, 40), '#Error6' ) productSbu,
  left(pdhProduct.productBrand, 40) productBrand,
  left(pdhProduct.productSubBrand, 40) productSubBrand,
  left(pdhProduct.marketingCode, 40) marketingCode,

  main.regionalProductStatus,
  left(nvl(pdhProduct.productM4Category, '#Error22'), 40) productM4Category,
  pdhProduct.originId,
  pdhProduct.originDescription,
  left(pdhProduct.productM4Group, 40) productM4Group,
  left(pdhProduct.productM4Family, 40) productM4Family,
  case 
    when pdhProduct.ansStdUom is null then '#Error18' 
    when pdhProduct.ansStdUom not in ('PAIR', 'PIECE') then '#Error19' 
    else pdhProduct.ansStdUom
  end ansStdUom,
  pdhProduct.piecesInCarton,
  main.gtcReplacementFlag  
  
from 
   main_40_2a main
   left join pdhProduct on main.productCode = pdhProduct.productCode 
where 
  1=1
  and nvl(pdhProduct.itemClass,'X') not in ('Ancillary Products')
 """)
  
main_40_3_inv.createOrReplaceTempView('main_40_3_inv')

# COMMAND ----------

# edm  
main_40_4_inv = spark.sql("""
select 
  main._SOURCE,
  pdhProductFlag _PDHPRODUCTFLAG,
  '1' Pyramid_Level,
  left(main.productCode,40) Forecast_1_ID,
  main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
  left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
  'AA' Record_Type,
  name Description,  
 'Y' Forecast_calculate_indicator,
  nvl(qvunitpricePdh.AVG_UNIT_SELLING_PRICE,0) Unit_price,
  nvl(qvunitpricePdh.AVG_UNIT_COST_PRICE,0) Unit_cost,
  main.Unit_cube,
  main.Unit_Weight,
  'N' Product_group_conversion_option,
  1 Product_group_conversion_factor,
  main.Forecast_Planner,
  main.productStyle || '_' || main.sizeDescription User_Data_84,
  main.productStyle  User_Data_01,
  main.sizeDescription User_Data_02,
  main.productStyle || '_' || main.sizeDescription User_Data_03,
  main.productGbu User_Data_04,
  main.productSbu User_Data_05,
  main.productBrand User_Data_06,
  main.productSubBrand User_Data_07,
  main.productStyle User_Data_08,
  main.region User_Data_09,
  main.forecastArea User_Data_10,
  main.subRegion User_Data_11,
  main.channel User_Data_12,
  main.subRegion || '_' || main.channel User_Data_13,
  main.marketingCode User_Data_14,
  main.productStatus User_Data_15,
  main.regionalProductStatus User_Data_16,
  '' User_Data_17,
  '' User_Data_18,
  '' User_Data_19,
  '' User_Data_20,
  '' User_Data_21,
  '' User_Data_22,
  main.productM4Category User_Data_23,
  'Undefined' User_Data_24,
  'Undefined' User_Data_25,
  'Undefined' User_Data_26,
  'Undefined' User_Data_27,
  main.originId  User_Data_28,
  main.originDescription User_Data_29,
  main.productM4Group User_Data_30,
  main.productM4Family User_Data_31,
  main.productM4Category User_Data_32,
  main.customerGbu || '_' || main.forecastGroup User_Data_33,
  'M' Cube_unit_of_measure,
  'KG' Weight_unit_of_measure,
  main.ansStdUom Unit_of_measure,
  case when main.ansStdUom = 'PIECE' then main.piecesInCarton else main.piecesInCarton / 2 end Case_quantity,
  main.gtcReplacementFlag,
  main.orderNumber,
  current_date creationDate   
from
  main_40_3_inv main 
  left join sap.qvunitprice qvunitpricePdh on  main.productStylePdh  = qvunitpricePdh.StyleCode
    and main.subRegionGis = qvunitpricePdh.subRegion
""")
  
main_40_4_inv.createOrReplaceTempView('main_40_4_inv')

# COMMAND ----------

# edm

main20_ss_missing = spark.sql("""
select main20.* from main_20_4_ss main20
left join LOM_AA_DO_L1 main5 on main5.Forecast_1_ID = main20.Forecast_1_ID and main5.Forecast_2_ID = main20.Forecast_2_ID and main5.Forecast_3_ID = main20.Forecast_3_ID
where main5.Forecast_1_ID  is null
""")

main40_inv_missing = spark.sql("""
select main40.* from main_40_4_inv main40
left join LOM_AA_DO_L1 main5 on main5.Forecast_1_ID = main40.Forecast_1_ID and main5.Forecast_2_ID = main40.Forecast_2_ID and main5.Forecast_3_ID = main40.Forecast_3_ID
where main5.Forecast_1_ID  is null
""")

# COMMAND ----------

LOM_AA_DO_L1_ALL = (
  LOM_AA_DO_L1.select('*')
  .unionByName(main20_ss_missing.select('*'), allowMissingColumns=True) # edm
  .unionByName(main40_inv_missing.select('*'), allowMissingColumns=True)  # edm
)
LOM_AA_DO_L1_ALL.createOrReplaceTempView('LOM_AA_DO_L1_ALL')

# COMMAND ----------

LOM_AA_DO_L1_ALL1 = (
  LOM_AA_DO_L1.select('*')
  .unionByName(main20_ss_missing.select('*'), allowMissingColumns=True) # edm
  .unionByName(main40_inv_missing.select('*'), allowMissingColumns=True)  # edm
)
LOM_AA_DO_L1_ALL.createOrReplaceTempView('LOM_AA_DO_L1_ALL')

# COMMAND ----------

LOM_AA_DO_L1_ALL_UNIQUE = spark.sql("""
select 
      max(_SOURCE) _SOURCE,
      max(_PDHPRODUCTFLAG) _PDHPRODUCTFLAG,
      max(Pyramid_Level) Pyramid_Level,
      Forecast_1_ID,
      Forecast_2_ID,
      Forecast_3_ID,
      max(Record_Type) Record_Type,
      max(description) description, 
      max(Forecast_calculate_indicator) Forecast_calculate_indicator,
      max(Unit_price) Unit_price,
      max(Unit_cost) Unit_cost,
      max(Unit_cube) Unit_cube,
      max(Unit_weight) Unit_weight, 
      max(Product_group_conversion_option) Product_group_conversion_option,
      max(Product_group_conversion_factor) Product_group_conversion_factor,
      max(Forecast_Planner) Forecast_Planner,
      max(User_Data_84) User_Data_84,
      max(User_Data_01) User_Data_01,
      max(User_Data_02) User_Data_02,
      max(User_Data_03) User_Data_03,
      max(User_Data_04) User_Data_04,
      max(User_Data_05) User_Data_05,
      max(User_Data_06) User_Data_06,
      max(User_Data_07) User_Data_07,
      max(User_Data_08) User_Data_08,
      max(User_Data_09) User_Data_09,
      max(User_Data_10) User_Data_10,
      max(User_Data_11) User_Data_11,
      max(User_Data_12) User_Data_12,
      max(User_Data_13) User_Data_13,
      max(User_Data_14) User_Data_14,
      max(User_Data_15) User_Data_15,
      max(User_Data_16) User_Data_16,
      max(User_Data_17) User_Data_17,
      max(User_Data_18) User_Data_18,
      max(User_Data_19) User_Data_19,
      max(User_Data_20) User_Data_20,
      max(User_Data_21) User_Data_21,
      max(User_Data_22) User_Data_22,
      max(User_Data_23) User_Data_23,
      max(User_Data_24) User_Data_24,
      max(User_Data_25) User_Data_25,
      max(User_Data_26) User_Data_26,
      max(User_Data_27) User_Data_27,
      max(User_Data_28) User_Data_28,
      max(User_Data_29) User_Data_29,
      max(User_Data_30) User_Data_30,
      max(User_Data_31) User_Data_31,
      max(User_Data_32) User_Data_32,
      max(User_Data_33) User_Data_33,
      max(Cube_unit_of_measure) Cube_unit_of_measure,
      max(Weight_unit_of_measure) Weight_unit_of_measure,
      max(Unit_of_measure) Unit_of_measure,
      max(Case_quantity) Case_quantity,
      max(gtcReplacementFlag) gtcReplacementFlag,
      max(orderNumber) orderNumber,
      max(creationDate) creationDate
  from 
    LOM_AA_DO_L1_ALL
    where _SOURCE not in ('DEFAULT')
  group by
     Forecast_1_ID,
     Forecast_2_ID,
     Forecast_3_ID
""")
LOM_AA_DO_L1_ALL_UNIQUE.createOrReplaceTempView('LOM_AA_DO_L1_ALL_UNIQUE')

# COMMAND ----------

# DBTITLE 1,Product LOM AA from Orders, Shipments and Orders
# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_AA_DO_L1_HIST', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AA_DO_L1_ALL_UNIQUE.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,41.1  Build LOM_XX data set - Shipments
main_41_ss = spark.sql("""
WITH MAIN_41_SS AS
 (Select
  _source,
  pdhProductFlag,
  '1' as Pyramid_Level,
  productCode Forecast_1_ID,
  main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
  left(main.customerGbu || '_' || main.forecastGroup, 40) Forecast_3_ID,
  'XX' as Record_type,
  cast(yearShipped as decimal(4,0)) as Year,
  cast(monthShipped as decimal(2,0)) as Period,
  nvl(main.quantityShipped,0) as User_Array_1,
  '' as User_Array_1_sign,
  0 as User_Array_2,
  '' as User_Array_2_sign,
  0 as User_Array_8,
  '' as User_Array_8_sign
From 
  main_20_3 main
)
select 
  _source,
  pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_type,
  Year,
  Period,
  sum(User_Array_1) User_Array_1,
  User_Array_1_sign,
  User_Array_2,
  User_Array_2_sign,
  User_Array_8,
  User_Array_8_sign
from 
  MAIN_41_SS
group by
  _source,
  pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_type,
  Year,
  Period,
  User_Array_1_sign,
  User_Array_2,
  User_Array_2_sign,
  User_Array_8,
  User_Array_8_sign
""")
  
main_41_ss.createOrReplaceTempView('main_41_ss')


# COMMAND ----------

# DBTITLE 1,41.2  Build LOM_XX data set - Invoices
main_41_inv = spark.sql("""
WITH MAIN_41_INV AS
(Select
  _source,
  pdhProductFlag,
  '1' as Pyramid_Level,
  productCode Forecast_1_ID,
  main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
  left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
  'XX' as Record_type,
  cast(yearInvoiced as decimal(4,0)) as Year,
  cast(monthInvoiced as decimal(2,0)) as Period,
  0 as User_Array_1,
  '' as User_Array_1_sign,
  nvl(main.quantityInvoiced,0) as User_Array_2,
  '' as User_Array_2_sign,
  0 as User_Array_8,
  '' as User_Array_8_sign
From 
  main_40_3_inv main
)
select 
  _source,
  pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_type,
  Year,
  Period,
  User_Array_1,
  User_Array_1_sign,
  sum(User_Array_2) User_Array_2,
  User_Array_2_sign,
  User_Array_8,
  User_Array_8_sign
from 
  MAIN_41_INV
group by 
  _source,
  pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_type,
  Year,
  Period,
  User_Array_1,
  User_Array_1_sign,
  User_Array_2_sign,
  User_Array_8,
  User_Array_8_sign
""")
  
main_41_inv.createOrReplaceTempView('main_41_inv')

# COMMAND ----------

# DBTITLE 1,41.3  Build LOM_XX data set - Order Lines
main_41_ol = spark.sql("""
WITH MAIN_41_OL as
(
Select
  _source,
  pdhProductFlag,
  '1' as Pyramid_Level,
  productCode Forecast_1_ID,
  main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
  left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
  'XX' as Record_type,
  cast(yearRequestDeliveryBy as decimal(4,0)) as Year,
  cast(monthRequestDeliveryBy as decimal(2,0)) as Period,
  0 as User_Array_1,
  '' as User_Array_1_sign,
  0 as User_Array_2,
  '' as User_Array_2_sign,
  (orderLines) as User_Array_8,
  '' as User_Array_8_sign
From 
 -- main_40_3_ol main
  main_5 main
)
select 
  _source,
  pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_type,
  Year,
  Period,
  User_Array_1,
  User_Array_1_sign,
  User_Array_2,
  User_Array_2_sign,
  sum(User_Array_8) User_Array_8,
  User_Array_8_sign
from 
  MAIN_41_OL
group by 
  _source,
  pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_type,
  Year,
  Period,
  User_Array_1,
  User_Array_1_sign,
  User_Array_2,
  User_Array_2_sign,
  User_Array_8_sign
""")
  
main_41_ol.createOrReplaceTempView('main_41_ol')

# COMMAND ----------

# DBTITLE 1,42. Union LOM_XX data sets
main_XX = (
  main_41_ss.select('*')
  .union(main_41_inv.select('*')
  .union(main_41_ol.select('*'))      )
)

main_XX.createOrReplaceTempView('main_XX')

# COMMAND ----------

# DBTITLE 1,43. Build LOM_XX data set
LOM_XX_L1 = spark.sql("""
WITH LOM_XX_L1 AS
(select 
  _source,
  pdhProductFlag _pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_Type,
  Year,
  Period,
  sum(User_Array_1) User_Array_1,
  sum(User_Array_2) User_Array_2,
  sum(User_Array_8) User_Array_8
from 
  main_xx
group by 
  _source,
  pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_Type,
  Year,
  Period
)
select 
  _source,
  _pdhProductFlag,
  Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  Record_Type,
  Year,
  Period,
  (case when User_Array_1 < 0 then User_Array_1 * -1 else User_Array_1 end) User_Array_1,
  case when User_Array_1 < 0 then '-' else '' end User_Array_1_sign,
  (case when User_Array_2 < 0 then User_Array_2 * -1 else User_Array_2 end) User_Array_2,
  case when User_Array_2 < 0 then '-' else '' end User_Array_2_sign,
  (case when User_Array_8 < 0 then User_Array_8 * -1 else User_Array_8 end) User_Array_8,
  case when User_Array_8 < 0 then '-' else '' end User_Array_8_sign
from 
  LOM_XX_L1
""")
  
LOM_XX_L1.createOrReplaceTempView('LOM_XX_L1')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_XX_DO_L1_HIST', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_XX_L1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,Get in transit routes
FND_FLEX_VALUES_VL = spark.sql("""
select
  B.FLEX_VALUE routeName,
  B.VALUE_CATEGORY routeKey,
  B.ATTRIBUTE1 routeAttribute,
  B.ATTRIBUTE2 portOfDeparture,
  B.ATTRIBUTE3 routeType, 
  B.ATTRIBUTE4 portOfArrival,
  B.ATTRIBUTE5 transitTime
from
  EBS.FND_FLEX_VALUES_TL T,
  EBS.FND_FLEX_VALUES B
where
  B.FLEX_VALUE_ID = T.FLEX_VALUE_ID
  and T.LANGUAGE = 'US' 
  and B.FLEX_VALUE_SET_ID = '1017215'
  and B.ATTRIBUTE3 = 'FRT'
""")
FND_FLEX_VALUES_VL.createOrReplaceTempView('FND_FLEX_VALUES_VL')
# FND_FLEX_VALUES_VL.display()

# COMMAND ----------

# DBTITLE 1,Define Make/Buy items (to be added to item_org table #1610 and #1479)
make_items = spark.sql("""
  select 
      'EBS' _source,
      msib.segment1 productCode,
      'DC' || mtl.organization_code organizationCode,
    cast(coalesce(msib.fixed_lead_time,lm.OrderLeadTime,0) as decimal(3,0)) fixedLeadTime
    from 
      ebs.mtl_system_items_b msib
      join ebs.mtl_parameters mtl on msib.organization_id = mtl.organization_id
      join spt.locationmaster lm on 'DC' || mtl.organization_code = lm.globalLocationCode
    where 
      msib.PLANNING_MAKE_BUY_CODE = '1'
      and  mtl.organization_code not in ('000','250','325','355','811','824','825','832','834' )
      
""")
make_items.createOrReplaceTempView('make_items')


# COMMAND ----------

# DBTITLE 1,Define intra org transit times
intra_org_transit = spark.sql("""
select distinct
    sm.DEFAULT_FLAG, 
    sm.INTRANSIT_TIME, 
    FR.ORGANIZATION_CODE as FROM_ORG, 
    flc.LOCATION_CODE, 
    flc.DESCRIPTION, 
    sm.FROM_LOCATION_ID, 
    TT.ORGANIZATION_CODE as TO_ORG,  
    tlc.LOCATION_CODE, 
    tlc.DESCRIPTION,
    sm.TO_LOCATION_ID
from
  ebs.msc_interorg_ship_methods sm,
  ebs.mtl_parameters FR,
  (
    SELECT
      loc.location_id,
      lot.location_code,
      loc.inventory_ORGANIZATION_id,
      lot.DESCRIPTION
    from
      ebs.hr_locations_all loc,
      ebs.hr_locations_all_tl lot
    where
      loc.location_id = lot.location_id
      and lot.language = 'US'
  )  flc,
  ebS.HR_ALL_ORGANIZATION_UNITS fhorg,
  ebs.mtl_parameters TT,
  (
    SELECT
      loc.location_id,
      lot.location_code,
      loc.inventory_ORGANIZATION_id,
      lot.DESCRIPTION
    from
      ebs.hr_locations_all loc,
      ebs.hr_locations_all_tl lot
    where
      loc.location_id = lot.location_id
      and lot.language = 'US'
  ) tlc,
  ebs.HR_ALL_ORGANIZATION_UNITS thorg
WHERE
  flc.inventory_ORGANIZATION_id = FR.ORGANIZATION_id
  and flc.inventory_ORGANIZATION_id = fhorg.ORGANIZATION_id
  and tlc.inventory_ORGANIZATION_id = TT.ORGANIZATION_id
  and tlc.inventory_ORGANIZATION_id = thorg.ORGANIZATION_id
  and sm.FROM_LOCATION_ID = flc.LOCATION_ID
  and sm.TO_LOCATION_ID = tlc.LOCATION_ID
  and fhorg.DATE_TO is null
  and thorg.DATE_TO is null
  and sm.DEFAULT_FLAG = '1'
""")
intra_org_transit.createOrReplaceTempView('intra_org_transit')

# COMMAND ----------

inventory_items = spark.sql("""
select distinct 
   inv._source,
   pr.e2eProductCode productCode,
   'DC' || org.organizationCode DC,
   pr.ansStdUomConv,
   pdhProduct.originId,
   case when pdhProduct.productCode is null then '#Error1' end pdhProductFlag
from 
  s_supplychain.inventory_agg inv
  join s_core.product_agg pr on inv.item_id = pr._id
  left join s_core.product_org_agg pr_org on inv.item_ID = pr_org.item_ID 
        and inv.inventoryWarehouse_ID = pr_org.organization_ID
  join s_core.organization_agg org on inv.inventoryWarehouse_ID = org._ID
  join s_core.organization_agg comp on inv.owningBusinessUnit_ID = comp._ID
  left join pdhProduct 
    on pr.e2eProductCode = pdhProduct.productCode
  where inv._source in  (select sourceSystem from source_systems)
    and org.isActive
    and year(inv.inventoryDate) * 100 + month(inv.inventoryDate) =  (select max(year(inv.inventoryDate) * 100 + month(inv.inventoryDate)) from s_supplychain.inventory_agg inv where year(inv.inventoryDate) * 100 + month(inv.inventoryDate) <  (select runPeriod from run_period))
    and inv.primaryQty > 0
    and not inv._deleted
    and comp.organizationCode not in (select organization from exclude_organizations)
 --   and pr_org.productStatus not in ('DISCONTINUED', 'Obsolete', 'Inactive')
""")
inventory_items.createOrReplaceTempView('inventory_items')
inventory_items.display()

# COMMAND ----------

demand_items = spark.sql("""
select distinct
  _source,
  productCode,
  DC,
  ansStdUomConv,
  originId,
  pdhProductFlag
from 
  main_5
""")

# COMMAND ----------

all_items = (
  demand_items.select('*')
  .union(inventory_items.select('*'))
).distinct()

all_items.createOrReplaceTempView('all_items')
all_items.count()

# COMMAND ----------

# DBTITLE 1,CDR AI Scenario 3a
cdr_ai_3a = spark.sql("""
-- only MAKE items
-- Source Location #1 is the same as Forecast_2_ID
select distinct
  '3a' scenario,
   null _source, --main._source, 
   '2'  Pyramid_Level,
   main.productCode Forecast_1_ID, 
   main.DC Forecast_2_ID,
   'AI' Record_type,
   main.DC Source_location_1,
   make_items.fixedLeadTime Replenishment_lead_time_1,
   'Internal'  Vendor,
   'DEFAULT' DRP_planner,
   'M' Make_buy_code,
   nvl(cast(main.ansStdUomConv as decimal(7,0)), '#Error31') Order_multiple,
   Cast(10 as numeric(3,0)) Network_level,
   main.originId,
   main.pdhProductFlag
 from 
  all_items main
  join make_items on main.productCode = make_items.productCode and main.DC = make_items.organizationCode
where 1=1

""")

cdr_ai_3a.createOrReplaceTempView('cdr_ai_3a')
# cdr_ai_3a.display()
cdr_ai_3a.count()

# COMMAND ----------

# DBTITLE 1,CDR AI Scenario 3b
cdr_ai_3bi = spark.sql("""
-- not make items
-- default sourcing rule = 'Origin
-- use origin code from respective item in PDH
select distinct
  '3bi' scenario,
  null _source, --main._source, 
  '2'  Pyramid_Level,
  main.productcode Forecast_1_ID, 
  main.dc Forecast_2_ID,  
  'AI' Record_type,
  nvl(globalLocationsOrigins.globallocationcode, '#Error22' || '-' || main.originId) Source_location_1, 
  case
    when globalLocationsDC.GTCPOAPortOfArrival is null then '#Error25-' || main.dc
    when globalLocationsOrigins.GTCPODPortOfDeparture is null then '#Error23-' || main.originId
    when FND_FLEX_VALUES_VL.transitTime is null then '#Error33-' || 'POD= ' || globalLocationsOrigins.GTCPODPortOfDeparture || ', POA = ' || globalLocationsDC.GTCPOAPortOfArrival
    when FND_FLEX_VALUES_VL.transitTime = 0 then '#Error34-' || 'POD= ' || globalLocationsOrigins.GTCPODPortOfDeparture || ', POA = ' || globalLocationsDC.GTCPOAPortOfArrival
    when nvl(globalLocationsOrigins.orderleadtime,0) = 0 then '#Error35-' || main.originId  
    when TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) > 999 then '#Error36' || 'POD= ' || globalLocationsOrigins.GTCPODPortOfDeparture || ', POA = ' || globalLocationsDC.GTCPOAPortOfArrival
    else cast(TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) as numeric(3,0))
  end Replenishment_lead_time_1,
  case 
    when main._source = 'EBS' then nvl(globalLocationsOrigins.EBSSUPPLIERCODE, '#Error27-' || main.originId)
    when main._source = 'SAP' then nvl(globalLocationsOrigins.SAPGOODSSUPPLIERCODE, '#Error28-' || main.originId)
    when main._source = 'TOT' then  nvl(globalLocationsOrigins.MICROSIGASUPPLIERCODE, '#Error39-' || main.originId)
    when main._source = 'KGD' then  nvl(globalLocationsOrigins.KINGDEESUPPLIERCODE, '#Error40-' || main.originId)
    when main._source = 'COL' then  nvl(globalLocationsOrigins.DYNAMICSNAVSUPPLIERCODECOLOMBIA, '#Error41-' || main.originId)
    else '#Error32-' || main._source || '-' || main.originId
  end Vendor,
   'DEFAULT' DRP_planner,
   case 
     when globalLocationsOrigins.Locationtype = 'Vendor Location' then 'B'
     when globalLocationsOrigins.Locationtype = 'Manufacturing Facility' then 'X'
     else '#Error26' || '-' || main.originId
   end Make_buy_code,
   nvl(cast(main.ansStdUomConv as decimal(7,0)), '#Error31') Order_multiple,
   Cast(10 as numeric(3,0)) Network_level,
   main.originId,
   main.pdhProductFlag,
   globalLocationsDC.GTCPOAPortOfArrival,
   globalLocationsOrigins.GTCPODPortOfDeparture
from 
  all_items main
  join (select 
            Globallocationcode dc, GTCPOAPortOfArrival, GTCPODPortOfDeparture
        from 
            spt.locationmaster
        where 
            Defaultsourcingrule = 'Origin') globalLocationsDC on main.dc= globallocationsDC.dc
  join (select 
            origin originCode, 
            globallocationcode, 
            GTCPODPortOfDeparture, 
            GTCPOAPortOfArrival, 
            Locationtype, 
            REPLACE(STRING(INT(EBSSUPPLIERCODE)), ",", "") EBSSUPPLIERCODE, 
            REPLACE(STRING(INT (SAPGOODSSUPPLIERCODE)), ",", "") SAPGOODSSUPPLIERCODE,
            REPLACE(STRING(INT(MICROSIGASUPPLIERCODE)), ",", "") MICROSIGASUPPLIERCODE, 
            REPLACE(STRING(INT(KINGDEESUPPLIERCODE)), ",", "") KINGDEESUPPLIERCODE, 
            REPLACE(STRING(INT(DYNAMICSNAVSUPPLIERCODECOLOMBIA)), ",", "") DYNAMICSNAVSUPPLIERCODECOLOMBIA, 
            INT (orderleadtime) orderleadtime
        from 
            spt.locationmaster) globalLocationsOrigins on main.originId = globalLocationsOrigins.originCode
  left join FND_FLEX_VALUES_VL on globalLocationsDC.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival and globalLocationsOrigins.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
where 1=1
  and main.productcode || '-' ||  main.DC not in (select productcode || '-' || organizationcode from make_items)
""")
cdr_ai_3bi.createOrReplaceTempView('cdr_ai_3bi')
cdr_ai_3bi.count()

# COMMAND ----------

## closing
cdr_ai_3bii = spark.sql("""
select distinct
  '3bii' scenario,
  _source,
  Pyramid_Level,
  Forecast_1_ID,
  Source_location_1 Forecast_2_ID,
  'AI' Record_type,
  Source_location_1, 
  cast(5 as numeric(3,0)) Replenishment_lead_time_1,
  'N/A' Vendor,
  DRP_planner,
  'M' Make_buy_code,
  Order_multiple,
  Cast(90 as numeric(3,0)) Network_level,
  pdhProductFlag
from 
  cdr_ai_3bi
""")
  
cdr_ai_3bii.createOrReplaceTempView('cdr_ai_3bii')

# cdr_ai_3bii.display()

# COMMAND ----------

cdr_ai_3ci = spark.sql("""
with c1 as
(select distinct
  '3ci' scenario,
  null _source, --main._source, 
  '2'  Pyramid_Level,
  main.productcode Forecast_1_ID, 
  main.DC Forecast_2_ID,
  'AI' Record_type,
  nvl(globalLocationsDC.Defaultsourcingrule, '#Error22' || '-' || main.originId)  Source_location_1, 
  case
    when globalLocationsDC.GTCPOAPortOfArrival is null then '#Error25-' || main.dc
    when globalLocationsOrigins.GTCPODPortOfDeparture is null then '#Error23-' || main.originId
    when FND_FLEX_VALUES_VL.transitTime is null and intra_org_transit.from_org is null then '#Error33-' || 'POD= ' || globalLocationsOrigins.GTCPODPortOfDeparture || ', POA = ' || globalLocationsDC.GTCPOAPortOfArrival
    when FND_FLEX_VALUES_VL.transitTime = 0 then '#Error34-' || 'POD= ' || globalLocationsOrigins.GTCPODPortOfDeparture || ', POA = ' || globalLocationsDC.GTCPOAPortOfArrival
    when nvl(globalLocationsOrigins.orderleadtime,0) = 0 then '#Error35-' || main.originId  
    when FND_FLEX_VALUES_VL.TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) > 999 then '#Error36-' || 'POD= ' || globalLocationsOrigins.GTCPODPortOfDeparture || ', POA = ' || globalLocationsDC.GTCPOAPortOfArrival
    when intra_org_transit.from_org is not null then cast(intra_org_transit.intransit_time as numeric(3,0))
    else cast(FND_FLEX_VALUES_VL.TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) as numeric(3,0))
  end Replenishment_lead_time_1,
  nvl(globalLocationsDC.Defaultsourcingrule, '#Error22' || '-' || main.originId) Vendor,
  'DEFAULT' DRP_planner,
  'X' Make_buy_code,
  nvl(cast(main.ansStdUomConv as decimal(7,0)), '#Error31') Order_multiple,
  Cast(10 as numeric(3,0)) Network_level,
  main.originId,
  globalLocationsDC.Defaultsourcingrule,
  main.pdhProductFlag,
  globalLocationsDC.GTCPOAPortOfArrival,
  globalLocationsOrigins.GTCPODPortOfDeparture
from 
  all_items main
  join (select 
            Globallocationcode dc, GTCPOAPortOfArrival,GTCPODPortOfDeparture , Defaultsourcingrule
        from 
            spt.locationmaster
        where 
            Defaultsourcingrule <> 'Origin' and Defaultsourcingrule is not null) globalLocationsDC on main.dc= globallocationsDC.dc
  join (select 
            origin originCode, 
            globallocationcode, 
            GTCPODPortOfDeparture, 
            GTCPOAPortOfArrival, 
            Locationtype, 
            REPLACE(STRING(INT(EBSSUPPLIERCODE)), ",", "") EBSSUPPLIERCODE, 
            REPLACE(STRING(INT (SAPGOODSSUPPLIERCODE)), ",", "") SAPGOODSSUPPLIERCODE,
            INT (orderleadtime) orderleadtime
        from 
            spt.locationmaster) globalLocationsOrigins on globalLocationsDC.Defaultsourcingrule = globalLocationsOrigins.GlobalLocationCode
  left join FND_FLEX_VALUES_VL on globalLocationsDC.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival and globalLocationsOrigins.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
  left join intra_org_transit on globalLocationsOrigins.GTCPODPortOfDeparture = intra_org_transit.from_org and globalLocationsDC.GTCPOAPortOfArrival = intra_org_transit.to_org
 where 1=1
  and main.originId is not null
  and main.productcode || '-' ||  main.DC not in (select productcode || '-' || organizationcode from make_items))
select 
  scenario,
  _source, 
  Pyramid_Level,
  Forecast_1_ID, 
  Forecast_2_ID,
  Record_type,
  Source_location_1, 
  Replenishment_lead_time_1,
  Vendor,
  DRP_planner,
  Make_buy_code,
  Order_multiple,
  Network_level,
  originId,
  Defaultsourcingrule,
  pdhProductFlag,
  GTCPOAPortOfArrival,
  GTCPODPortOfDeparture
from c1
  
""")
cdr_ai_3ci.createOrReplaceTempView('cdr_ai_3ci')

cdr_ai_3ci.count()

# COMMAND ----------

cdr_ai_3cii = spark.sql("""

select 
  '3cii' scenario,
  main._source,
  main.Pyramid_Level,
  Forecast_1_ID,
  main.Source_location_1 Forecast_2_ID,
  'AI' Record_type,
  case when main.originId is null then '#Error29' else nvl(globallocationsDC.globallocationcode, '#Error30' || '-' || main.originId) end Source_location_1,
  case
    when globalLocationsSources.GTCPOAPortOfArrival is null then '#Error25-' || main.Source_location_1
    when globalLocationsDC.GTCPODPortOfDeparture is null then '#Error23-' || main.originId
    when FND_FLEX_VALUES_VL.transitTime is null then '#Error33-' || 'POD= ' || globalLocationsDC.GTCPODPortOfDeparture || ', POA = ' || globalLocationsSources.GTCPOAPortOfArrival
    when FND_FLEX_VALUES_VL.transitTime = 0 then '#Error34-' || 'POD= ' || globalLocationsDC.GTCPODPortOfDeparture || ', POA = ' || globalLocationsSources.GTCPOAPortOfArrival
    when nvl(globalLocationsDC.orderleadtime,0) = 0 then '#Error35-' || main.originId  
    when TransitTime +  nvl(globalLocationsDC.orderleadtime,0) > 999 then '#Error36-' || 'POD= ' || globalLocationsDC.GTCPODPortOfDeparture || ', POA = ' || globalLocationsSources.GTCPOAPortOfArrival
    else cast(TransitTime +  nvl(globalLocationsDC.orderleadtime,0) as numeric(3,0))
  end Replenishment_lead_time_1,
  nvl(globallocationsDC.globallocationcode, '#Error30' || '-' || main.originId) Vendor,
  'DEFAULT' DRP_planner,
  'X' Make_buy_code,
  Order_multiple,
  Cast(20 as numeric(3,0)) Network_level,
  globallocationsDC.Defaultsourcingrule,
  --null Defaultsourcingrule,
  main.pdhProductFlag,
  globalLocationsDC.GTCPODPortOfDeparture,
  globalLocationsSources.GTCPOAPortOfArrival
  
from 
  cdr_ai_3ci main
  join (select 
            origin, globallocationcode, GTCPOAPortOfArrival, GTCPODPortOfDeparture, Defaultsourcingrule, 
            INT (orderleadtime) orderleadtime
        from 
            spt.locationmaster) globalLocationsDC on main.originId= globallocationsDC.origin 
  join (select 
            origin, globallocationcode, GTCPOAPortOfArrival, GTCPODPortOfDeparture, Defaultsourcingrule
        from 
            spt.locationmaster) globalLocationsSources on main.Source_location_1= globalLocationsSources.globallocationcode 
  left join FND_FLEX_VALUES_VL on globalLocationsSources.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival and globalLocationsDC.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
""")
cdr_ai_3cii.createOrReplaceTempView('cdr_ai_3cii')

cdr_ai_3cii.display()

# COMMAND ----------

##-- cdr_ai_3ciii
cdr_ai_3ciii = spark.sql("""
select 
  '3ciii' scenario,
   main._source,
  main.Pyramid_Level,
  Forecast_1_ID,
  main.Source_location_1 Forecast_2_ID,
  'AI' Record_type,
  main.Source_location_1 Source_location_1,
  Cast(1 as numeric(3,0))  Replenishment_lead_time_1,
  main.Source_location_1 Vendor,
  'DEFAULT' DRP_planner,
  'M' Make_buy_code,
  Order_multiple,
  Cast(90 as numeric(3,0)) Network_level,
  main.pdhProductFlag
from 
  cdr_ai_3cii main
where defaultsourcingrule is  null
 and main.Forecast_1_ID || '-' || main.Source_location_1 || '-' ||  main.Source_location_1 not in (select distinct Forecast_1_ID || '-' || Forecast_2_ID || '-' || Source_location_1 from  cdr_ai_3bii)
   """)
cdr_ai_3ciii.createOrReplaceTempView('cdr_ai_3ciii')

cdr_ai_3ciii.display()

# COMMAND ----------

cdr_ai = (
  cdr_ai_3a.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag' )
  .union(cdr_ai_3bi.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag'  ))
  .union(cdr_ai_3bii.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag'  ))
  .union(cdr_ai_3ci.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag'  ))
  .union(cdr_ai_3cii.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag'  ))
  .union(cdr_ai_3ciii.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag'  ))
)

cdr_ai.createOrReplaceTempView('cdr_ai')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_ip_cdr_ai_l2_hist', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
cdr_ai.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,51.1 Build IP LOM AA data set #1
main_51_1 = spark.sql("""
select distinct 
   inv._source,
   inv.item_id,
   inv.inventoryWarehouse_ID,
   pr.e2eProductCode productCode,
   'DC' || org.organizationCode DC,
   pr.e2eProductCode || '-DC' || org.organizationCode || '-WH' key 
from 
  s_supplychain.inventory_agg inv
  join s_core.product_agg pr on inv.item_id = pr._id
  left join s_core.product_org_agg pr_org on inv.item_ID = pr_org.item_ID 
        and inv.inventoryWarehouse_ID = pr_org.organization_ID
  join s_core.organization_agg org on inv.inventoryWarehouse_ID = org._ID
  where inv._source in  (select sourceSystem from source_systems)
    --and pr.productcode || '-DC' || org.organizationCode not in (select distinct productcode || '-' || DC from main_5 )
    and org.isActive
    and year(inv.inventoryDate) * 100 + month(inv.inventoryDate) =  (select max(year(inv.inventoryDate) * 100 + month(inv.inventoryDate)) from s_supplychain.inventory_agg inv where year(inv.inventoryDate) * 100 + month(inv.inventoryDate) <  (select runPeriod from run_period))
    
    and inv.primaryQty > 0
    and not inv._deleted
""")
main_51_1.createOrReplaceTempView('main_51_1')
main_51_1.display()

# COMMAND ----------

loaded_items=spark.sql("""
select distinct forecast_1_id || '-' || forecast_2_id key
  from 
 g_tembo.lom_aa_do_l1_hist -- to be replaced with archive table
where 
forecast_1_id not like '%#Error%'
and forecast_2_id not like '%#Error%'
""")
loaded_items.createOrReplaceTempView('loaded_items')

# COMMAND ----------

# DBTITLE 1,51.1.a Find substitution if available
main_51_2 = spark.sql("""
select 
  main._SOURCE,
  main.item_id,
  main.inventoryWarehouse_id,
  case when nvl(HistoricalProductSubstitution.final_successor, main.productCode) = ''
    then '#Error37'
  else nvl(HistoricalProductSubstitution.final_successor, main.productCode) end productCode,
  case when HistoricalProductSubstitution.final_successor is not null
    then Predecessor_of_Final_successor
  end predecessorCode,
  nvl(case when main.DC in ('DC813') then 'DC822' else main.DC end, '#Error9') DC,
  main.key
from 
   main_51_1 main
   left join HistoricalProductSubstitution on main.key =   HistoricalProductSubstitution.key
--where nvl(HistoricalProductSubstitution.final_successor, 'Include') not in ('dis')
""")
  
main_51_2.createOrReplaceTempView('main_51_2')


# COMMAND ----------

# DBTITLE 1,51.1.b. replace GTC product code with PDH product Code
main_51_3 = spark.sql("""
select 
  main._SOURCE,
  main.item_id,
  main.inventoryWarehouse_id,
  main.DC,
  nvl(gtc_replacement.item,main.productCode) productCode,
  case when gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID is null then false else true end gtcReplacementFlag
from
  main_51_2 main
  left join gtc_replacement on main.productCode = gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID
where 
  nvl(gtc_replacement.ITEM_STATUS, 'Active') not in ('Inactive')
""")
main_51_3.createOrReplaceTempView('main_51_3')

# COMMAND ----------

# DBTITLE 1,51.2 Build IP LOM AA data set #2
LOM_AA_IP_L2 = spark.sql("""
select 
      main._source,
      '2' Pyramid_Level,
      main.productCode Forecast_1_ID,
      main.DC Forecast_2_ID,
      '' Forecast_3_ID,
      'AA' Record_Type,
      pr.e2eItemDescription Description,
      'N' Forecast_calculate_indicator,
      cast(0 as decimal(11,5)) Unit_price,
      cast(0 as decimal(11,5)) Unit_cost,
      cast(nvl(pdhProduct.caseVolume,0) as decimal(9,4)) Unit_cube,
      cast(nvl(pdhProduct.caseNetWeight,0) as decimal(9,4)) Unit_weight,
      'N' Product_group_conversion_option,
      cast(100000 as decimal(11,5)) Product_group_conversion_factor,
      'DEFAULT' Forecast_Planner,
      nvl(pdhProduct.productStyle, 'No Style') || '_' || nvl(pdhProduct.sizeDescription, 'No Size')  User_Data_84,  
      nvl(pdhProduct.productStyle, 'No Style') User_Data_01,
      nvl(pdhProduct.sizeDescription, 'No Size')  User_Data_02,
      nvl(pdhProduct.productStyle, 'No Style') || '_' || nvl(pr.sizeDescription, 'No Size') User_Data_03, 
      pdhProduct.gbu User_Data_04,
      pdhProduct.productSbu User_Data_05,
      pdhProduct.productBrand User_Data_06,
      pdhProduct.productSubBrand User_Data_07,
      main.productCode User_Data_08,
      pdhProduct.marketingCode User_Data_14,
      pdhProduct.productStatus User_Data_15,
      pr_org.productStatus User_Data_16,
      pdhProduct.ansStdUom Unit_of_measure,
      gtcReplacementFlag
from 
  main_51_3 main
  join s_core.product_agg pr on main.item_id = pr._id
  join s_core.product_org_agg pr_org on main.item_id = pr_org.item_id
          and main.inventoryWarehouse_ID = pr_org.organization_ID
  left join (select * from s_core.product_agg where _source = 'PDH' and not _deleted) pdhProduct on main.productCode = pdhProduct.productCode 
where pr.productcode || '-' || main.DC not in (select key from loaded_items)--(select distinct productcode || '-' || DC  from main_5 )
""")
LOM_AA_IP_L2.createOrReplaceTempView('LOM_AA_IP_L2')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_ip_lom_aa_l2_hist', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AA_IP_L2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,52.1 Build IP LOM AU data set #1
LOM_AU_IP_L2 = spark.sql("""
select 
      '2' Pyramid_Level,
      main.productCode Forecast_1_ID,
      main.DC Forecast_2_ID,
      '' Forecast_3_ID,
      'AU' Record_Type,
      main.DC User_Data_43
from 
  main_51_3 main
  join s_core.product_agg pr on main.item_id = pr._id
  join s_core.product_org_agg pr_org on main.item_id = pr_org.item_id
           and main.inventoryWarehouse_ID = pr_org.organization_ID
where pr.productcode || '-' || main.DC not in (select key from loaded_items)--(select distinct productcode || '-' || DC  from main_5 )
     
""")
LOM_AU_IP_L2.createOrReplaceTempView('LOM_AU_IP_L2')
--LOM_AU_IP_L2.count()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_ip_lom_au_l2_hist', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AU_IP_L2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,SPS_ITEMLOC_RESOURCE
sql_SPS_ITEMLOC_RESOURCE = """
select 
main_5.productCode ITEM_INDX,
cms.LocationID LOC_INDX,
cms.ResourceIndex RSRC_INDX,
main_5.ansStdUomConv PROD_MIN_QTY,
main_5.ansStdUomConv PROD_MULT_QTY

from
main_5 
left join spt.capacitymasterresourcemaster cms
on productstyle = capacitygroup and main_5.sizeDescription = cms.size and cms.title='Primary'
"""

create_tmp_table(sql_SPS_ITEMLOC_RESOURCE, "SPS_ITEMLOC_RESOURCE_hist")

# COMMAND ----------

# DBTITLE 1,SPS_ITEMLOC_PROD_CONV
sql_SPS_ITEMLOC_PROD_CONV = """
select
productCode as Item,                                    
cms.location as Location,
cms.ProductionResource as Production_Resource,
CURRENT_DATE as Begin_Date,
cast(null as string) as End_Date,
1 as Material_Cost,
UnitsPerHour as Units_per_Hour_ORHours_Per_Unit,
1 as Duration_in_Days,
cms.REGLR_CPCTY_QTY as Hours_Per_Day,
cast(null as decimal) Shelf_Life_Days

from
main_5 
left join spt.capacitymasteritemresource cms
on productstyle = capacitygroup and main_5.sizeDescription = cms.size and cms.title='Primary'
"""

create_tmp_table(sql_SPS_ITEMLOC_PROD_CONV, "SPS_ITEMLOC_PROD_CONV_hist")
