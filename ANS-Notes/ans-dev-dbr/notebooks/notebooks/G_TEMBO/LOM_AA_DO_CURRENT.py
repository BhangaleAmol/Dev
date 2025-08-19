# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

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
table_name = get_input_param('table_name', default_value = 'TMP_LOM_AA_DO_L1')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# DBTITLE 1,0.1a Determine current runPeriod
run_period  = spark.sql("select {0} as runPeriod".format(get_run_period()))
run_period.createOrReplaceTempView("run_period")

# COMMAND ----------

# DBTITLE 1,0.1b Determine latest PO/TO Period
latestOrderPeriod=spark.sql("""
  select year(add_months(current_date, 18)) * 100 + month(add_months(current_date, 18)) toPeriod
""")
latestOrderPeriod.createOrReplaceTempView('latestOrderPeriod')

# COMMAND ----------

# DBTITLE 1,0.2. Build temp substitution table - Current
E2EProductSubstitution = spark.sql("""
select distinct
  predecessorCode || '-' || DCValue || '-' || DSWHValue key,
  predecessorCode,
  SuccessorCode,
  DCValue DC,
  DSWHValue DSWH,
  PredecessorCodeDescription,
  Phaseoutdate
from spt.globalproductsubstitutionmaster
where 
  not _deleted
""")

E2EProductSubstitution.createOrReplaceTempView("E2EProductSubstitution")

# COMMAND ----------

# DBTITLE 1,0.3 Determine active source systems
source_systemssql= "select distinct cvalue sourceSystem from smartsheets.edm_control_table where table_id = 'TEMBO_CONTROLS'   and key_value = '{0}_SOURCE_SYSTEM'   and active_flg is true   and current_date between valid_from and valid_to".format(ENV_NAME)

source_systems  = spark.sql(source_systemssql)
source_systems.createOrReplaceTempView("source_systems")

# COMMAND ----------

# DBTITLE 1,0.4 Build temp substitution table - Historical
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

locationmaster = spark.sql("""
select 
  CountryCode,
  LocationAbbreviationValue,
  LocationType,
  Description,
  Origin,
  GlobalLocationCode,
  GlobalLocationName,
  StatusValue,
  LengthLocationNameMax40,
  City,
  LatitudeLongitude,
  InventoryHoldingLegalEntityName,
  InventoryHoldingERP,
  InventoryHoldingERPLocationCode,
  InventoryHoldingERPLocationDescription,
  GTCPODPortOfDeparture,
  GTCPOAPortOfArrival,
  SAPGOODSSUPPLIERCODE,
  EBSSUPPLIERCODE,
  KINGDEESUPPLIERCODE,
  cast(MICROSIGASUPPLIERCODE as integer) MICROSIGASUPPLIERCODE,
  DYNAMICSNAVSUPPLIERCODECOLOMBIA,
  DefaultSourcingRule,
  AlternativeSourcingRule1_ForDC,
  AlternativeSourcingRule2DC,
  OrderLeadTime,
  MainOrigin,
  Vendor_id
from spt.locationmaster lm
  where nvl(lm.StatusValue, 'Inactive') = 'Active'
  and GlobalLocationCode is not null
  and MainOrigin = 'Y'
  and  not _deleted
""")
locationmaster.createOrReplaceTempView('locationmaster')

# COMMAND ----------

# DBTITLE 1,0.5 get LOM MPX L0 product substitution
main_MPX_L0 = spark.sql("""
select distinct
predecessorCode,
  SuccessorCode,
  DCValue DC,
  to_date(phaseOutDate,'MM/dd/yyyy HH:mm:ss') phaseOutDate,
  SubRegionValue Subregion,
  DSWHValue DSWH 
from spt.globalproductsubstitutionmaster
join locationmaster lm on DCValue = lm.globalLocationCode
where successorcode is not null
   and UPPER(predecessorcode) not in ('N/A')
   and date_format(to_date(phaseOutDate,'MM/dd/yyyy HH:mm:ss'),'yyyyMMdd')  <= date_format(current_date,'yyyyMMdd') 
    and not globalproductsubstitutionmaster._deleted 
""")
 
main_MPX_L0.createOrReplaceTempView('main_MPX_L0')
# main_MPX_L0.display()

# COMMAND ----------

# DBTITLE 1,0.5a get LOM MPX product substitution New Items
main_MPX_new_items = spark.sql("""  
select distinct
  predecessorCode,
  SuccessorCode,
  DCValue DC,
  SubRegionValue Subregion,
  DSWHValue DSWH
from spt.globalproductsubstitutionmaster
  join locationmaster lm on DCValue = lm.globalLocationCode
where successorcode is not null
  and UPPER(predecessorcode) = 'N/A'
  and phaseInDate <= current_date
  and SuccessorCode not in ('Dummy')
  and not globalproductsubstitutionmaster._deleted
""")
  
main_MPX_new_items.createOrReplaceTempView('main_MPX_new_items')
# main_MPX_new_items.display()

# COMMAND ----------

# DBTITLE 1,0.6 Get PDH product master records
pdhProduct = spark.sql("""
select 
  * 
from 
  s_core.product_agg 
where _source = 'PDH'
  and not _deleted
  and productCode not like 'Del%'
""")
pdhProduct.createOrReplaceTempView('pdhProduct')
# pdhProduct.display()

# COMMAND ----------

# DBTITLE 1,0.7 Determine organizations to exclude
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

# DBTITLE 1,0.8 Get product details for replaced item numbers
regionalproductstatus = spark.sql("""
select 
  pr_org.organization_id, 
  pr_org.item_id,
  pr.e2eProductCode,
  pr_org.productstatus,
  pr_org.mtsMtoFlag,
  nvl(org.region,concat('#Error49','_', nvl(org.commonorganizationCode, 'DC' || org.organizationCode))) orgRegion,
  nvl(org.commonorganizationCode, 'DC' || org.organizationCode) organizationCode
from s_core.product_org_agg pr_org
  join s_core.product_agg pr on 
     pr_org.item_id = pr._ID
  join s_core.organization_agg org
    on pr_org.organization_id = org._id
where pr.productcode = pr.e2eproductcode
  and not pr._deleted
  and not pr_org._deleted
  and pr._source  in  (select sourceSystem from source_systems) 

""")
regionalproductstatus.createOrReplaceTempView("regionalproductstatus")

# COMMAND ----------

globalregionproductstatus = spark.sql("""
select 
  productcode,
  region,
  productStatus
from 
  s_core.product_region_agg
where 
  not _deleted
  and _source = 'PDH'
""")
globalregionproductstatus.createOrReplaceTempView("globalregionproductstatus")
# globalregionproductstatus.display()

# COMMAND ----------

# DBTITLE 1,0.9 Get sub region override table
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
# sub_region_override.display()

# COMMAND ----------

# DBTITLE 1,0.10 Get Subinventory Hold Codes
TEMBO_HOLD_SUBINVENTORY_CODES=spark.sql("""
select 
  distinct key_value subInventoryCode
from 
  smartsheets.edm_control_table
where 
  table_id = 'TEMBO_HOLD_SUBINVENTORY_CODES'
  and active_flg = 'true'
  and not _deleted
  and current_date between valid_from and valid_to
""")
TEMBO_HOLD_SUBINVENTORY_CODES.createOrReplaceTempView('TEMBO_HOLD_SUBINVENTORY_CODES')
# TEMBO_HOLD_SUBINVENTORY_CODES.display()

# COMMAND ----------

# Temporary, to be moved to inventory_sap
WHS_LKP = spark.sql("""select distinct z_low  from sapp01.ZBC_DEV_PARAM
 where 1=1
 and ID_PROGRAM = 'ZMM_PRODUCT_QUANTITY'
 and ID_PARAM = 'PRODUCT_QUANTITY'
 and PARAM1 = 'WERKS'
 and nvl(_deleted, 0) = 0""")
WHS_LKP.createOrReplaceTempView('WHS_LKP')

# COMMAND ----------

locationmasterNonPrimary = spark.sql("""
select * from spt.locationmaster lm
  where nvl(lm.StatusValue, 'Inactive') = 'Active'
  and GlobalLocationCode is not null
  and  not _deleted
""")
locationmasterNonPrimary.createOrReplaceTempView('locationmasterNonPrimary')

# COMMAND ----------

current_capacity_master=spark.sql("""
select 
    line,
    capacityGroup,
    Calendar,
    ConstrainedResIndi,
    ConstrActiveIndi,
    Style,
    size, 
    location,
    plannerID,
    case when PlanningTimeFence - 30 < 30 then 30 else PlanningTimeFence - 30 end PlanningTimeFence,
    left(productionResource,40) productionResource,
    resourceType,
    rateOrUnit,
    grossOutput,
    regularCapacityQuantity,
    oeePct,
    unitsPerHour,
    uom
from s_core.capacity_master_agg
where 1=1
  and current_date between beginDate and endDate
  and not _deleted
""")
current_capacity_master.createOrReplaceTempView('current_capacity_master')

# COMMAND ----------

current_primary_capacity_master=spark.sql("""
select 
    Style,
    size, 
    Location,
    min(plannerID) plannerID,
    min(case when PlanningTimeFence - 30 < 30 then 30 else PlanningTimeFence - 30 end) PlanningTimeFence
from s_core.capacity_master_agg
where 1=1
  and current_date between beginDate and endDate
  and resourceType = 'Primary'
  and not _deleted
group by
    location,
    Style,
    size
""")
current_primary_capacity_master.createOrReplaceTempView('current_primary_capacity_master')

# COMMAND ----------

DBO_CM_RESOURCE_DATA=spark.sql("""
select 
  * 
from 
  tmb2.dbo_cm_resource_data
where 
  not _deleted
""")
DBO_CM_RESOURCE_DATA.createOrReplaceTempView('DBO_CM_RESOURCE_DATA')

# COMMAND ----------

COLUMBIA_SUPPLIER_CODES=spark.sql("""
select 
  substring(KEY_VALUE, 4, 20) keyValue,
  substring(CVALUE,4,20) cValue
from smartsheets.edm_control_table
where 1=1
  and table_id = 'COLUMBIA_SUPPLIER_CODES'
  and not _deleted
  and active_flg is True
  and current_date between valid_from and valid_to
""")
COLUMBIA_SUPPLIER_CODES.createOrReplaceTempView('COLUMBIA_SUPPLIER_CODES')

# COMMAND ----------

# DBTITLE 1,1. Get base selection for LOM AA DO - Current
main_1 = spark.sql("""
WITH MAIN_1 as
(
select 
  acc.accountnumber,
  pr.e2eProductCode || '-' || coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) || '-' ||  orderType.dropShipmentFlag key,
  pr.e2eProductCode productCode,
  ol.inventoryWarehouse_ID, 
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
    --when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion in ('BR') then 'BR'
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion not in ('MX') and oh._source = 'EBS' then 'OLAC' 
    else  nvl(soldtoTerritory.subRegion, '#Error8-' || soldtoTerritory.territoryCode) 
  end subRegion,
  coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, 'DC' || inv.organizationcode)  DC ,
  orderType.dropShipmentFlag Channel,
  soldtoTerritory.territoryCode,
  soldtoTerritory.subRegionGis,
  case when nvl(soldto.siteCategory,'X') = 'LA' then 'LAC' else nvl(soldtoTerritory.Region, '#Error14') end Region,
  case 
    when inv.region = 'EMEA' and left(pr.gbu, 1) = 'M' then 'EURAF'
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.forecastArea not in ('MX','CO') and oh._source = 'EBS' then 'OLAC' 
    else soldtoTerritory.forecastArea 
  end forecastArea,
  ol._SOURCE,
  oh.orderNumber,
  case 
    when pr_org.mtsMtoFlag is null 
      then 'MTS' 
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
  then 
    (ol.quantitycancelled  * ol.ansStdUomConv)
  else
    (nvl(ol.quantityOutstandingOrders,0) + nvl(ol.quantityFutureOrders,0)) * ol.ansStdUomConv
  end quantityOutstandingOrders,
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
  ol.lineNumber,
  ol.requestDeliveryBy,
  ol.pricePerUnit / exchangeRateUsd pricePerUnitUSD,
  nvl(inv.region,concat('#Error49','-',coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, 'DC' || inv.organizationcode))) orgRegion
from 
  s_supplychain.sales_order_lines_agg ol
  join s_supplychain.sales_order_headers_agg oh on ol.salesorder_id = oh._ID
  join s_core.product_agg pr on ol.item_ID = pr._ID
  join s_core.account_agg acc on oh.customer_ID = acc._ID
  left join s_core.customer_location_agg soldto on oh.soldtoAddress_ID = soldto._ID
  join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
  join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
  join s_core.transaction_type_agg orderType on oh.orderType_ID = orderType._id  
  left join sub_region_override on nvl(soldto.partySiteNumber, soldto.addressid) = sub_region_override.partySiteNumber
  left join s_core.territory_agg soldtoTerritory on nvl(sub_region_override.country_ID, soldto.territory_ID) = soldtoTerritory._id  
  left join s_core.account_organization_agg acc_org on oh.customerOrganization_ID = acc_org._id
  left join regionalproductstatus  pr_org on pr.ProductCode = pr_org.e2eProductCode
                                    and ol.inventoryWarehouse_ID = pr_org.organization_ID  
where
  not ol._deleted
  and not oh._deleted
  and oh._source  in  (select sourceSystem from source_systems) 
  and inv.isActive
  and coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) not in ('DC325', '000', '250','325', '355', '811', '821', '802', '824', '825', '832', '834')
  and year(ol.requestDeliveryBy) * 100 + month(ol.requestDeliveryBy)  >=  (select runPeriod from run_period)
  and (nvl(orderType.e2eFlag, true) or ol.e2eDistributionCentre = 'DC827'
    or CONCAT(orderType.transactionId , '-' , nvl(UPPER(pr.packagingCode),'')) in (select CONCAT(KEY_VALUE,'-',CVALUE)
                                                                                 from smartsheets.edm_control_table 
                                                                                 WHERE table_id = 'ADDITIONAL_ORDER_TYPE_PACK_TYPE')
       )
  and nvl(pr.itemType, 'X') not in  ('Service')
  and (nvl(ol.orderlinestatus, 'Include') not like ('%CANCELLED%')
    --or cancelReason like '%COVID-19%' 
      or cancelReason like '%PLANT DIRECT BILLING%'
      or cancelReason like '%Plant Direct Billing Cancellation%')
  and nvl(pr.productdivision, 'Include') not in ('SH&WB')
  and nvl(acc.customerType, 'External') not in ('Internal')
  and oh.customerId is not null
  and pr.productCode not like 'PALLET%'
  and pr.itemType in ('FINISHED GOODS', 'ACCESSORIES','ZPRF')
  and orderType.name not like 'AIT_DIRECT SHIPMENT%'
  and ol.bookedFlag = 'Y'
  --and upper(nvl(pr_org.productStatus,'X')) not in ('INACTIVE', 'OBSOLETE', 'OBSOLETE (OLD VALUE)')
  and OH._SOURCE || '-' || upper(nvl(pr_org.productStatus,'X')) not in ('EBS-INACTIVE', 'EBS-OBSOLETE', 'KGD-INACTIVE', 'KGD-OBSOLETE','TOT-INACTIVE', 'TOT-OBSOLETE','COL-INACTIVE', 'COL-OBSOLETE','SAP-INACTIVE')
  and org.organizationcode not in (select organization from exclude_organizations)
  and ol.reasonForRejection is null
  --and oh.ordernumber  in ('1661403', '1661401')
)
select 
  accountnumber,
  key,
  productCode,
  inventoryWarehouse_ID,
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
  sum(quantityOutstandingOrders) quantityOutstandingOrders,
  sum(orderAmountUsd) orderAmountUsd,
  customerTier,
  yearRequestDeliveryBy,
  monthRequestDeliveryBy,
  sum(orderlines) orderlines,
  regionalProductStatus,
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  orgRegion
from MAIN_1
group by
  accountnumber,
  key,
  productCode,
  inventoryWarehouse_ID,
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
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  orgRegion
""")
  
main_1.createOrReplaceTempView('main_current_1')
#main_1.columns

# COMMAND ----------

# DBTITLE 1,2. Find substitution if available
main_2 = spark.sql("""
with main_2 as (
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
  main.quantityOutstandingOrders,
  main.orderAmountUsd,
  main.yearRequestDeliveryBy,
  main.monthRequestDeliveryBy,
  main.orderlines,
  main.customerTier,
  main.key,
  main.regionalProductStatus,
  main.inventoryWarehouse_ID,
--   pr_org.productStatus,
  main.lineNumber,
  main.requestDeliveryBy,
  main.pricePerUnitUSD,
  main.orgRegion
from 
   main_current_1 main
   left join HistoricalProductSubstitution on main.key =   HistoricalProductSubstitution.key
where nvl(HistoricalProductSubstitution.final_successor, 'Include') not in ('dis')
  )
select
  main_2._SOURCE,
  main_2.productCode,
  main_2.predecessorCode,
  main_2._SOURCE,
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
  main_2.quantityOutstandingOrders,
  main_2.orderAmountUsd,
  main_2.yearRequestDeliveryBy,
  main_2.monthRequestDeliveryBy,
  main_2.orderlines,
  main_2.customerTier,
  main_2.key,
--   main_2.regionalProductStatus,
  pr_org.productStatus regionalProductStatus,
  main_2.lineNumber,
  main_2.requestDeliveryBy,
  main_2.pricePerUnitUSD,
  main_2.orgRegion
from main_2
left join regionalproductstatus pr_org on main_2.productCode = pr_org.e2eProductCode and main_2.inventoryWarehouse_ID = pr_org.organization_ID

""")
  
main_2.createOrReplaceTempView('main_2')

# COMMAND ----------

# DBTITLE 1,2.1 GTC Replacement
gtc_replacement = spark.sql("""
  select 
    ORACLE_PRODUCT_ID_OR_GTC_ID, 
    ITEM_STATUS,
    min(item) item 
  from pdh.master_records 
  where 
    nvl(STYLE_ITEM_FLAG, 'N') = 'N' 
    and ORACLE_PRODUCT_ID_OR_GTC_ID not in ('GTC')
    and ORACLE_PRODUCT_ID_OR_GTC_ID not like 'Rule generated%'
    and ITEM_STATUS not in ('Inactive', 'Discontinue')
  group by 
    ORACLE_PRODUCT_ID_OR_GTC_ID,
    ITEM_STATUS
""")
  
gtc_replacement.createOrReplaceTempView('gtc_replacement')

# COMMAND ----------

# DBTITLE 1,3.a. replace GTC product code with PDH product Code
main_2a = spark.sql("""
select 
  main_2._SOURCE,
  nvl(gtc_replacement.item,main_2.productCode) productCode,
  main_2.predecessorCode,
  main_2.subRegion,
--  case
--    when main_2.subRegion = 'UK-E' and main_2.DC = 'DCNLT1'
--        then main_2.DC
--    when main_2.subRegion = 'UK-E' and main_2.DC <> 'DCNLT1'  and _source = 'SAP' then main_2.DC 
--    when main_2.subRegion <> 'UK-E' and main_2.DC = 'DCNLT1'  and _source = 'SAP' then 'DCANV1'
--    else main_2.DC
--  end DC,
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
  main_2.quantityOutstandingOrders,
  main_2.orderAmountUsd,
  main_2.yearRequestDeliveryBy,
  main_2.monthRequestDeliveryBy,
  main_2.customerTier,
  case when gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID is null then false else true end gtcReplacementFlag,
  gtc_replacement.ITEM_STATUS,
  main_2.orderlines,
  regionalProductStatus,
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  orgRegion
from
  main_2
  left join gtc_replacement on main_2.productCode = gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID
where 
  nvl(gtc_replacement.ITEM_STATUS, 'Active') not in ('Inactive')
""")
main_2a.createOrReplaceTempView('main_2a')

# COMMAND ----------

# DBTITLE 1,3. Add Product data from source systems and PDH data - Current
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
  main_2.quantityOutstandingOrders,
  main_2.orderAmountUsd,
  main_2.yearRequestDeliveryBy,
  main_2.monthRequestDeliveryBy,
  case when left(pdhProduct.productStyle,1) = '0' then substr(pdhProduct.productStyle, 2, 50) else pdhProduct.productStyle end productStylePdh,
  case when pdhProduct.productCode is null then '#Error1' end pdhProductFlag,
  left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36) description,
  Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
  Cast(pdhProduct.caseGrossWeight as numeric(9,4)) Unit_weight,
  nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  sizeDescription,
  nvl(left(pdhProduct.gbu, 40), '#Error5' ) productGbu,
  nvl(left(pdhProduct.productSbu, 40), '#Error6' ) productSbu,
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
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  pdhProduct.packagingCode,
  pdhProduct.baseProduct,
  orgRegion
from
  main_2a main_2
  left join pdhProduct on main_2.productCode = pdhProduct.productCode 
 -- left join s_core.product_agg erpProduct on main_2.productCode = erpProduct.productCode and main_2._SOURCE = erpProduct._SOURCE
where
  1=1
--   and nvl(pdhProduct.itemClass,'X') not in ('Ancillary Products')
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
  main.quantityOutstandingOrders,
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
     when upper(main.forecastGroup) in ('I_FHP GROUP') then 'I_FHP GROUP'
     when upper(main.forecastGroup) like 'I_GRAINGER%' then 'I_GRAINGER*' 
     when main.customerGbu in ('I', 'N')
      and main.Region = 'NA'
      and main.productGbu not in ('MEDICAL')
      and upper(main.forecastGroup) not like 'I_GRAINGER%'  then 'I_* & <> I_GRAINGER*'
    when upper(main.customerGbu) in ('I', 'N', 'O', 'U') then 'I_*'
  end key,
  nvl(plannerCodeL1.forecastplannerid, '#Error2') forecastL1plannerid,
  nvl(plannerCodeL3.forecastplannerid, '#Error3') forecastL3plannerid,
  main.orderlines,
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  packagingCode,
  baseProduct,
  orgRegion
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
  and   case
           when main.customerGbu = 'M' then 'M_*'
           when upper(main.forecastGroup) in ('I_FHP GROUP') then 'I_FHP GROUP'
           when upper(main.forecastGroup) like 'I_GRAINGER%' then 'I_GRAINGER*' 
           when main.customerGbu in ('I', 'N')
              and main.Region = 'NA'
              and main.productGbu not in ('MEDICAL')
              and upper(main.forecastGroup) not like 'I_GRAINGER%'  then 'I_* & <> I_GRAINGER*'
           when upper(main.customerGbu) in ('I', 'N', 'O', 'U') then 'I_*'
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

# DBTITLE 1,5. Add average cost and sales price if available - Current
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
  main.quantityOutstandingOrders,
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
  nvl(qvunitpricePdh.AVG_UNIT_COST_PRICE,0.00001) Unit_cost,
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  pdhProduct.packagingCode,
  pdhProduct.baseProduct,
  orgRegion
from main_4 main
  left join sap.qvunitprice qvunitpricePdh on  main.productStylePdh  = qvunitpricePdh.StyleCode
    and main.subRegionGis = qvunitpricePdh.subRegion
  left join pdhProduct on main.productCode = pdhProduct.productCode
  join locationmaster lm on main.dc = lm.globallocationcode
""")
main_5.cache()  
main_5.createOrReplaceTempView('main_5')

# COMMAND ----------

# DBTITLE 1,5.1.Product LOM AA DO (L1) - Current
LOM_AA_DO_L1 = spark.sql("""
select distinct
    main._SOURCE,
    main.pdhProductFlag _PDHPRODUCTFLAG,
    '1' Pyramid_Level,
    left(main.productCode,40) Forecast_1_ID,
    main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
    left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
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
    nvl(main.baseProduct, main.productStyle) User_Data_08,
    main.region User_Data_09,
    main.forecastArea User_Data_10,
    main.subRegion User_Data_11,
    main.channel User_Data_12,
    main.subRegion || '_' || main.channel User_Data_13,
    main.marketingCode User_Data_14,
    main.productStatus User_Data_15,
    nvl(globalregionproductstatus.productstatus, main.regionalProductStatus) User_Data_16,
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
    left join globalregionproductstatus 
      on left(main.productCode,40) = globalregionproductstatus.productCode 
        and case when main.DC in ('DSNA', 'DSLAC') then 'NALAC' else substring(main.dc,3,5) end = globalregionproductstatus.region
""")

LOM_AA_DO_L1.createOrReplaceTempView('LOM_AA_DO_L1')
# LOM_AA_DO_L1.display()

# COMMAND ----------

LOM_AA_DO_ARCHIVE_Keys=spark.sql("""
with cte1 as
(select distinct forecast_1_id, forecast_2_id, forecast_3_id from LOM_AA_DO_L1
union all
select distinct forecast_1_id, forecast_2_id, forecast_3_id from g_tembo.lom_aa_do_l1_hist_archive
)
select distinct forecast_1_id, forecast_2_id, forecast_3_id from cte1
""")
LOM_AA_DO_ARCHIVE_Keys.createOrReplaceTempView('LOM_AA_DO_ARCHIVE_Keys')

# COMMAND ----------

LOM_AA_DO_ARCHIVE_1=spark.sql("""
with cte1 as
(select 
        Pyramid_Level,
        Forecast_1_ID,
        Forecast_2_ID,
        Forecast_3_ID,
        Record_Type,
        Description,
        Forecast_calculate_indicator,
        Unit_price,
        Unit_cost,
        Unit_cube,
        Unit_Weight,
        Product_group_conversion_option,
        Product_group_conversion_factor,
        Forecast_Planner,
        User_Data_84,
        User_Data_01,
        User_Data_02,
        User_Data_03,
        User_Data_04,
        User_Data_05,
        User_Data_06,
        User_Data_07,
        User_Data_08,
        User_Data_09,
        User_Data_10,
        User_Data_11,
        User_Data_12,
        User_Data_13,
        User_Data_14,
        User_Data_15,
        User_Data_16,
        User_Data_17,
        User_Data_18,
        User_Data_19,
        User_Data_20,
        User_Data_21,
        User_Data_22,
        User_Data_23,
        User_Data_24,
        User_Data_25,
        User_Data_26,
        User_Data_27,
        User_Data_28,
        User_Data_29,
        User_Data_30,
        User_Data_31,
        User_Data_32,
        User_Data_33,
        Cube_unit_of_measure,
        Weight_unit_of_measure,
        Unit_of_measure,
        Case_quantity
from 
  LOM_AA_DO_L1
UNION 
select * from g_tembo.LOM_AA_DO_L1_hist_archive 
)
select * 
from 
  cte1
  join locationmaster lm on split(forecast_2_id, '_')[1] = lm.globallocationcode
""")
LOM_AA_DO_ARCHIVE_1.createOrReplaceTempView('LOM_AA_DO_ARCHIVE_1')

# COMMAND ----------

LOM_AU_DO_ARCHIVE_1=spark.sql("""
with tmp3 as
(select * from g_tembo.LOM_AU_DO_L1_ARCHIVE
union all
select * from g_tembo.lom_au_do_l1_hist_archive
)
select distinct tmp3.*  
  from 
    tmp3
  join locationmaster lm on split(forecast_2_id, '_')[1] = lm.globallocationcode
""")
LOM_AU_DO_ARCHIVE_1.createOrReplaceTempView('LOM_AU_DO_ARCHIVE_1')

# COMMAND ----------

LOM_AA_DO_CHANGES = spark.sql("""
with tmp0 as (
  select
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_Type,
    Description,
    Forecast_calculate_indicator,
    Unit_price,
    Unit_cost,
    Unit_cube,
    Unit_Weight,
    Product_group_conversion_option,
    Product_group_conversion_factor,
    Forecast_Planner,
    User_Data_84,
    User_Data_01,
    User_Data_02,
    User_Data_03,
    User_Data_04,
    User_Data_05,
    User_Data_06,
    User_Data_07,
    User_Data_08,
    User_Data_09,
    User_Data_10,
    User_Data_11,
    User_Data_12,
    User_Data_13,
    User_Data_14,
    User_Data_15,
    User_Data_16,
    User_Data_17,
    User_Data_18,
    User_Data_19,
    User_Data_20,
    User_Data_21,
    User_Data_22,
    User_Data_23,
    User_Data_24,
    User_Data_25,
    User_Data_26,
    User_Data_27,
    User_Data_28,
    User_Data_29,
    User_Data_30,
    User_Data_31,
    User_Data_32,
    User_Data_33,
    Cube_unit_of_measure,
    Weight_unit_of_measure,
    Unit_of_measure,
    Case_quantity
  from
    LOM_AA_DO_L1
  UNION
  select
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_Type,
    Description,
    Forecast_calculate_indicator,
    Unit_price,
    Unit_cost,
    Unit_cube,
    Unit_Weight,
    Product_group_conversion_option,
    Product_group_conversion_factor,
    Forecast_Planner,
    User_Data_84,
    User_Data_01,
    User_Data_02,
    User_Data_03,
    User_Data_04,
    User_Data_05,
    User_Data_06,
    User_Data_07,
    User_Data_08,
    User_Data_09,
    User_Data_10,
    User_Data_11,
    User_Data_12,
    User_Data_13,
    User_Data_14,
    User_Data_15,
    User_Data_16,
    User_Data_17,
    User_Data_18,
    User_Data_19,
    User_Data_20,
    User_Data_21,
    User_Data_22,
    User_Data_23,
    User_Data_24,
    User_Data_25,
    User_Data_26,
    User_Data_27,
    User_Data_28,
    User_Data_29,
    User_Data_30,
    User_Data_31,
    User_Data_32,
    User_Data_33,
    Cube_unit_of_measure,
    Weight_unit_of_measure,
    Unit_of_measure,
    Case_quantity
  from
    g_tembo.LOM_AA_DO_L1_hist_archive
),
tmp1 (
  select
    *,
    split(Forecast_2_ID, '_') as org_id
  from
    tmp0
),
tmp (
  select
    *,
    substr(Forecast_3_ID, 3) as forecastGroup,
    left(Forecast_3_ID, 1) as customerGbu,
    User_Data_04 as productGbu,
    User_Data_05 as productSbu,
    User_Data_09 as region,
    org_id [1] as orgid
  from
    tmp1
),
pdhProductCTE (
  select
    left(pdhProduct.productCode, 40) AS Forecast_1_ID,
    nvl(left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36),'#Error12') Description,
    Cast(pdhProduct.caseVolume as numeric(9, 4)) Unit_cube,
    Cast(pdhProduct.caseGrossWeight as numeric(9, 4)) Unit_weight,
    pdhProduct.piecesInCarton,
    case
      when pdhProduct.ansStdUom is null then '#Error18'
      when pdhProduct.ansStdUom not in ('PAIR', 'PIECE') then '#Error19'
      else pdhProduct.ansStdUom
    end Unit_of_measure,
    case
      when ansStdUom = 'PIECE' then pdhProduct.piecesInCarton
      else pdhProduct.piecesInCarton / 2
    end Case_quantity,
    nvl(left(pdhProduct.productStyle, 40), '#Error13') || '_' || nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA') AS User_Data_84,
    nvl(left(pdhProduct.productStyle, 40), '#Error13') AS User_Data_01,
    nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA') AS User_Data_02,
    nvl(left(pdhProduct.productStyle, 40), '#Error13') || '_' || nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA') AS User_Data_03,
    nvl(left(pdhProduct.gbu, 40), '#Error5') AS User_Data_04,
    nvl(left(pdhProduct.productSbu, 40), '#Error6') AS User_Data_05,
    left(pdhProduct.productBrand, 40) AS User_Data_06,
    left(pdhProduct.productSubBrand, 40) AS User_Data_07,
    nvl(pdhProduct.baseProduct,nvl(left(pdhProduct.productStyle, 40), '#Error13')) AS User_Data_08,
    left(pdhProduct.marketingCode, 40) AS User_Data_14,
    left(pdhProduct.productStatus, 40) AS User_Data_15,
    pdhProduct.packagingCode AS User_Data_17,
    '' AS User_Data_18,
    '' AS User_Data_19,
    '' AS User_Data_20,
    '' AS User_Data_21,
    '' AS User_Data_22,
    left(nvl(pdhProduct.productM4Category, '#Error22'),40) AS User_Data_23,
    'Undefined' AS User_Data_24,
    'Undefined' AS User_Data_25,
    'Undefined' AS User_Data_26,
    'Undefined' AS User_Data_27,
    pdhProduct.originId AS User_Data_28,
    pdhProduct.originDescription AS User_Data_29,
    left(pdhProduct.productM4Group, 40) AS User_Data_30,
    left(pdhProduct.productM4Family, 40) AS User_Data_31,
    left(nvl(pdhProduct.productM4Category, '#Error22'),40) AS User_Data_32,
    gbu
  from
    pdhProduct
)
select
  distinct tmp.Pyramid_Level,
  tmp.Forecast_1_ID,
  tmp.Forecast_2_ID,
  tmp.Forecast_3_ID,
  tmp.Record_Type,
  pdhProduct.Description,
  tmp.Forecast_calculate_indicator,
  qvunitpricePdh.AVG_UNIT_SELLING_PRICE Unit_price,
  qvunitpricePdh.AVG_UNIT_COST_PRICE Unit_cost,
  pdhProduct.Unit_cube,
  pdhProduct.Unit_Weight,
  tmp.Product_group_conversion_option,
  tmp.Product_group_conversion_factor,
  plannerCodeL1.forecastplannerid Forecast_Planner,
  pdhProduct.User_Data_84,
  pdhProduct.User_Data_01,
  pdhProduct.User_Data_02,
  pdhProduct.User_Data_03,
  pdhProduct.User_Data_04,
  pdhProduct.User_Data_05,
  pdhProduct.User_Data_06,
  pdhProduct.User_Data_07,
  pdhProduct.User_Data_08,
  tmp.User_Data_09,
  case when tmp.User_Data_09 = 'EMEA' and left(pdhProduct.gbu,1) = 'M'then 'EURAF' else tmp.User_Data_10 end User_Data_10,
  --tmp.User_Data_10,
  tmp.User_Data_11,
  tmp.User_Data_12,
  tmp.User_Data_13,
  pdhProduct.User_Data_14,
  pdhProduct.User_Data_15,
  coalesce(rps.productstatus,globalregionproductstatus.productstatus, '#Error51-' || tmp.Forecast_1_ID || '-' || split(tmp.Forecast_2_ID,'_')[1]) User_Data_16,
  pdhProduct.User_Data_17,
  pdhProduct.User_Data_18,
  pdhProduct.User_Data_19,
  pdhProduct.User_Data_20,
  pdhProduct.User_Data_21,
  pdhProduct.User_Data_22,
  pdhProduct.User_Data_23,
  pdhProduct.User_Data_24,
  pdhProduct.User_Data_25,
  pdhProduct.User_Data_26,
  pdhProduct.User_Data_27,
  pdhProduct.User_Data_28,
  pdhProduct.User_Data_29,
  pdhProduct.User_Data_30,
  pdhProduct.User_Data_31,
  pdhProduct.User_Data_32,
  tmp.User_Data_33,
  tmp.Cube_unit_of_measure,
  tmp.Weight_unit_of_measure,
  pdhProduct.Unit_of_measure,
  pdhProduct.Case_quantity
from
  tmp
  LEFT JOIN sap.qvunitprice qvunitpricePdh on tmp.user_data_08 = qvunitpricePdh.StyleCode
  and User_Data_11 = qvunitpricePdh.subRegion
  LEFT JOIN pdhProductCTE pdhProduct on tmp.Forecast_1_ID = pdhProduct.Forecast_1_ID
  LEFT JOIN regionalproductstatus rps on rps.e2eproductcode = tmp.Forecast_1_ID
  and rps.organizationcode = tmp.orgid
  LEFT JOIN (
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
  ) plannerCodeL1 on tmp.region = plannercodeL1.region
  and tmp.productSbu = plannercodeL1.SBU
  and tmp.productGbu = plannercodeL1.pdhgbu
  and case
    when tmp.customerGbu = 'M' then 'M_*'
    when upper(tmp.forecastGroup) in ('I_FHP GROUP') then 'I_FHP GROUP'
    when upper(tmp.forecastGroup) like 'I_GRAINGER%' then 'I_GRAINGER*'
    when tmp.customerGbu in ('I', 'N')
    and tmp.Region = 'NA'
    and tmp.productGbu not in ('MEDICAL')
    and upper(tmp.forecastGroup) not like 'I_GRAINGER%' then 'I_* & <> I_GRAINGER*'
    when upper(tmp.customerGbu) in ('I', 'N', 'O', 'U') then 'I_*'
  end = plannercodeL1.CustomerForecastGroup
  left join globalregionproductstatus on  tmp.Forecast_1_ID = globalregionproductstatus.productcode
  and case
    when tmp.User_Data_09 in ('NA', 'LAC') then 'NALAC'
    else tmp.User_Data_09
  end = globalregionproductstatus.region
  join locationmaster lm on split(forecast_2_id, '_')[1] = lm.globallocationcode
WHERE
  (tmp.Unit_price <> qvunitpricePdh.AVG_UNIT_SELLING_PRICE
  OR tmp.Unit_cost <> qvunitpricePdh.AVG_UNIT_COST_PRICE
  OR tmp.Unit_cube <> pdhProduct.Unit_cube
  OR tmp.Unit_Weight <> pdhProduct.Unit_Weight
  OR tmp.Unit_of_measure <> pdhProduct.Unit_of_measure
  OR tmp.Case_quantity <> pdhProduct.Case_quantity
  OR tmp.Description <> pdhProduct.Description
  OR tmp.Forecast_Planner <> plannerCodeL1.forecastplannerid
  OR tmp.User_Data_84 <> pdhProduct.User_Data_84
  OR tmp.User_Data_01 <> pdhProduct.User_Data_01
  OR tmp.User_Data_02 <> pdhProduct.User_Data_02
  OR tmp.User_Data_03 <> pdhProduct.User_Data_03
  OR tmp.User_Data_04 <> pdhProduct.User_Data_04
  OR tmp.User_Data_05 <> pdhProduct.User_Data_05
  OR tmp.User_Data_06 <> pdhProduct.User_Data_06
  OR tmp.User_Data_07 <> pdhProduct.User_Data_07
  OR tmp.User_Data_08 <> pdhProduct.User_Data_08
  OR tmp.User_Data_10 <> case when tmp.User_Data_09 = 'EMEA' and left(pdhProduct.gbu,1) = 'M' then 'EURAF' else tmp.User_Data_10 end
  OR tmp.User_Data_14 <> pdhProduct.User_Data_14
  OR tmp.User_Data_15 <> pdhProduct.User_Data_15
  OR nvl(tmp.User_Data_16, 0) <> coalesce(rps.productstatus,globalregionproductstatus.productstatus, '#Error51-' || tmp.Forecast_1_ID || '-' || split(tmp.Forecast_2_ID,'_')[1])
  OR tmp.User_Data_17 <> pdhProduct.User_Data_17
  OR tmp.User_Data_18 <> pdhProduct.User_Data_18
  OR tmp.User_Data_19 <> pdhProduct.User_Data_19
  OR tmp.User_Data_20 <> pdhProduct.User_Data_20
  OR tmp.User_Data_21 <> pdhProduct.User_Data_21
  OR tmp.User_Data_22 <> pdhProduct.User_Data_22
  OR tmp.User_Data_23 <> pdhProduct.User_Data_23
  OR tmp.User_Data_24 <> pdhProduct.User_Data_24
  OR tmp.User_Data_25 <> pdhProduct.User_Data_25
  OR tmp.User_Data_26 <> pdhProduct.User_Data_26
  OR tmp.User_Data_27 <> pdhProduct.User_Data_27
  OR tmp.User_Data_28 <> pdhProduct.User_Data_28
  OR tmp.User_Data_29 <> pdhProduct.User_Data_29
  OR tmp.User_Data_30 <> pdhProduct.User_Data_30
  OR tmp.User_Data_31 <> pdhProduct.User_Data_31
  OR tmp.User_Data_32 <> pdhProduct.User_Data_32)
""")
LOM_AA_DO_CHANGES.createOrReplaceTempView("LOM_AA_DO_CHANGES")
# LOM_AA_DO_CHANGES.display()

# COMMAND ----------

# DBTITLE 1,Add LOM AA L1 records for Successor Items
LOM_AA_DO_L1_MPX_substituted = spark.sql("""
select distinct
        'MPX' _source,
        case when pdhProduct.productCode is null then '#Error1' end _PDHPRODUCTFLAG,
        main.Pyramid_Level,
        mpx.SuccessorCode Forecast_1_ID,
        main.Forecast_2_ID,
        main.Forecast_3_ID,
        main.Record_Type,
        left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36) description, 
        main.Forecast_calculate_indicator,
        nvl(main.Unit_price,0) Unit_price,
        nvl(main.Unit_cost,0.00001) Unit_cost,
        Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
        Cast(pdhProduct.caseGrossWeight as numeric(9,4)) Unit_weight, 
        main.Product_group_conversion_option,
        main.Product_group_conversion_factor,
        main.Forecast_Planner,
        pdhProduct.productStyle || '_' || nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  User_Data_84,
        pdhProduct.productStyle  User_Data_01,
        pdhProduct.sizeDescription User_Data_02,
        pdhProduct.productStyle || '_' || nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  User_Data_03,
        nvl(left(pdhProduct.gbu, 40), '#Error5' ) User_Data_04,
        nvl(left(pdhProduct.productSbu, 40), '#Error6' )  User_Data_05,
        left(pdhProduct.productBrand, 40) User_Data_06,
        left(pdhProduct.productSubBrand, 40) User_Data_07,
        nvl(pdhProduct.baseProduct, nvl(left(pdhProduct.productStyle, 40), '#Error13')) AS User_Data_08,
        main.User_Data_09,
        case when main.User_Data_09 = 'EMEA' and left(pdhProduct.gbu,1) = 'M' then 'EURAF' else main.User_Data_10 end User_Data_10 ,
        --main.User_Data_10,
        main.User_Data_11,
        main.User_Data_12,
        main.User_Data_13,
        pdhProduct.marketingCode User_Data_14,
        pdhProduct.productStatus User_Data_15,
        coalesce(regionalproductstatus.productstatus,globalregionproductstatus.productstatus, '#Error51-' || mpx.successorcode || '-' || split(main.Forecast_2_ID,'_')[1]) User_Data_16,
        main.User_Data_17,
        main.User_Data_18,
        main.User_Data_19,
        main.User_Data_20,
        main.User_Data_21,
        main.User_Data_22,
        pdhProduct.productM4Category User_Data_23,
        main.User_Data_24,
        main.User_Data_25,
        main.User_Data_26,
        main.User_Data_27,
        pdhProduct.originId  User_Data_28,
        pdhProduct.originDescription User_Data_29,
        pdhProduct.productM4Group User_Data_30,
        pdhProduct.productM4Family User_Data_31,
        pdhProduct.productM4Category User_Data_32,
        main.User_Data_33,
        main.Cube_unit_of_measure,
        main.Weight_unit_of_measure,
        main.Unit_of_measure,
        case 
            when main.Unit_of_measure = 'PAIR' 
              then pdhProduct.piecesInCarton  / 2
            else pdhProduct.piecesInCarton  
        end Case_quantity,
        null gtcReplacementFlag,
        null orderNumber,
        current_date creationDate
from 
  LOM_AA_DO_ARCHIVE_1 main
  join main_MPX_L0 mpx on main.Forecast_1_ID = mpx.predecessorcode and left(substr(main.forecast_2_id, instr(main.forecast_2_id, '_')+1), instr(substr(main.forecast_2_id, instr(main.forecast_2_id, '_')+1), '_')-1) = mpx.dc
  left join pdhProduct on mpx.successorcode = pdhProduct.productcode 
  left join regionalproductstatus on mpx.successorcode = regionalproductstatus.e2eProductCode and split(main.Forecast_2_ID,'_')[1] = regionalproductstatus.organizationCode
  left join globalregionproductstatus on mpx.successorcode = globalregionproductstatus.productcode and case when main.User_Data_09 in ('NA', 'LAC') then 'NALAC' else main.User_Data_09 end  = globalregionproductstatus.region
  
where 
 mpx.SuccessorCode not in ('N/A')
 and mpx.SuccessorCode is not null
""")
LOM_AA_DO_L1_MPX_substituted.createOrReplaceTempView('LOM_AA_DO_L1_MPX_substituted')
# LOM_AA_DO_L1_MPX_substituted.display()

# COMMAND ----------

# DBTITLE 1,Add LOM AA L1 records for Successor Items (obsolete)
# LOM_AA_DO_L1_MPX_substituted = spark.sql("""
# with cte1(
#   select 
#     main.*,mpx.SuccessorCode 
#   from 
#     LOM_AA_DO_ARCHIVE_1 main -- archived records
#     join main_MPX_L0 mpx on main.Forecast_1_ID = mpx.predecessorcode and left(substr(main.forecast_2_id, instr(main.forecast_2_id, '_')+1), instr(substr(main.forecast_2_id, instr(main.forecast_2_id, '_')+1), '_')-1) = mpx.dc
# )
# select '' _SOURCE,
#     null _PDHPRODUCTFLAG,
#     * except (SuccessorCode),
#     null gtcReplacementFlag,
#     null orderNumber,
#     current_date creationDate 
# from cte1
# union all
# select 
#     '' _SOURCE,
#     case when pdhProduct.productCode is null then '#Error1' end _PDHPRODUCTFLAG,
#     main.Pyramid_Level,
#     main.successorcode Forecast_1_ID,
#     main.Forecast_2_ID,
#     main.Forecast_3_ID,
#     main.Record_Type,
#     left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36) description, 
#     main.Forecast_calculate_indicator,
#     main.Unit_price,
#     main.Unit_cost,
#     Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
#     Cast(pdhProduct.caseGrossWeight as numeric(9,4)) Unit_weight, 
#     main.Product_group_conversion_option,
#     main.Product_group_conversion_factor,
#     main.Forecast_Planner,
#     pdhProduct.productStyle || '_' || nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  User_Data_84,
#     pdhProduct.productStyle  User_Data_01,
#     pdhProduct.sizeDescription User_Data_02,
#     pdhProduct.productStyle || '_' || nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  User_Data_03,
#     nvl(left(pdhProduct.gbu, 40), '#Error5' ) User_Data_04,
#     nvl(left(pdhProduct.productSbu, 40), '#Error6' )  User_Data_05,
#     left(pdhProduct.productBrand, 40) User_Data_06,
#     left(pdhProduct.productSubBrand, 40) User_Data_07,
#     --pdhProduct.productStyle User_Data_08,  
#     nvl(pdhProduct.baseProduct, nvl(left(pdhProduct.productStyle, 40), '#Error13')) AS User_Data_08,
#     main.User_Data_09 ,
#     main.User_Data_10 ,
#     main.User_Data_11 ,
#     main.User_Data_12 ,
#     main.User_Data_13,
#     pdhProduct.marketingCode User_Data_14,
#     pdhProduct.productStatus User_Data_15,
#     regionalproductstatus.productstatus User_Data_16,
#     main.User_Data_17,
#     main.User_Data_18,
#     main.User_Data_19,
#     main.User_Data_20,
#     main.User_Data_21,
#     main.User_Data_22,
#     pdhProduct.productM4Category User_Data_23,
#     main.User_Data_24,
#     main.User_Data_25,
#     main.User_Data_26,
#     main.User_Data_27,
#     pdhProduct.originId  User_Data_28,
#     pdhProduct.originDescription User_Data_29,
#     pdhProduct.productM4Group User_Data_30,
#     pdhProduct.productM4Family User_Data_31,
#     pdhProduct.productM4Category User_Data_32,
#     main.User_Data_33,
#     main.Cube_unit_of_measure,
#     main.Weight_unit_of_measure,
#     main.Unit_of_measure,
#     case 
#       when main.Unit_of_measure = 'PAIR' 
#       then pdhProduct.piecesInCarton  / 2
#       else pdhProduct.piecesInCarton  
#     end Case_quantity,
#     null gtcReplacementFlag,
#     null orderNumber,
#     current_date creationDate
#   from cte1 main     
#     left join pdhProduct on main.successorcode = pdhProduct.productcode 
#     left join regionalproductstatus on main.successorcode = regionalproductstatus.e2eProductCode and main.Forecast_2_ID = regionalproductstatus.organizationCode
# """)

# LOM_AA_DO_L1_MPX_substituted.createOrReplaceTempView('LOM_AA_DO_L1_MPX_substituted')
# # LOM_AA_DO_L1_MPX_substituted.display()

# COMMAND ----------

LOM_AA_DO_L1_MPX = spark.sql("""
select 
  subs.* 
from 
  LOM_AA_DO_L1_MPX_substituted subs
  left join logftp.logility_do_level_1_for_mpx mpx on 
    subs.forecast_1_id=mpx.Lvl1Fcst and subs.forecast_2_id=mpx.Lvl2Fcst and subs.forecast_3_id=mpx.Lvl3Fcst
where 
  mpx.Lvl1Fcst is null
""")

LOM_AA_DO_L1_MPX.createOrReplaceTempView('LOM_AA_DO_L1_MPX')
# LOM_AA_DO_L1_MPX.display()

# COMMAND ----------

# DBTITLE 1,Add LOM AA L1 for new Items
LOM_AA_DO_L1_MPX_new_items = spark.sql("""
select distinct
    'SPT' as _SOURCE,
    case when pdhProduct.productCode is null then '#Error1' end _PDHPRODUCTFLAG,
    '1' Pyramid_Level,
    mpx.successorcode Forecast_1_ID,
    mpx.subRegion || '_' || mpx.DC || '_' || mpx.DSWH Forecast_2_ID,
    mr.customerGbu || '_' || 'Other' Forecast_3_ID,
    'AA' Record_Type,
    left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36) description, 
    'Y' Forecast_calculate_indicator,
    0 Unit_price,
    0.00001 Unit_cost,
    Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
    Cast(pdhProduct.caseGrossWeight as numeric(9,4)) Unit_weight, 
    'N' Product_group_conversion_option,
    1 Product_group_conversion_factor,
    plannerCodeL1.forecastplannerid Forecast_Planner,
    pdhProduct.productStyle || '_' || nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  User_Data_84,
    pdhProduct.productStyle  User_Data_01,
    pdhProduct.sizeDescription User_Data_02,
    pdhProduct.productStyle || '_' || nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  User_Data_03,
    nvl(left(pdhProduct.gbu, 40), '#Error5' ) User_Data_04,
    nvl(left(pdhProduct.productSbu, 40), '#Error6' )  User_Data_05,
    left(pdhProduct.productBrand, 40) User_Data_06,
    left(pdhProduct.productSubBrand, 40) User_Data_07,
    pdhProduct.baseProduct User_Data_08,
    con.region User_Data_09,
    case when con.region = 'EMEA' and pdhProduct.gbu = 'M'
      then 'EURAF'
    else
      con.forecastArea
    end User_Data_10,
    mpx.subRegion User_Data_11,
    mpx.DSWH User_Data_12,
    mpx.subRegion || '_' || mpx.DSWH User_Data_13,
    pdhProduct.marketingCode User_Data_14,
    pdhProduct.productStatus User_Data_15,
    coalesce(regionalProductStatus.productStatus, globalregionproductstatus.productstatus, '#Error51-' || mpx.successorcode || '-' || mpx.DC) as User_Data_16, 
    pdhProduct.packagingCode User_Data_17,
    '' User_Data_18,
    '' User_Data_19,
    '' User_Data_20,
    '' User_Data_21,
    '' User_Data_22,
    pdhProduct.productM4Category User_Data_23,
    'Undefined' User_Data_24,
    'Undefined' User_Data_25,
    'Undefined' User_Data_26,
    'Undefined' User_Data_27,
    pdhProduct.originId  User_Data_28,
    pdhProduct.originDescription User_Data_29,
    pdhProduct.productM4Group User_Data_30,
    pdhProduct.productM4Family User_Data_31,
    pdhProduct.productM4Category User_Data_32,
    mr.customerGbu || '_' || 'Other' User_Data_33,
    'M' Cube_unit_of_measure,
    'KG' Weight_unit_of_measure,
    case 
      when pdhProduct.ansStdUom is null then '#Error18' 
      when pdhProduct.ansStdUom not in ('PAIR', 'PIECE') then '#Error19' 
      else pdhProduct.ansStdUom
    end Unit_of_measure,
    case 
      when pdhProduct.ansStdUom = 'PAIR' 
      then pdhProduct.piecesInCarton  / 2
      else pdhProduct.piecesInCarton  
    end Case_quantity,
    null as gtcReplacementFlag,
    null as orderNumber,
    current_date creationDate
  from 
    main_MPX_new_items mpx   
    left join (select item,gbu,sbu,case when gbu in ('SINGLE USE','INDUSTRIAL') then 'I' when gbu = 'MEDICAL' then 'M' end as customerGbu,
               case when gbu in ('SINGLE USE','INDUSTRIAL') then 'I_*' when gbu = 'MEDICAL' then 'M_*' end as customerForecastgroup              
             from pdh.master_records) mr on mr.item = mpx.successorcode
    join pdhProduct on mpx.successorcode = pdhProduct.productcode
    left join (select distinct subregion, region, foreCastArea from s_core.territory_agg) con on mpx.subregion = con.subregion
    left join (select
                    case 
                       when customerforecastgroup = 'I_* & <> I_GRAINGER*' 
                       then 'I_*'
                       else customerforecastgroup 
                    end customerforecastgroup,
                    forecastplannerid,
                    pdhgbu,
                    region,
                    sbu
                 from smartsheets.e2eforecastplanner 
                 where level = 'L1-2' 
                     and not _deleted) plannerCodeL1 
      on con.region = plannercodeL1.region      
        and mr.sbu = plannercodeL1.SBU
        and mr.gbu = plannercodeL1.pdhgbu
        and mr.CustomerForecastGroup   = plannercodeL1.CustomerForecastGroup    
    left join regionalproductstatus on mpx.successorcode = regionalproductstatus.e2eProductCode and mpx.DC = regionalproductstatus.organizationCode
    left join globalregionproductstatus on mpx.successorcode = globalregionproductstatus.productcode and case when con.region in ('NA', 'LAC') then 'NALAC' else con.region end  = globalregionproductstatus.region
where 
  mpx.subRegion is not null
  and mpx.dc is not null
""")

LOM_AA_DO_L1_MPX_new_items.createOrReplaceTempView('LOM_AA_DO_L1_MPX_new_items')
# LOM_AA_DO_L1_MPX_new_items.display()

# COMMAND ----------

# DBTITLE 1,5.2.Product LOM AA DO (L4) - Current
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
    main.ordernumber
  from main_5 main 
""")

LOM_AA_DO_L4.createOrReplaceTempView('LOM_AA_DO_L4')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_AA_DO_L4', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AA_DO_L4.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,5.3.Product LOM AU - Current
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
    nvl(main.orgRegion, '#Error49-' || DC) User_Data_41,
    main.customerTier User_Data_42,
    main.DC User_Data_43,
    main.subRegion || '_' || main.DC || '_' || main.channel User_Data_44,
    main.mtsMtoFlag User_Data_45,
    '' User_Data_46,
    '' User_Data_47,
    current_date User_data_48
  from main_5 main
""")
LOM_AU_L1.createOrReplaceTempView('LOM_AU_L1')

# COMMAND ----------

LOM_AU_L1_MPX_substituted = spark.sql("""
with cte1(
  select main.*,mpx.SuccessorCode from 
  LOM_AU_DO_ARCHIVE_1 main -- archived records
  join main_MPX_L0 mpx on main.Forecast_1_ID = mpx.predecessorcode and left(substr(main.forecast_2_id, instr(main.forecast_2_id, '_')+1), instr(substr(main.forecast_2_id, instr(main.forecast_2_id, '_')+1), '_')-1) = mpx.dc
)
select '' _SOURCE,
    null _PDHPRODUCTFLAG,
    * except (SuccessorCode)    
from cte1
union all
select 
    '' _SOURCE,
    case when pdhProduct.productCode is null then '#Error1' end _PDHPRODUCTFLAG,
    main.Pyramid_Level,
    main.successorcode Forecast_1_ID,
    main.Forecast_2_ID,
    main.Forecast_3_ID,
    main.Record_Type,
    main.User_Data_34,
    main.User_Data_35,
    pdhProduct.gtinInner User_Data_38,
    pdhProduct.gtinouter User_Data_39,
    main.User_Data_41,
    main.User_Data_42,
    main.User_Data_43,
    main.User_Data_44,
    main.User_Data_45,
    main.User_Data_46,
    main.User_Data_47,
    current_date User_data_48
  from cte1 main     
    left join pdhProduct on main.successorcode = pdhProduct.productcode    
""")

LOM_AU_L1_MPX_substituted.createOrReplaceTempView('LOM_AU_L1_MPX_substituted')
LOM_AU_L1_MPX_substituted.display()

# COMMAND ----------

LOM_AU_L1_MPX = spark.sql("""
select subs.* 
from 
  LOM_AU_L1_MPX_substituted subs
  left join logftp.logility_do_level_1_for_mpx mpx on 
    subs.forecast_1_id=mpx.Lvl1Fcst and subs.forecast_2_id=mpx.Lvl2Fcst and subs.forecast_3_id=mpx.Lvl3Fcst
where mpx.Lvl1Fcst is null
""")
LOM_AU_L1_MPX.createOrReplaceTempView('LOM_AU_L1_MPX')

# COMMAND ----------

# DBTITLE 1,Product LOM AU - New Items
LOM_AU_L1_MPX_new_items = spark.sql("""
select distinct
    'SPT' _source,
    case when pdhProduct.productCode is null then '#Error1' end _PDHPRODUCTFLAG,
    '1' Pyramid_Level,
    mpx.successorcode Forecast_1_ID,
    mpx.subRegion || '_' || mpx.DC || '_' || mpx.DSWH Forecast_2_ID,
    mr.customerGbu || '_' || 'Other' Forecast_3_ID,
    'AU' Record_Type,
    'Other' as User_Data_34, 
    mr.customerGbu User_Data_35,
    pdhProduct.gtinInner User_Data_38,
    pdhProduct.gtinouter User_Data_39,
    nvl(inv.region,concat('#Error49','-',mpx.DC)) User_Data_41,
    'No Tier' as  User_Data_42, 
    mpx.DC User_Data_43,
    mpx.subRegion || '_' || mpx.DC || '_' || mpx.DSWH User_Data_44,
    'MTS' as User_Data_45, 
    '' User_Data_46,
    '' User_Data_47,
    current_date User_data_48
  from 
    main_MPX_new_items mpx   
    left join (select item,gbu,sbu,case when gbu in ('SINGLE USE','INDUSTRIAL') then 'I' when gbu = 'MEDICAL' then 'M' end as customerGbu,
               case when gbu in ('SINGLE USE','INDUSTRIAL') then 'I_*' when gbu = 'MEDICAL' then 'M_*' end as customerForecastgroup              
             from pdh.master_records) mr on mr.item = mpx.successorcode
    left join pdhProduct on mpx.successorcode = pdhProduct.productcode
    left join (select distinct subregion, region, forecastarea from s_core.territory_agg) con on mpx.subregion = con.subregion
    left join (select customerforecastgroup,forecastplannerid,pdhgbu,region,sbu from smartsheets.e2eforecastplanner where level = 'L1-2' and not _deleted) plannerCodeL1 
      on con.region = plannercodeL1.region      
    and mr.sbu = plannercodeL1.SBU
    and mr.gbu = plannercodeL1.pdhgbu
    and mr.CustomerForecastGroup   = plannercodeL1.CustomerForecastGroup
    left join s_core.organization_agg inv
      on inv.organizationCode = Replace(DC,LEFT(DC,2),'')
""")
LOM_AU_L1_MPX_new_items.createOrReplaceTempView('LOM_AU_L1_MPX_new_items')
# LOM_AU_L1_MPX_new_items.display()

# COMMAND ----------

# DBTITLE 1,7.1 Get base selection for LOM HH Shipments (Current) 
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
    --when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion in ('BR') then 'BR'
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion not in ('MX') and oh._source = 'EBS' then 'OLAC' 
    else  nvl(soldtoTerritory.subRegion, '#Error8-' || soldtoTerritory.territoryCode) 
  end subRegion, 
  coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode)  DC ,
  orderType.dropShipmentFlag Channel,
  left(case when nvl(acc.gbu,'X') = 'NV&AC' then 'I' else  left(nvl(acc.gbu, acc_org.salesoffice), 1) end || '_' || coalesce(acc_org.forecastGroup, acc.forecastGroup, 'Other'), 40)  Forecast_3_ID,
  soldtoTerritory.territoryCode,
  soldtoTerritory.subRegionGis,
  case when nvl(soldto.siteCategory,'X') = 'LA' then 'LAC' else nvl(soldtoTerritory.Region, '#Error14') end Region,
  case 
    when inv.region = 'EMEA' and left(pr.gbu, 1) = 'M' then 'EURAF'
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.forecastArea not in ('MX','CO') and oh._source = 'EBS' then 'OLAC' 
    else soldtoTerritory.forecastArea 
  end forecastArea,
  ol._SOURCE,
  oh.orderNumber,
  ol.linenumber,
  (sl.quantityShipped  * sl.ansStdUomConv) quantityShipped,
  (sl.shippingAmount / nvl(sl.exchangeRateUsd,1) ) shippingAmountUsd,
  year(sl.actualShipDate) yearShipped,
  month(sl.actualShipDate) monthShipped,
  sl.actualShipDate,
  year(ol.requestDeliveryBy) yearRequestedBy,
  month(ol.requestDeliveryBy) monthRequestedBy,
  ol.requestDeliveryBy,
  sl.pricePerUnit / nvl(oh.exchangeRateUsd,1) pricePerUnitUsd 
  ,pr_org.productStatus regionalProductStatus 
  ,nvl(acc.customerTier, 'No Tier') customerTier,
  case 
    when pr_org.mtsMtoFlag is null then 'MTS' 
    when pr_org.mtsMtoFlag not in ('MTS', 'MTO') then '#Error21' 
    else pr_org.mtsMtoFlag
  end as mtsMtoFlag  ,
  nvl(inv.region, concat('#Error49','_',coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode))) orgRegion,
  nvl(left(acc.name, 40), 'CUST01') accountName
from 
  s_supplychain.sales_order_lines_agg ol 
   join s_supplychain.sales_shipping_lines_agg sl on ol._ID = sl.salesOrderDetail_ID 
   join s_supplychain.sales_order_headers_agg oh on ol.salesorder_id = oh._ID 
   join s_core.product_agg pr on ol.item_ID = pr._ID 
   join s_core.account_agg acc on oh.customer_ID = acc._ID 
   left join s_core.customer_location_agg soldto on oh.soldtoAddress_ID = soldto._ID 
   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID 
   join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
   left join s_core.transaction_type_agg orderType on oh.orderType_ID = orderType._id    
   left join sub_region_override on nvl(soldto.partySiteNumber, soldto.addressid) = sub_region_override.partySiteNumber
   left join s_core.territory_agg soldtoTerritory on nvl(sub_region_override.country_ID, soldto.territory_ID) = soldtoTerritory._id   
   left join s_core.account_organization_agg acc_org on oh.customerOrganization_ID = acc_org._id 
   left join regionalproductstatus  pr_org on pr.ProductCode = pr_org.e2eProductCode
                                    and ol.inventoryWarehouse_ID = pr_org.organization_ID
   
where
  not ol._deleted
  and not oh._deleted
  and not acc._deleted
  and not pr._deleted
  and not sl._deleted
  and oh._source  in  (select sourceSystem from source_systems) 
  and not orderType._deleted
  and inv.isActive 
  and (year(sl.actualShipDate) * 100 + month(sl.actualShipDate) =   (select runPeriod from run_period)
      or year(ol.requestDeliveryBy) * 100 + month(ol.requestDeliveryBy) =   (select runPeriod from run_period))
  and coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) not in ( 'DC325', '821', '802')
  and (nvl(orderType.e2eFlag, true)
         or ol.e2edistributionCentre = 'DC827'
         or CONCAT(orderType.transactionId , '-' , nvl(upper(pr.packagingCode),'')) in (select CONCAT(KEY_VALUE,'-',CVALUE) 
                                                                                 from smartsheets.edm_control_table 
                                                                                 WHERE table_id = 'ADDITIONAL_ORDER_TYPE_PACK_TYPE')
      )
  and upper(pr.itemType) not in  ('SERVICE')
  and (ol.quantityOrdered <> 0
    --or cancelReason like '%COVID-19%' 
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
  Forecast_3_ID,
  territoryCode,
  subRegionGis,
  Region,
  forecastArea,
  _SOURCE,
  linenumber,
  orderNumber,
  sum(quantityShipped) quantityShipped,
  sum(shippingAmountUsd) shippingAmountUsd,
  yearShipped,
  monthShipped,
  actualShipDate,
  yearRequestedBy,
  monthRequestedBy,
  requestDeliveryBy,
  pricePerUnitUsd,
  regionalProductStatus,
  customerTier,
  mtsMtoFlag,
  orgRegion,
  accountName
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
  Forecast_3_ID,
  territoryCode,
  subRegionGis,
  Region,
  forecastArea,
  _SOURCE,
  linenumber,
  orderNumber,
  yearShipped,
  monthShipped,
  actualShipDate,
  yearRequestedBy,
  monthRequestedBy,
  requestDeliveryBy,
  pricePerUnitUsd,
  regionalProductStatus,
  customerTier,
  mtsMtoFlag ,
  orgRegion,
  accountName
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
  main.linenumber,
  main.orderNumber,
  main.quantityShipped,
  main.shippingAmountUsd,
  main.yearShipped,
  main.monthShipped,
  actualShipDate,
  yearRequestedBy,
  monthRequestedBy,
  requestDeliveryBy,
  pricePerUnitUsd,
  main.regionalProductStatus,
  main.customerTier,
  main.mtsMtoFlag  ,
  main.orgRegion,
  main.accountName
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
 -- case
 --   when main_2.subRegion = 'UK-E' and main_2.DC = 'DCNLT1'
 --       then main_2.DC
 --   when main_2.subRegion = 'UK-E' and main_2.DC <> 'DCNLT1'  and _source = 'SAP' then main_2.DC
 --   when main_2.subRegion <> 'UK-E' and main_2.DC = 'DCNLT1'  and _source = 'SAP' then 'DCANV1'
 --   else main_2.DC
 -- end DC,
  main_2.DC,
  main_2.channel,
  main_2.region,
  main_2.subRegionGis,
  main_2.customerGbu,
  main_2.forecastGroup,
  main_2.Forecast_Planner,
  main_2.forecastArea,
  main_2.linenumber,
  main_2.orderNumber, 
  main_2.quantityShipped,
  main_2.shippingAmountUsd,
  main_2.yearShipped,
  main_2.monthShipped,  
  case when gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID is null then false else true end gtcReplacementFlag,
  gtc_replacement.ITEM_STATUS,
  main_2.actualShipDate,
  main_2.yearRequestedBy,
  main_2.monthRequestedBy,
  main_2.requestDeliveryBy,
  main_2.pricePerUnitUsd,
  main_2.regionalProductStatus,
  main_2.customerTier,
  main_2.mtsMtoFlag  ,
  main_2.orgRegion,
  main_2.accountName
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
  --   null as orderNumber, 
  main.ordernumber as orderNumber, 
  main.linenumber,
  main.quantityShipped,
  main.shippingAmountUsd,
  main.yearShipped,
  main.monthShipped,
  main.actualShipDate,
  main.yearRequestedBy,
  main.monthRequestedBy,
  main.requestDeliveryBy,
  main.pricePerUnitUsd,
  case when left(pdhProduct.productStyle,1) = '0' then substr(pdhProduct.productStyle, 2, 50) else pdhProduct.productStyle end productStylePdh,
  nvl(left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36), '#Error12') name,
  Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
  Cast(pdhProduct.caseGrossWeight as numeric(9,4)) Unit_weight,
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
  main.gtcReplacementFlag,
  nvl(pdhProduct.gtinInner, 'No gtinInner') gtinInner,
  nvl(pdhProduct.gtinouter, 'No gtinOuter') gtinouter,
  main.customerTier,
  main.mtsMtoFlag,
  pdhProduct.packagingCode,
  pdhProduct.baseProduct  ,
  main.orgRegion,
  main.accountName
from
  main_20_2a main
  left join (select * from s_core.product_agg where _source = 'PDH' and not _deleted) pdhProduct on main.productCode = pdhProduct.productCode 
  join locationmaster lm on main.dc = lm.globallocationcode
where
  1=1
--   and nvl(pdhProduct.itemClass,'X') not in ('Ancillary Products')
""")
  
main_20_3.createOrReplaceTempView('main_20_3')

# COMMAND ----------

main_20_4_ss = spark.sql("""
select distinct
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
  nvl(qvunitpricePdh.AVG_UNIT_COST_PRICE,0.00001) Unit_cost,
  main.Unit_cube,
  main.Unit_Weight,
  'N' Product_group_conversion_option,
  1 Product_group_conversion_factor,
  plannerCodeL1.forecastplannerid Forecast_Planner,
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
  current_date creationDate,  
  main.orgRegion User_Data_41
from
  main_20_3 main 
  left join sap.qvunitprice qvunitpricePdh on  main.productStylePdh  = qvunitpricePdh.StyleCode
    and main.subRegionGis = qvunitpricePdh.subRegion
  left join (select customerforecastgroup,forecastplannerid,pdhgbu,region,sbu from smartsheets.e2eforecastplanner where level = 'L1-2' and not _deleted) plannerCodeL1 
      on main.region = plannercodeL1.region      
    and main.productSbu = plannercodeL1.SBU
    and main.productGbu = plannercodeL1.pdhgbu
    and case
           when main.customerGbu = 'M' then 'M_*'
           when upper(main.forecastGroup) in ('I_FHP GROUP') then 'I_FHP GROUP'
           when upper(main.forecastGroup) like 'I_GRAINGER%' then 'I_GRAINGER*' 
           when main.customerGbu in ('I', 'N')
              and main.Region = 'NA'
              and main.productGbu not in ('MEDICAL')
              and upper(main.forecastGroup) not like 'I_GRAINGER%'  then 'I_* & <> I_GRAINGER*'
           when upper(main.customerGbu) in ('I', 'N', 'O', 'U') then 'I_*'
        end   = plannercodeL1.CustomerForecastGroup
WHERE 
   year(actualShipDate) * 100 + month(actualShipDate) =   (select runPeriod from run_period)
""")
  
main_20_4_ss.createOrReplaceTempView('main_20_4_ss')

# COMMAND ----------

LOM_AU_L1_ss = spark.sql("""
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
    current_date User_data_48
  from main_20_3 main
  WHERE 
   year(actualShipDate) * 100 + month(actualShipDate) =   (select runPeriod from run_period)
""")
LOM_AU_L1_ss.createOrReplaceTempView('LOM_AU_L1_ss')

# COMMAND ----------

# DBTITLE 1,6.2. LOM FF - Current
DO_LOM_FF_L1 = spark.sql("""
WITH DO_LOM_FF_1 as 
(select
    '1' Pyramid_Level,
    main.productCode Forecast_1_ID,
    main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
    left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
    'FF' Record_type,
    yearRequestDeliveryBy Year,
    monthRequestDeliveryBy Period,
    quantityOrdered Future_order
from main_5 main
)
select 
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_type,
    Year,
    Period,
    case when sum(Future_order) < 0 then sum(-Future_order) else sum(future_order) end Future_order,
    case when sum(Future_order) < 0 then '-' else '' end Future_order_sign
from DO_LOM_FF_1
group by
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_type,
    Year,
    Period
""")
DO_LOM_FF_L1.createOrReplaceTempView('DO_LOM_FF_L1')
# DO_LOM_FF_L1.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_FF_L1', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
DO_LOM_FF_L1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,20.4.Build interface layout for Shipments
main_HH2 = spark.sql("""
select 
    main._SOURCE,
    main.pdhProductFlag,
    '1' Pyramid_Level,
    left(main.productCode,40) Forecast_1_ID,
    main.subRegion || '_' || main.DC || '_' || main.channel Forecast_2_ID,
    left(main.customerGbu || '_' || main.forecastGroup, 40) Forecast_3_ID,
    'HH' Record_Type,
    main.yearShipped Year,
    main.monthShipped Period,
    0 Demand,
    '' Demand_sign,
    sum(quantityShipped)  Historical_ADS1_data,
    '' Historical_ADS1_sign,
    0 Value_history_in_currency,
    '' Value_history_sign,
    sum(shippingAmountUsd) Historical_ADS2_data,
    '' Historical_ADS2_sign
from  main_20_3 main
WHERE 
   year(actualShipDate) * 100 + month(actualShipDate) =   (select runPeriod from run_period)
group by
    main._SOURCE,
    main.pdhProductFlag,
    left(main.productCode,40),
    main.subRegion || '_' || main.DC || '_' || main.channel,
    left(main.customerGbu || '_' || main.forecastGroup, 40),
    main.yearShipped,
    main.monthShipped    
""")
  
main_HH2.createOrReplaceTempView('main_HH2')
# main_HH2.display()

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
where yearRequestDeliveryBy * 100 + monthRequestDeliveryBy =  (select runPeriod from run_period) --year(current_date) * 100 + month(current_date)

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
file_name = get_file_name('TMP_LOM_HH_DO_L1', 'dlt')
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
  --coalesce(acc_org.forecastGroup, acc.forecastGroup, 'Not Assigned') forecastGroup,
  case when acc_org._id = 0 then 
     nvl(acc.forecastGroup, 'Other')
  else
     nvl(acc_org.forecastGroup, 'Other')
  end forecastGroup,
  soldtoTerritory.region Forecast_Planner,
 case 
    when sub_region_override.subRegion is not null then sub_region_override.subRegion 
    --when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion in ('BR') then 'BR'
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion not in ('MX') and oh._source = 'EBS' then 'OLAC' 
    else  nvl(soldtoTerritory.subRegion, '#Error8-' || soldtoTerritory.territoryCode) 
  end subRegion,  
  coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode)  DC ,
  orderType.dropShipmentFlag Channel,
  soldtoTerritory.territoryCode,
  soldtoTerritory.subRegionGis,
  case when nvl(soldto.siteCategory,'X') = 'LA' then 'LAC' else nvl(soldtoTerritory.Region, '#Error14') end Region,
  case 
    when inv.region = 'EMEA' and left(pr.gbu, 1) = 'M' then 'EURAF'
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.forecastArea not in ('MX','CO') and oh._source = 'EBS' then 'OLAC' 
    else soldtoTerritory.forecastArea 
  end forecastArea,
  ol._SOURCE,
  oh.orderNumber,
  (il.quantityInvoiced  * nvl(il.ansStdUomConv,1)) quantityInvoiced,
  year(ih.dateInvoiced) yearInvoiced,
  month(ih.dateInvoiced) monthInvoiced,
  pr_org.productStatus regionalProductStatus,
 nvl(acc.customerTier, 'No Tier') customerTier,
  case 
    when pr_org.mtsMtoFlag is null then 'MTS' 
    when pr_org.mtsMtoFlag not in ('MTS', 'MTO') then '#Error21' 
    else pr_org.mtsMtoFlag
  end as mtsMtoFlag,
 nvl(inv.region, concat('#Error49','-',coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode))) orgRegion
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
  left join sub_region_override on nvl(soldto.partySiteNumber, soldto.addressid) = sub_region_override.partySiteNumber
  left join s_core.territory_agg soldtoTerritory on nvl(sub_region_override.country_ID, soldto.territory_ID) = soldtoTerritory._id  
  left join s_core.account_organization_agg acc_org on oh.customerOrganization_ID = acc_org._id
  --left join s_core.product_org_agg pr_org on il.item_ID = pr_org.item_ID 
    --    and il.inventoryWarehouse_ID = pr_org.organization_ID
   left join regionalproductstatus  pr_org on pr.ProductCode = pr_org.e2eProductCode
                                    and il.inventoryWarehouse_ID = pr_org.organization_ID
 
where
  not il._deleted
  and not ih._deleted
  and inv.isActive
  and ih._source  in  (select sourceSystem from source_systems) 
  and coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) not in ('DC325', '000', '250','325', '355', '811', '821', '802', '824', '825', '832', '834')
  and year(ih.dateInvoiced) * 100 + month(ih.dateInvoiced) =  (select runPeriod from run_period)
 and (nvl(orderType.e2eFlag, true)
         or ol.e2edistributionCentre = 'DC827'
         or CONCAT(orderType.transactionId , '-' , nvl(upper(pr.packagingCode),'')) in (select CONCAT(KEY_VALUE,'-',CVALUE) 
                                                                                 from smartsheets.edm_control_table 
                                                                                 WHERE table_id = 'ADDITIONAL_ORDER_TYPE_PACK_TYPE')
     )
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
  regionalProductStatus,
  customerTier,
  mtsMtoFlag,
  orgRegion
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
  regionalProductStatus,
  customerTier,
  mtsMtoFlag,
  orgRegion
""")
  
main_40_1_inv.createOrReplaceTempView('main_40_1_inv')

# COMMAND ----------

# DBTITLE 1,40.2.INV Find substitution if available for LOM XX Invoices
main_40_2a_inv = spark.sql("""
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
  main.regionalProductStatus,
  main.customerTier,
  main.mtsMtoFlag ,
  main.orgRegion
from 
   main_40_1_inv main
   left join HistoricalProductSubstitution on main.key =   HistoricalProductSubstitution.key
where nvl(HistoricalProductSubstitution.final_successor, 'Include') not in ('dis')
""")
  
main_40_2a_inv.createOrReplaceTempView('main_40_2a_inv')

# COMMAND ----------

# DBTITLE 1,40.2.. replace GTC product code with PDH product Code
main_40_2_inv = spark.sql("""
select 
  main.accountnumber,
  nvl(gtc_replacement.item,main.productCode) productCode, 
  main.productStatus,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
  main.subRegion, 
 -- case
 --   when main.subRegion = 'UK-E' and main.DC = 'DCNLT1'
 --       then main.DC
 --   when main.subRegion = 'UK-E' and main.DC <> 'DCNLT1'  and _source = 'SAP' then main.DC
 --   when main.subRegion <> 'UK-E' and main.DC = 'DCNLT1'  and _source = 'SAP' then 'DCANV1'
 --   else main.DC
 -- end DC,
  main.DC,
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
  main.regionalProductStatus,
  main.customerTier,
  main.mtsMtoFlag ,
  main.orgRegion
from
  main_40_2a_inv main
  left join gtc_replacement on main.productCode = gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID
where 
  nvl(gtc_replacement.ITEM_STATUS, 'Active') not in ('Inactive')
""")
main_40_2_inv.createOrReplaceTempView('main_40_2_inv')

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
  Cast(pdhProduct.caseGrossWeight as numeric(9,4)) Unit_weight,
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
  main.gtcReplacementFlag,  
  nvl(pdhProduct.gtinInner, 'No gtinInner') gtinInner,
  nvl(pdhProduct.gtinouter, 'No gtinOuter') gtinouter,
  main.customerTier,
  main.mtsMtoFlag,
  pdhProduct.packagingCode,
  pdhProduct.baseProduct ,
  main.orgRegion
from 
   main_40_2_inv main
   left join (select * from s_core.product_agg where _source = 'PDH' and not _deleted) pdhProduct on main.productCode = pdhProduct.productCode 
where 
  1=1
--   and nvl(pdhProduct.itemClass,'X') not in ('Ancillary Products')
 """)
  
main_40_3_inv.createOrReplaceTempView('main_40_3_inv')

# COMMAND ----------

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
  nvl(qvunitpricePdh.AVG_UNIT_COST_PRICE,0.00001) Unit_cost,
  main.Unit_cube,
  main.Unit_Weight,
  'N' Product_group_conversion_option,
  1 Product_group_conversion_factor,
  plannerCodeL1.forecastplannerid Forecast_Planner,
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
  current_date creationDate,
  main.orgRegion User_Data_41
from
  main_40_3_inv main 
  left join sap.qvunitprice qvunitpricePdh on  main.productStylePdh  = qvunitpricePdh.StyleCode
    and main.subRegionGis = qvunitpricePdh.subRegion
  left join (select customerforecastgroup,forecastplannerid,pdhgbu,region,sbu from smartsheets.e2eforecastplanner where level = 'L1-2' and not _deleted) plannerCodeL1 
      on main.region = plannercodeL1.region      
    and main.productSbu = plannercodeL1.SBU
    and main.productGbu = plannercodeL1.pdhgbu
    and case
           when main.customerGbu = 'M' then 'M_*'
           when upper(main.forecastGroup) in ('I_FHP GROUP') then 'I_FHP GROUP'
           when upper(main.forecastGroup) like 'I_GRAINGER%' then 'I_GRAINGER*' 
           when main.customerGbu in ('I', 'N')
              and main.Region = 'NA'
              and main.productGbu not in ('MEDICAL')
              and upper(main.forecastGroup) not like 'I_GRAINGER%'  then 'I_* & <> I_GRAINGER*'
           when upper(main.customerGbu) in ('I', 'N', 'O', 'U') then 'I_*'
        end    = plannercodeL1.CustomerForecastGroup
""")
  
main_40_4_inv.createOrReplaceTempView('main_40_4_inv')

# COMMAND ----------

LOM_AU_L1_inv = spark.sql("""
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
    current_date User_data_48
  from main_40_3_inv main
""")
LOM_AU_L1_inv.createOrReplaceTempView('LOM_AU_L1_inv')

# COMMAND ----------

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


LOM_AU_L1_ss_missing = spark.sql("""
select main20.* from LOM_AU_L1_ss main20
left join LOM_AU_L1 main5 on main5.Forecast_1_ID = main20.Forecast_1_ID and main5.Forecast_2_ID = main20.Forecast_2_ID and main5.Forecast_3_ID = main20.Forecast_3_ID
where main5.Forecast_1_ID  is null
""")

LOM_AU_L1_inv_missing = spark.sql("""
select main40.* from LOM_AU_L1_inv main40
left join LOM_AU_L1 main5 on main5.Forecast_1_ID = main40.Forecast_1_ID and main5.Forecast_2_ID = main40.Forecast_2_ID and main5.Forecast_3_ID = main40.Forecast_3_ID
where main5.Forecast_1_ID  is null
""")

main20_ss_missing.createOrReplaceTempView('main20_ss_missing')
main40_inv_missing.createOrReplaceTempView('main40_inv_missing')
LOM_AU_L1_ss_missing.createOrReplaceTempView('LOM_AU_L1_ss_missing')
LOM_AU_L1_inv_missing.createOrReplaceTempView('LOM_AU_L1_inv_missing')

# COMMAND ----------

LOM_AA_DO_L1_ALL = (
  LOM_AA_DO_L1.select('*')
  .unionByName(LOM_AA_DO_CHANGES.select('*'), allowMissingColumns=True)
  .unionByName(LOM_AA_DO_L1_MPX.select('*'), allowMissingColumns=True) 
  .unionByName(LOM_AA_DO_L1_MPX_new_items.select('*'), allowMissingColumns=True) 
  .unionByName(main20_ss_missing.select('*'), allowMissingColumns=True) 
  .unionByName(main40_inv_missing.select('*'), allowMissingColumns=True)  
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
  group by
     Forecast_1_ID,
     Forecast_2_ID,
     Forecast_3_ID
""")
LOM_AA_DO_L1_ALL_UNIQUE.createOrReplaceTempView('LOM_AA_DO_L1_ALL_UNIQUE')

# COMMAND ----------

# DBTITLE 1,Product LOM AA from Orders, Shipments and Orders
# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_AA_DO_L1', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AA_DO_L1_ALL_UNIQUE.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

LOM_AU_DO_CHANGES_3 = spark.sql("""
select
  '' _source,
  case when pdhProduct.productCode is null then '#Error1' end  _PDHPRODUCTFLAG,
  '1' Pyramid_Level,
  l1.forecast_1_id,
  l1.forecast_2_id,
  l1.forecast_3_id,
  'AU' Record_Type,
  trim(substr(l1.forecast_3_id, 3)) User_Data_34,
  left(l1.forecast_3_id, 1) User_Data_35,
  nvl(pdhProduct.gtinInner, 'No gtinInner') User_Data_38,
  nvl(pdhProduct.gtinouter, 'No gtinOuter') User_Data_39,
  nvl(regionalproductstatus.orgRegion, 
      concat('#Error49',
             '_',
             left(substr(l1.forecast_2_id, instr(l1.forecast_2_id, '_')+1), instr(substr(l1.forecast_2_id, instr(l1.forecast_2_id, '_')+1), '_')-1)
            )
     ) AS User_Data_41,
  'No Tier' User_Data_42,
  left(substr(l1.forecast_2_id, instr(l1.forecast_2_id, '_')+1), instr(substr(l1.forecast_2_id, instr(l1.forecast_2_id, '_')+1), '_')-1) User_Data_43,
  l1.forecast_2_id User_Data_44,
  nvl(regionalproductstatus.mtsMtoFlag, 'MTS') User_Data_45,
  '' User_Data_46,
  '' User_Data_47,
  current_date User_data_48
from 
  LOM_AA_DO_ARCHIVE_Keys l1
  left join LOM_AU_DO_ARCHIVE_1 l2 on l1.forecast_1_id = l2.forecast_1_id
    and l1.forecast_2_id = l2.forecast_2_id
    and l1.forecast_3_id = l2.forecast_3_id
  left join pdhProduct on l1.forecast_1_id = pdhProduct.productCode
  left join regionalproductstatus on l1.forecast_1_id = regionalproductstatus.e2eProductCode
    and left(substr(l1.forecast_2_id, instr(l1.forecast_2_id, '_')+1), instr(substr(l1.forecast_2_id, instr(l1.forecast_2_id, '_')+1), '_')-1) = regionalproductstatus.organizationCode
-- where 1=1
--   and l2.User_Data_45 is null
 
""")
LOM_AU_DO_CHANGES_3.createOrReplaceTempView('LOM_AU_DO_CHANGES_3')

# COMMAND ----------

# this cell is moved after cell 63 to include LOM_AU_L1_SS and LOM_AU_L1_INV
LOM_AU_L1_ALL = (
  LOM_AU_L1.select('*')
  .union(LOM_AU_L1_MPX.select('*'))
  .union(LOM_AU_L1_MPX_new_items.select('*'))
  .union(LOM_AU_L1_ss_missing.select('*'))
  .union(LOM_AU_L1_inv_missing.select('*'))
  .union(LOM_AU_DO_CHANGES_3.select('*'))
)
LOM_AU_L1_ALL.createOrReplaceTempView('LOM_AU_L1_ALL')

# COMMAND ----------

LOM_AU_L1_ALL_UNIQUE=spark.sql("""
select 
  Pyramid_Level,
  Forecast_1_ID, 
  Forecast_2_ID, 
  Forecast_3_ID,
  Record_Type,
  max(User_Data_34) User_Data_34,
  max(User_Data_35) User_Data_35,
  max(User_Data_38) User_Data_38,
  max(User_Data_39) User_Data_39,
  max(User_Data_41) User_Data_41,
  max(User_Data_42) User_Data_42,
  max(User_Data_43) User_Data_43,
  max(User_Data_44) User_Data_44,
  max(User_Data_45) User_Data_45,
  max(User_Data_46) User_Data_46,
  max(User_Data_47) User_Data_47,
  max(User_Data_48) User_Data_48
from 
  LOM_AU_L1_ALL
group by
  Pyramid_Level,
  Forecast_1_ID, 
  Forecast_2_ID, 
  Forecast_3_ID,
  Record_Type
""")
LOM_AU_L1_ALL_UNIQUE.createOrReplaceTempView('LOM_AU_L1_ALL_UNIQUE')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_AU_DO_L1', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AU_L1_ALL_UNIQUE.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

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
  left(main.customerGbu || '_' || main.forecastGroup,40) Forecast_3_ID,
  'XX' as Record_type,
  --cast(yearRequestedBy as decimal(4,0)) as Year,
  --cast(monthRequestedBy as decimal(2,0)) as Period,
  yearRequestedBy as Year,
  monthRequestedBy as Period,
  nvl(main.quantityShipped,0) as User_Array_1,
  '' as User_Array_1_sign,
  0 as User_Array_2,
  '' as User_Array_2_sign,
  0 as User_Array_8,
  '' as User_Array_8_sign
From 
  --main_40_3_ss main
  main_20_3 main
WHERE 
   year(actualShipDate) * 100 + month(actualShipDate) =   (select runPeriod from run_period)
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
--  main_40_3_ol main
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
file_name = get_file_name('TMP_LOM_XX_DO_L1', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_XX_L1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

Level_1_for_MPX=spark.sql("""
select
  Lvl1Fcst Lvl1Fcst1,
  Lvl2Fcst Lvl1Fcst2,
  Lvl3Fcst Lvl1Fcst3,
  field_10 field10,
  field_12 field12,
  field_13 field13,
  substr(substr(Lvl2Fcst, instr(Lvl2Fcst,'_' )+1),1, instr(substr(Lvl2Fcst, instr(Lvl2Fcst,'_' )+1),'_' )-1) DC,
  left(field_13, instr(field_13,'_')-1) subregion,
  productPdh.productStyle,
  productPdh.sizeDescription,
  field_09 field09
from
  logftp.logility_do_level_1_for_mpx
  join (select productCode, productStyle, sizeDescription from s_core.product_agg where _source = 'PDH' and not _deleted) productPdh on logility_do_level_1_for_mpx.Lvl1Fcst = productPdh.productCode
where
  Lvl3Fcst not like 'TEST%'
  and Lvl3Fcst not like '%#Error%'
  and trim(field_12) <> ''
""")
Level_1_for_MPX.createOrReplaceTempView('Level_1_for_MPX')
# Level_1_for_MPX.display()


# COMMAND ----------

# DBTITLE 1,40.1 LOM MPX L1 - New version
main_MPX_L1 = spark.sql("""
select distinct
    'MPX'  Record_type,
    'D' Face_From,
    '1' Pyramid_level_From,
    'D' Group_Face_From,
    '2' Group_pyramid_level_From,
    
    Lvl1Fcst1 Forecast_1_ID_Group_From,
    Field13 Forecast_2_ID_Group_From,
    Lvl1Fcst3 Forecast_3_ID_Group_From,
    
    '1' Item_pyramid_level_From,
    
    Lvl1Fcst1 Forecast_1_ID_Item_From,
    Lvl1Fcst2 Forecast_2_ID_Item_From,
    Lvl1Fcst3 Forecast_3_ID_Item_From,
    
    'D' Face_To,
    '1' Pyramid_level_To,
    'D' Group_Face_To,
    '2' Group_pyramid_level_To,
    
    successorcode Forecast_1_ID_Group_To,
    Field13 Forecast_2_ID_Group_To,
    Lvl1Fcst3 Forecast_3_ID_Group_To,
    
    '1' Item_pyramid_level_To,
    successorcode Forecast_1_ID_Item_To,
    Lvl1Fcst2 Forecast_2_ID_Item_To,
    Lvl1Fcst3 Forecast_3_ID_Item_To,
    cast(00000000000 as decimal(11,5)) Factor_From_Records,
    'Y' Factor_Adjusted_Demand_on_From_Record,
    'N' Factor_Actual_Demand_on_From_Record,
    'N' Zero_Actual_Demand_on_From_Record,
    'Y' Create_Supersedure_Key_on_From_Record,
    'Y' Inhibit_From_Forecasts_on_From_Record,
    'N' Delete_the_From_Record,
    cast(00000100000 as decimal(11,5)) Factor_To_Record,
    'Y' Factor_Adjusted_Demand_on_To_Record,
    'N' Factor_Actual_Demand_on_To_Record,
    'Y' Zero_Actual_Demand_on_To_Record,
    'N' Do_Not_Move_Adjusted_Demand_Indicator,
    'N' Do_Not_Move_Future_Forecast_Indicator,
    'N' Effective_Date_Option,
    ' ' Effective_Year,
    ' ' Effective_Period,
    'N' Adjust_From_Forecasts,
    'N' Move_History_Option,
    ' ' History_Begin_Year,
    ' ' History_Begin_Period,
    ' ' History_End_Year,
    ' ' History_End_Period,
    'Y' Create_MPX_Audit_Report,
    ' ' Transaction_File_Version,
    ' ' Factor_From_Promotional_Lift,
    ' ' Factor_To_Promotion_Lift,
    'Y' Copy_Remarks
from 
    Level_1_for_MPX
    join main_MPX_L0 on Lvl1Fcst1 = main_MPX_L0.PredecessorCode
      and Level_1_for_MPX.DC = main_MPX_L0.DC
""")
  
main_MPX_L1.createOrReplaceTempView('main_MPX_L1')
# main_MPX_L1.count()
# main_MPX_L1.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_MPX_L1', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
main_MPX_L1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,40.1 LOM MPX L2 - new version
main_MPX_L2 = spark.sql("""
select distinct
    'MPX'  Record_type,
    'D' Face_From,
    '2' Pyramid_level_From,
    'D' Group_Face_From,
    '3' Group_pyramid_level_From,
    
    Lvl1Fcst1 Forecast_1_ID_Group_From,
    field10 Forecast_2_ID_Group_From,
    field12 Forecast_3_ID_Group_From,
    
    '2' Item_pyramid_level_From,
    
    Lvl1Fcst1 Forecast_1_ID_Item_From,
    Field13 Forecast_2_ID_Item_From,
    Lvl1Fcst3 Forecast_3_ID_Item_From,
    
    'D' Face_To,
    '2' Pyramid_level_To,
    'D' Group_Face_To,
    '3' Group_pyramid_level_To,
    
    successorcode Forecast_1_ID_Group_To,
    field10 Forecast_2_ID_Group_To,
    field12 Forecast_3_ID_Group_To,
    
    '2' Item_pyramid_level_To,
    
    successorcode Forecast_1_ID_Item_To,
    Field13 Forecast_2_ID_Item_To,
    Lvl1Fcst3 Forecast_3_ID_Item_To,
    
    cast(00000000000 as decimal(11,5)) Factor_From_Records,
    'Y' Factor_Adjusted_Demand_on_From_Record,
    'N' Factor_Actual_Demand_on_From_Record,
    'N' Zero_Actual_Demand_on_From_Record,
    'Y' Create_Supersedure_Key_on_From_Record,
    'Y' Inhibit_From_Forecasts_on_From_Record,
    'N' Delete_the_From_Record,
    cast(00000100000 as decimal(11,5)) Factor_To_Record,
    'Y' Factor_Adjusted_Demand_on_To_Record,
    'N' Factor_Actual_Demand_on_To_Record,
    'Y' Zero_Actual_Demand_on_To_Record,
    'N' Do_Not_Move_Adjusted_Demand_Indicator,
    'N' Do_Not_Move_Future_Forecast_Indicator,
    'N' Effective_Date_Option,
    ' ' Effective_Year,
    ' ' Effective_Period,
    'N' Adjust_From_Forecasts,
    'N' Move_History_Option,
    ' ' History_Begin_Year,
    ' ' History_Begin_Period,
    ' ' History_End_Year,
    ' ' History_End_Period,
    'Y' Create_MPX_Audit_Report,
    ' ' Transaction_File_Version,
    ' ' Factor_From_Promotional_Lift,
    ' ' Factor_To_Promotion_Lift,
    'Y' Copy_Remarks
from 
    Level_1_for_MPX
    join main_MPX_L0 on Lvl1Fcst1 = main_MPX_L0.PredecessorCode
      and Level_1_for_MPX.DC = main_MPX_L0.DC
""")
  
main_MPX_L2.createOrReplaceTempView('main_MPX_L2')
# main_MPX_L2.count()
# main_MPX_L2.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_MPX_L2', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
main_MPX_L2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,40.3 LOM MPX L3 - New version
main_MPX_L3 = spark.sql("""
select distinct
    'MPX'  Record_type,
    'D' Face_From,
    '3' Pyramid_level_From,
    'D' Group_Face_From,
    '4' Group_pyramid_level_From,
    
    nvl(Level_1_for_MPX.productStyle, 'No Style') || '_' || nvl(Level_1_for_MPX.sizeDescription, 'No Size')  Forecast_1_ID_Group_From,
    Field09 Forecast_2_ID_Group_From,
    field12 Forecast_3_ID_Group_From,
    
    '3' Item_pyramid_level_From,
    
    Lvl1Fcst1 Forecast_1_ID_Item_From,
    Field10 Forecast_2_ID_Item_From,
    field12 Forecast_3_ID_Item_From,
    
    'D' Face_To,
    '3' Pyramid_level_To,
    'D' Group_Face_To,
    '4' Group_pyramid_level_To,
    
    nvl(product_agg.productStyle, 'No Style') || '_' || nvl(product_agg.sizeDescription, 'No Size') Forecast_1_ID_Group_To,
    Field09 Forecast_2_ID_Group_To,
    field12 Forecast_3_ID_Group_To,
    
    '3' Item_pyramid_level_To,
    
    successorcode Forecast_1_ID_Item_To,
    Field10 Forecast_2_ID_Item_To,
    Field12 Forecast_3_ID_Item_To,
    
    cast(00000000000 as decimal(11,5)) Factor_From_Records,
    'Y' Factor_Adjusted_Demand_on_From_Record,
    'N' Factor_Actual_Demand_on_From_Record,
    'N' Zero_Actual_Demand_on_From_Record,
    'Y' Create_Supersedure_Key_on_From_Record,
    'Y' Inhibit_From_Forecasts_on_From_Record,
    'N' Delete_the_From_Record,
    cast(00000100000 as decimal(11,5)) Factor_To_Record,
    'Y' Factor_Adjusted_Demand_on_To_Record,
    'N' Factor_Actual_Demand_on_To_Record,
    'Y' Zero_Actual_Demand_on_To_Record,
    'N' Do_Not_Move_Adjusted_Demand_Indicator,
    'N' Do_Not_Move_Future_Forecast_Indicator,
    'N' Effective_Date_Option,
    ' ' Effective_Year,
    ' ' Effective_Period,
    'N' Adjust_From_Forecasts,
    'N' Move_History_Option,
    ' ' History_Begin_Year,
    ' ' History_Begin_Period,
    ' ' History_End_Year,
    ' ' History_End_Period,
    'Y' Create_MPX_Audit_Report,
    ' ' Transaction_File_Version,
    ' ' Factor_From_Promotional_Lift,
    ' ' Factor_To_Promotion_Lift,
    'Y' Copy_Remarks
from 
    Level_1_for_MPX
    join main_MPX_L0 on Lvl1Fcst1 = main_MPX_L0.PredecessorCode
      and Level_1_for_MPX.DC = main_MPX_L0.DC
    join (select productCode, productStyle, sizeDescription from s_core.product_agg where _source = 'PDH' and not _deleted) product_agg on main_MPX_L0.successorcode = product_agg.productCode
""")
  
main_MPX_L3.createOrReplaceTempView('main_MPX_L3')
# main_MPX_L3.count()
# main_MPX_L3.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LOM_MPX_L3', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
main_MPX_L3.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

all_do_items = spark.sql("""
select distinct 
  forecast_1_id , split(forecast_2_id, '_')[1] forecast_2_id
from 
  g_tembo.lom_aa_do_l1_hist_archive
  join locationmaster lm on split(forecast_2_id, '_')[1] = lm.globalLocationCode
where upper(nvl(user_data_16, 'Active')) in ('ACTIVE', 'NEW', 'DISCON', 'DISCONTINUED')
  and upper(forecast_1_id) not like 'DEL-%'
union 
select distinct 
  forecast_1_id , split(forecast_2_id, '_')[1] forecast_2_id
from 
  LOM_AA_DO_L1_ALL_UNIQUE
where 
  forecast_1_id not like '%#Error%'
  and forecast_2_id not like '%#Error%'
  and _pdhproductflag is null
  and upper(nvl(user_data_16, 'Active')) in ('ACTIVE', 'NEW', 'DISCON', 'DISCONTINUED')
  and upper(forecast_1_id) not like 'DEL-%'
union 
select distinct 
  lvl1fcst forecast_1_id, 
  split(lvl2fcst, '_')[1]  forecast_2_id
from 
  logftp.logility_do_level_1_for_mpx
  join locationmaster lm on split(lvl2fcst, '_')[1]  = lm.globalLocationCode
where 1=1
  and upper(lvl2fcst) not like 'DEL-%'
  and split(lvl2fcst, '_')[1] not in ('325', '828')  
""")
target_file = get_file_path(temp_folder, 'all_do_items')
all_do_items.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
all_do_items = spark.read.format('delta').load(target_file)
all_do_items.createOrReplaceTempView('all_do_items')
# all_do_items.display()

# COMMAND ----------

# DBTITLE 1,Define Make items
make_items = spark.sql("""
select distinct
  pr._source,
  nvl(e2eproductCode, pr.productcode) productCode,
  'DC' || org.organizationcode organizationCode,
  fixedleadtime, 
  po.productStatus
from 
  all_do_items
  join s_core.product_agg pr on  all_do_items.forecast_1_id = pr.e2eProductCode
  join s_core.product_org_agg po on pr._id = po.item_id
  join s_core.organization_agg org on po.organization_id = org._id
  join locationmaster lm on nvl(org.commonOrganizationCode, 'DC' || org.organizationcode) = lm.globalLocationCode
where 
  not po._deleted 
  and not pr._deleted
  and not org._deleted
  and po.makeorbuyflag = 'Make'
--   and po._source in  (select sourceSystem from source_systems) 
 
""")
make_items.createOrReplaceTempView('make_items')
# make_items.display()

# COMMAND ----------

# DBTITLE 1,Define Buy items
buy_items = spark.sql("""
select distinct
  pr._source,
  nvl(pr.e2eproductCode, pr.productcode) productCode,
  nvl(org.commonOrganizationCode, 'DC' || org.organizationcode) organizationCode,
  fixedleadtime, 
  pr.productStatus,
  po.productStatus,
  pr.ansStdUomConv,
  pr.originId
from 
  all_do_items
  join s_core.product_agg pr on  all_do_items.forecast_1_id = pr.e2eProductCode
  join s_core.product_org_agg po on pr._id = po.item_id
  join s_core.organization_agg org on po.organization_id = org._id
  join locationmaster lm on nvl(org.commonOrganizationCode, 'DC' || org.organizationcode) = lm.globalLocationCode
where 
  not po._deleted 
  and not pr._deleted
  and not org._deleted
  and po.makeorbuyflag = 'Buy'
  and nvl(org.commonOrganizationCode, 'DC' || org.organizationcode)  = all_do_items.forecast_2_id
  and nvl(org.commonOrganizationCode, 'Include') not in ('DCHER')
--   and pr.productStatus not in ('Inactive')
--   and pr._source  in  (select sourceSystem from source_systems) 
  -- and lower(po.productStatus) not in ('inactive', 'obsolete')
  and po._SOURCE || '-' || upper(nvl(po.productStatus,'X')) not in ('EBS-INACTIVE', 'EBS-OBSOLETE', 'KGD-INACTIVE', 'KGD-OBSOLETE','TOT-INACTIVE', 'TOT-OBSOLETE','COL-INACTIVE', 'COL-OBSOLETE','SAP-INACTIVE')
""")
buy_items.createOrReplaceTempView('buy_items')
# buy_items.display()
# buy_items.count()

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

# DBTITLE 1,Define intra org transit times
intra_org_transit = spark.sql("""
select distinct
    sm.DEFAULT_FLAG, 
    sm.INTRANSIT_TIME TransitTime, 
    FR.ORGANIZATION_CODE as FROM_ORG, 
    flc.LOCATION_CODE, 
    flc.DESCRIPTION, 
    sm.FROM_LOCATION_ID, 
    TT.ORGANIZATION_CODE as TO_ORG,  
    tlc.LOCATION_CODE, 
    tlc.DESCRIPTION,
    sm.TO_LOCATION_ID
from
  ebs.msc_interorg_ship_methods sm, -- 10
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
  ebS.HR_ALL_ORGANIZATION_UNITS fhorg, -- 2198
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
# intra_org_transit.display()

# COMMAND ----------

inventory_items = spark.sql("""
select distinct 
   inv._source,
   pr.e2eProductCode productCode,
   NVL(org.commonOrganizationCode, 'DC' || org.organizationCode) DC,
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
    and year(inv.inventoryDate) * 100 + month(inv.inventoryDate) >=  (select runPeriod from run_period)
    and inv.primaryQty > 0
    and not inv._deleted
    and comp.organizationCode not in (select organization from exclude_organizations)
  --  and pr_org.productStatus not in ('DISCONTINUED', 'Obsolete', 'Inactive')
""")
inventory_items.createOrReplaceTempView('inventory_items')
# inventory_items.display()

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

all_active_items=spark.sql("""
select 
  pr.e2eProductCode || '-' || nvl(org.commonorganizationCode, 'DC' || org.organizationCode) key,
  pr._source,
  pr.e2eProductCode productCode,
  pr_org.productstatus,
  nvl(org.commonorganizationCode, 'DC' || org.organizationCode) DC
from 
  s_core.product_org_agg pr_org
  join s_core.product_agg pr on pr_org.item_id = pr._id
  join s_core.organization_agg org on pr_org.organization_id = org._id
where 
  not pr_org._deleted
  and not pr._deleted
  and not org._deleted
  and nvl(upper(pr_org.productstatus),'ACTIVE')  in ('ACTIVE', 'NEW')
  and nvl(org.commonorganizationCode, 'DC' || org.organizationCode) not in ('DCHER')
  and pr._source in (select sourceSystem from source_systems)
""")
all_active_items.createOrReplaceTempView('all_active_items')

# COMMAND ----------

all_items=spark.sql("""
select 
   case 
   when forecast_2_id in ('DCABL', 'DCHER')
     then 'TOT'
   when forecast_2_id in ('DC819')
     then 'COL'
   when forecast_2_id in ('DCASWH')
     then 'KGD'
   when all_do_items.forecast_2_id in ('DSEMEA', 'DSAPAC', 'DCAMR1', 'DCNLT1', 'DCAJN1' )
      or forecast_2_id like 'DCANV%' or forecast_2_id like 'DCAAL%' or forecast_2_id like 'DCALL%'
      then 'SAP'
    else 'EBS'
  end _source,
  forecast_1_id productCode,
  forecast_2_id DC,
  pdhProduct.ansStdUomConv,
  pdhProduct.originId,
  pdhProduct.ansStdUom,
  '' pdhProductFlag
from 
  all_do_items
  join pdhProduct on forecast_1_id = pdhProduct.productCode
""")
# all_items.display()
all_items.createOrReplaceTempView('all_items')

# COMMAND ----------

itemsourceexceptions_no_multisource=spark.sql("""
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
   SOURCE_LOCATIONValue source,
   ORIGIN_CODEValue origin,
   ORGANIZATION_CODEValue organization,
   SOURCING_RULE_NAME sourcingRule
   
from 
  spt.itemsourceexceptions
where
  ParameterValue = 'Overwrite Source'
  and ParameterValue0 = 'Y'
  and not _deleted
""")
itemsourceexceptions_no_multisource.createOrReplaceTempView('itemsourceexceptions_no_multisource')
# itemsourceexceptions_no_multisource.display()

# COMMAND ----------

itemsourceexceptions_multisource=spark.sql("""
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
   SOURCE_LOCATIONValue source,
   ORIGIN_CODEValue origin,
   ORGANIZATION_CODEValue organization,
   SOURCING_RULE_NAME sourcingRule
   
from 
  spt.itemsourceexceptions
where   
  ParameterValue = 'Multi-Source'
  and ParameterValue0 = 'Y'
  and not _deleted
""")
itemsourceexceptions_multisource.createOrReplaceTempView('itemsourceexceptions_multisource')
# itemsourceexceptions_multisource.display()

# COMMAND ----------

itemsourceexceptions_Planning_Time_Fence=spark.sql("""
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions
where
  ParameterValue = 'Planning Time Fence'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not _deleted
  and item_number is not null
union 
select 
  pdhProduct.productCode item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions spt
  join pdhProduct on spt.style = pdhProduct.productStyle
where
  spt.ParameterValue = 'Planning Time Fence'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not spt._deleted
  and spt.item_number is null
  and spt.style is not null
  and pdhProduct.productCode not in (select item_number from spt.itemsourceexceptions where ParameterValue = 'Planning Time Fence'  and nvl(cast(ParameterValue0 as integer),0) <> 0 and not _deleted and item_number is not null)
""")
itemsourceexceptions_Planning_Time_Fence.createOrReplaceTempView('itemsourceexceptions_Planning_Time_Fence')
#itemsourceexceptions_Planning_Time_Fence.display()

# COMMAND ----------

itemsourceexceptions_Max_Fulfillment_Delay_Days=spark.sql("""
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions
where
  ParameterValue = 'Max Fulfillment Delay Days'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not _deleted
  and item_number is not null
union 
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions spt
  join pdhProduct on spt.style = pdhProduct.productStyle
where
  spt.ParameterValue = 'Max Fulfillment Delay Days'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not spt._deleted
  and spt.item_number is null
  and spt.style is not null
  and pdhProduct.productCode not in (select item_number from spt.itemsourceexceptions where ParameterValue = 'Max Fulfillment Delay Days'  and nvl(cast(ParameterValue0 as integer),0) <> 0 and not _deleted and item_number is not null)
""")
itemsourceexceptions_Max_Fulfillment_Delay_Days.createOrReplaceTempView('itemsourceexceptions_Max_Fulfillment_Delay_Days')
# itemsourceexceptions_Max_Fulfillment_Delay_Days.display()

# COMMAND ----------

itemsourceexceptions_Order_Quantity_Days=spark.sql("""
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions
where
  ParameterValue = 'Order Quantity Days'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not _deleted
  and item_number is not null
union 
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions spt
  join pdhProduct on spt.style = pdhProduct.productStyle
where
  ParameterValue = 'Order Quantity(Days)' 
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not spt._deleted
  and spt.item_number is null
  and spt.style is not null
  and pdhProduct.productCode not in (select item_number from spt.itemsourceexceptions where ParameterValue = 'Order Quantity Days'  and nvl(cast(ParameterValue0 as integer),0) <> 0 and not _deleted and item_number is not null)
""")
itemsourceexceptions_Order_Quantity_Days.createOrReplaceTempView('itemsourceexceptions_Order_Quantity_Days')
# itemsourceexceptions_Order_Quantity_Days.display()

# COMMAND ----------

itemsourceexceptions_Minimum_Order_Quantity=spark.sql("""
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions
where
  ParameterValue = 'Minimum Order Quantity'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not _deleted
  and item_number is not null
union 
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions spt
  join pdhProduct on spt.style = pdhProduct.productStyle
where
  ParameterValue = 'Minimum Order Quantity'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not spt._deleted
  and spt.item_number is null
  and spt.style is not null
  and pdhProduct.productCode not in (select item_number from spt.itemsourceexceptions where ParameterValue = 'Minimum Order Quantity'  and nvl(cast(ParameterValue0 as integer),0) <> 0 and not _deleted and item_number is not null)
""")
itemsourceexceptions_Minimum_Order_Quantity.createOrReplaceTempView('itemsourceexceptions_Minimum_Order_Quantity')
# itemsourceexceptions_Minimum_Order_Quantity.display()

# COMMAND ----------

itemsourceexceptions_Load_Optimizer_Clusters=spark.sql("""
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions
where
  ParameterValue = 'Load Optimizer Clusters'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not _deleted
  and item_number is not null
union 
select 
  item_number,
  DESTINATION_LOCATIONValue destination,
  cast(ParameterValue0 as integer) as ParameterValue
from 
  spt.itemsourceexceptions spt
  join pdhProduct on spt.style = pdhProduct.productStyle
where
  ParameterValue = 'Cluster'
  and nvl(cast(ParameterValue0 as integer),0) <> 0
  and not spt._deleted
  and spt.item_number is null
  and spt.style is not null
  and pdhProduct.productCode not in (select item_number from spt.itemsourceexceptions where ParameterValue = 'Minimum Order Quantity'  and nvl(cast(ParameterValue0 as integer),0) <> 0 and not _deleted and item_number is not null)
""")
itemsourceexceptions_Load_Optimizer_Clusters.createOrReplaceTempView('itemsourceexceptions_Load_Optimizer_Clusters')
# itemsourceexceptions_Load_Optimizer_Clusters.display()

# COMMAND ----------

# DBTITLE 1,CDR AI Scenario 3a
cdr_ai_3a = spark.sql("""
-- only MAKE items
-- Source Location #1 is the same as Forecast_2_ID
select distinct
  '3a' scenario,
   '' _source, --main._source, 
   '2'  Pyramid_Level,
   main.productCode Forecast_1_ID, 
   main.DC Forecast_2_ID,
   'AI' Record_type,
   main.DC Source_location_1,
   make_items.fixedLeadTime Replenishment_lead_time_1,
   main.DC Vendor,
   'DEFAULT' DRP_planner,
   'M' Make_buy_code,
   nvl(cast(main.ansStdUomConv as decimal(7,0)), '#Error31') Order_multiple,
   Cast(10 as numeric(3,0)) Network_level,
   main.originId,
   '' pdhProductFlag,
   30 Order_Quantity_Days
 from 
  all_items main
  join make_items on main.productCode = make_items.productCode and main.DC = make_items.organizationCode
where 1=1
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3a')
cdr_ai_3a.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3a = spark.read.format('delta').load(target_file)
cdr_ai_3a.createOrReplaceTempView('cdr_ai_3a')
# cdr_ai_3a.display()
# cdr_ai_3a.count()

# COMMAND ----------

# DBTITLE 1,CDR AI Scenario 3bi1 - item - source - destination is on exception list first record
cdr_ai_3bi1 = spark.sql("""
select distinct
    '3bi1' scenario,
    'EM' _source,
    '2' Pyramid_Level,
    main.item_number Forecast_1_ID,
    main.destination Forecast_2_ID,
    'AI' Record_type,
    main.source Source_location_1,
    case
      when intra_org_transit.transitTime is null and left(main.destination,2) = 'DC' and left(main.source,2) = 'DC' then cast(7 as numeric(3,0))
      when intra_org_transit.transitTime is null then cast(222 as numeric(3,0))  
      else cast(intra_org_transit.TransitTime +  nvl(sources.orderleadtime,0) as numeric(3,0))
    end Replenishment_lead_time_1,
    main.source Vendor,
    'DEFAULT' DRP_planner,
    'X' Make_buy_code,
    nvl(cast(pdhProduct.ansStdUomConv as decimal(7,0)), '#Error31') Order_multiple,
    Cast(10 as numeric(3,0)) Network_level,
    case when left(main.source,2) = 'DC'
      then ''
     else
       sources.originId
    end originId,
    '' pdhProductFlag,
    30 Order_Quantity_Days
from
    itemsourceexceptions_no_multisource main
     join (select
             Globallocationcode,
             GTCPODPortOfDeparture,
             Locationtype,
             REPLACE(STRING(INT(EBSSUPPLIERCODE)), ",", "") EBSSUPPLIERCODE,
             REPLACE(STRING(INT (SAPGOODSSUPPLIERCODE)), ",", "") SAPGOODSSUPPLIERCODE,
             REPLACE(STRING(INT(MICROSIGASUPPLIERCODE)), ",", "") MICROSIGASUPPLIERCODE,
             REPLACE(STRING(INT(KINGDEESUPPLIERCODE)), ",", "") KINGDEESUPPLIERCODE,
             REPLACE(STRING(INT(DYNAMICSNAVSUPPLIERCODECOLOMBIA)), ",", "") DYNAMICSNAVSUPPLIERCODECOLOMBIA,
             INT (orderleadtime) orderleadtime,
             origin originId
         from
             locationmaster) sources on main.source = sources.Globallocationcode
     join (select
              Globallocationcode, GTCPOAPortOfArrival
         from
             locationmaster
         where 1=1) destinations on destination = destinations.Globallocationcode
         
    left join intra_org_transit on destinations.GTCPOAPortOfArrival = intra_org_transit.TO_ORG
       and sources.GTCPODPortOfDeparture = intra_org_transit.FROM_ORG
    join pdhProduct on main.item_number = pdhProduct.productCode
where 1=1
    and not exists (select 1 from cdr_ai_3a where main.item_number = cdr_ai_3a.Forecast_1_ID and main.destination = cdr_ai_3a.Forecast_1_ID )
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3bi1')
cdr_ai_3bi1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3bi1 = spark.read.format('delta').load(target_file)
cdr_ai_3bi1.createOrReplaceTempView('cdr_ai_3bi1')
# cdr_ai_3bi1.display()
# cdr_ai_3bi1.count()

# COMMAND ----------

# DBTITLE 1,CDR AI Scenario 3bi2 - item - source - destination is on exception list second record
cdr_ai_3bi2 = spark.sql("""
with q1 as
(select  distinct
   '3bi2' scenario,
    main._source, 
    '2'  Pyramid_Level,
    main.Forecast_1_ID, 
    itemsourceexceptions_no_multisource.destination Forecast_2_ID,  
    'AI' Record_type,
    itemsourceexceptions_no_multisource.source Source_location_1,
    case
      when FND_FLEX_VALUES_VL.transitTime is null and left(itemsourceexceptions_no_multisource.destination,2) = 'DC' and left(itemsourceexceptions_no_multisource.source,2) = 'DC' then cast(7 as numeric(3,0))
      when FND_FLEX_VALUES_VL.transitTime is null then cast(222 as numeric(3,0))  -- '#Error33-' || 'POD= ' || sources.GTCPODPortOfDeparture || ', POA = ' || destinations.GTCPOAPortOfArrival
      else cast(FND_FLEX_VALUES_VL.TransitTime +  nvl(sources.orderleadtime,0) as numeric(3,0))
    end Replenishment_lead_time_1,
   itemsourceexceptions_no_multisource.source Vendor,
   'DEFAULT' DRP_planner,
   case 
     when sources.Locationtype = 'Vendor Location' then 'B'
     when sources.Locationtype = 'Manufacturing Facility' then 'X'
     else '#Error26' || '-' || itemsourceexceptions_no_multisource.source
   end Make_buy_code,
   Order_multiple,
   Cast(20 as numeric(3,0)) Network_level,
   main.originId,
   '' pdhProductFlag,
   30 Order_Quantity_Days
from 
  cdr_ai_3bi1 main
  join itemsourceexceptions_no_multisource on   
     main.forecast_1_id = itemsourceexceptions_no_multisource.item_number 
     and main.Source_location_1 = itemsourceexceptions_no_multisource.destination

  join (select 
             Globallocationcode, 
             GTCPODPortOfDeparture,
             Locationtype, 
             REPLACE(STRING(INT(EBSSUPPLIERCODE)), ",", "") EBSSUPPLIERCODE, 
             REPLACE(STRING(INT (SAPGOODSSUPPLIERCODE)), ",", "") SAPGOODSSUPPLIERCODE,
             REPLACE(STRING(INT(MICROSIGASUPPLIERCODE)), ",", "") MICROSIGASUPPLIERCODE, 
             REPLACE(STRING(INT(KINGDEESUPPLIERCODE)), ",", "") KINGDEESUPPLIERCODE, 
             REPLACE(STRING(INT(DYNAMICSNAVSUPPLIERCODECOLOMBIA)), ",", "") DYNAMICSNAVSUPPLIERCODECOLOMBIA, 
             INT (orderleadtime) orderleadtime
         from 
             locationmaster
         where 1=1) sources on itemsourceexceptions_no_multisource.source = sources.Globallocationcode
         
     join (select 
              Globallocationcode, GTCPOAPortOfArrival 
         from 
             locationmaster
         where 1=1) destinations on itemsourceexceptions_no_multisource.destination = destinations.Globallocationcode
       
     left join FND_FLEX_VALUES_VL on destinations.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival 
        and sources.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
)
select
    q1.scenario,
    q1._source, 
    q1.Pyramid_Level,
    q1.Forecast_1_ID, 
    q1.Forecast_2_ID,  
    q1.Record_type,
    q1.Source_location_1,
    q1.Replenishment_lead_time_1,
    q1.Vendor,
    q1.DRP_planner,
    q1.Make_buy_code,
    q1.Order_multiple,
    q1.Network_level,
    q1.originId,
    q1.pdhProductFlag,
    q1.Order_Quantity_Days
from 
  q1
  left join all_items on q1.Forecast_1_ID = all_items.productcode and q1.Source_location_1 = all_items.DC
where
  1=2
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3bi2')
cdr_ai_3bi2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3bi2 = spark.read.format('delta').load(target_file)
cdr_ai_3bi2.createOrReplaceTempView('cdr_ai_3bi2')
# cdr_ai_3bi2.display()
# cdr_ai_3bi2.count()

# COMMAND ----------

# DBTITLE 1,CDR AI Scenario 3bi3 - item - origin - source exception list third record
cdr_ai_3bi3=spark.sql("""
select distinct
  '3bi3' scenario,
  main._source,
  '2' Pyramid_Level,
  main.Forecast_1_ID, 
  main.Source_Location_1 Forecast_2_ID,
  'AI' Record_Type,
  sources.Globallocationcode Source_Location_1,
  case 
      when FND_FLEX_VALUES_VL.transitTime is null and left( main.Source_Location_1,2) = 'DC' and left(sources.Globallocationcode ,2) = 'DC' then cast(7 as numeric(3,0))
      when FND_FLEX_VALUES_VL.transitTime is null then cast(222 as numeric(3,0))  -- '#Error33-' || 'POD= ' || sources.GTCPODPortOfDeparture || ', POA = ' || destinations.GTCPOAPortOfArrival
      else cast(FND_FLEX_VALUES_VL.TransitTime +  nvl(sources.orderleadtime,0) as numeric(3,0))
   end Replenishment_lead_time_1,
  sources.Globallocationcode Vendor,
   'DEFAULT' DRP_planner,
   case 
     when sources.Locationtype = 'Vendor Location' then 'B'
     when sources.Locationtype = 'Manufacturing Facility' then 'X'
     else '#Error26' || '-' || sources.Globallocationcode
   end Make_buy_code,
   main.Order_multiple,
   Cast(30 as numeric(3,0)) Network_level,
   main.originId,
   '' pdhProductFlag,
   30 Order_Quantity_Days
from 
  cdr_ai_3bi1 main
  join (select 
             Globallocationcode, 
             GTCPODPortOfDeparture,
             Locationtype, 
             REPLACE(STRING(INT(EBSSUPPLIERCODE)), ",", "") EBSSUPPLIERCODE, 
             REPLACE(STRING(INT (SAPGOODSSUPPLIERCODE)), ",", "") SAPGOODSSUPPLIERCODE,
             REPLACE(STRING(INT(MICROSIGASUPPLIERCODE)), ",", "") MICROSIGASUPPLIERCODE, 
             REPLACE(STRING(INT(KINGDEESUPPLIERCODE)), ",", "") KINGDEESUPPLIERCODE, 
             REPLACE(STRING(INT(DYNAMICSNAVSUPPLIERCODECOLOMBIA)), ",", "") DYNAMICSNAVSUPPLIERCODECOLOMBIA, 
             INT (orderleadtime) orderleadtime,
             origin
         from 
             locationmaster
         where origin is not null) sources on main.originId = sources.origin 
         
     join (select 
              Globallocationcode, GTCPOAPortOfArrival 
         from 
             locationmaster
         where 1=1) destinations on main.Source_Location_1 = destinations.Globallocationcode
     
     left join FND_FLEX_VALUES_VL on destinations.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival 
        and sources.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
     where 
       left(main.Source_Location_1, 2) not in ('EM', 'PL')
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3bi3')
cdr_ai_3bi3.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3bi3 = spark.read.format('delta').load(target_file)
cdr_ai_3bi3.createOrReplaceTempView('cdr_ai_3bi3')
# cdr_ai_3bi3.display()

# COMMAND ----------

# DBTITLE 1,CDR AI Scenario 3bi4 - item - source - source exception list closing 4th record
cdr_ai_3bi4=spark.sql("""
with cdr_3bi4 as
(select distinct 
  _source, 
  forecast_1_id, 
  source_location_1,
  Order_multiple
from 
  cdr_ai_3bi2
union 
select distinct 
  _source, 
  forecast_1_id, 
  source_location_1,
  Order_multiple
from 
  cdr_ai_3bi3)
select distinct
    '3bi4' scenario,
    main._source, 
    '2'  Pyramid_Level,
    main.Forecast_1_ID, 
    main.source_location_1 Forecast_2_ID,  
    'AI' Record_type,
    main.source_location_1 Source_location_1,
    cast(5 as numeric(3,0)) Replenishment_lead_time_1,
    main.source_location_1 Vendor,
    'DEFAULT' DRP_planner,
    'M' Make_buy_code,
    Order_multiple,
    Cast(90 as numeric(3,0)) Network_level,
    '' pdhProductFlag,
   30 Order_Quantity_Days
from cdr_3bi4 main
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3bi4')
cdr_ai_3bi4.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3bi4 = spark.read.format('delta').load(target_file)
cdr_ai_3bi4.createOrReplaceTempView('cdr_ai_3bi4')
# cdr_ai_3bi4.display()

# COMMAND ----------

cumulative_cdr_ai1 = (
  cdr_ai_3a.select('Forecast_1_ID','Forecast_2_ID')
  .union(cdr_ai_3bi1.select('Forecast_1_ID','Forecast_2_ID'))
  .union(cdr_ai_3bi2.select('Forecast_1_ID','Forecast_2_ID'))
  .union(cdr_ai_3bi3.select('Forecast_1_ID','Forecast_2_ID'))
  .union(cdr_ai_3bi4.select('Forecast_1_ID','Forecast_2_ID'))
)

cumulative_cdr_ai1.createOrReplaceTempView('cumulative_cdr_ai1')
cumulative_cdr_ai1.cache()

# COMMAND ----------

# DBTITLE 1,CDR AI Scenario 3bii1 - item - source - destination is NOT on exception list
cdr_ai_3bii1 = spark.sql("""
-- not make items
-- default sourcing rule = 'Origin'
-- use origin code from respective item in PDH
select distinct
  '3bii1' scenario,
  '' _source, --main._source, 
  '2'  Pyramid_Level,
  main.productcode Forecast_1_ID, 
  main.dc Forecast_2_ID,  
  'AI' Record_type,
  case when main.originid is null then '#Error29-' || main.productcode else nvl(globalLocationsOrigins.globallocationcode, '#Error22' || '-' || main.originId) end Source_location_1, 
  case
    when globalLocationsDC.GTCPOAPortOfArrival is null and left(main.dc,2) = 'DC' and left(globalLocationsOrigins.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0))
    when globalLocationsDC.GTCPOAPortOfArrival is null then cast(222 as numeric(3,0))
    when globalLocationsOrigins.GTCPODPortOfDeparture is null and left(main.dc,2) = 'DC' and left(globalLocationsOrigins.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0))
    when globalLocationsOrigins.GTCPODPortOfDeparture is null then cast(222 as numeric(3,0))
    when nvl(FND_FLEX_VALUES_VL.transitTime,0) = 0 and left(main.dc,2) = 'DC' and left(globalLocationsOrigins.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0))
    when nvl(FND_FLEX_VALUES_VL.transitTime,0) = 0 then cast(222 as numeric(3,0))
    when nvl(globalLocationsOrigins.orderleadtime,0) = 0 and left(main.dc,2) = 'DC' and left(globalLocationsOrigins.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0))
    when nvl(globalLocationsOrigins.orderleadtime,0) = 0 then cast(222 as numeric(3,0))
    when FND_FLEX_VALUES_VL.TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) > 999 and left(main.dc,2) = 'DC' and left(globalLocationsOrigins.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0))
    when FND_FLEX_VALUES_VL.TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) > 999 then cast(222 as numeric(3,0))
    else cast(FND_FLEX_VALUES_VL.TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) as numeric(3,0))
  end Replenishment_lead_time_1,
  case when main.originid is null then '#Error29-' || main.productcode else nvl(globalLocationsOrigins.globallocationcode, '#Error22' || '-' || main.originId) end Vendor,
   'DEFAULT' DRP_planner,
   case 
     when globalLocationsOrigins.Locationtype = 'Vendor Location' then 'B'
     when globalLocationsOrigins.Locationtype = 'Manufacturing Facility' then 'X'
     else '#Error26' || '-' || main.originId
   end Make_buy_code,
   nvl(cast(main.ansStdUomConv as decimal(7,0)), '#Error31') Order_multiple,
   Cast(10 as numeric(3,0)) Network_level,
   '' pdhProductFlag,
   30 Order_Quantity_Days
from 
  all_items main
  join (select 
            Globallocationcode dc, GTCPOAPortOfArrival, GTCPODPortOfDeparture, inventoryHoldingERP 
        from 
            locationmaster
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
            locationmasterNonPrimary
         ) globalLocationsOrigins on main.originId = globalLocationsOrigins.originCode
  left join FND_FLEX_VALUES_VL on globalLocationsDC.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival 
    and globalLocationsOrigins.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
  left join intra_org_transit on globalLocationsDC.GTCPOAPortOfArrival = intra_org_transit.TO_ORG 
    and globalLocationsOrigins.GTCPODPortOfDeparture = intra_org_transit.FROM_ORG
  left join (select distinct item_number, destination, source from itemsourceexceptions_no_multisource) itemsourceexceptions_no_multisource on 
    main.productcode = itemsourceexceptions_no_multisource.item_number 
    and main.dc = itemsourceexceptions_no_multisource.destination
    and nvl(globalLocationsOrigins.globallocationcode, '#Error22' || '-' || main.originId) = itemsourceexceptions_no_multisource.source
where 1=1
  and main.productcode || '-' ||  main.DC not in (select productcode || '-' || organizationcode from make_items)
  and itemsourceexceptions_no_multisource.item_number is null
  and  not exists (select distinct Forecast_1_ID || '-' Forecast_2_ID from cumulative_cdr_ai1 where main.productcode = cumulative_cdr_ai1.Forecast_1_ID and main.dc = cumulative_cdr_ai1.Forecast_2_ID)
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3bii1')
cdr_ai_3bii1.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3bii1 = spark.read.format('delta').load(target_file)
cdr_ai_3bii1.createOrReplaceTempView('cdr_ai_3bii1')
# cdr_ai_3bii1.display()
# cdr_ai_3bii1.count()

# COMMAND ----------

cumulative_cdr_ai2 = (
   cdr_ai_3a.select('Forecast_1_ID','Forecast_2_ID', 'Source_location_1')
  .union(cdr_ai_3bi1.select('Forecast_1_ID','Forecast_2_ID', 'Source_location_1'))
  .union(cdr_ai_3bi2.select('Forecast_1_ID','Forecast_2_ID', 'Source_location_1'))
  .union(cdr_ai_3bi3.select('Forecast_1_ID','Forecast_2_ID', 'Source_location_1'))
  .union(cdr_ai_3bi4.select('Forecast_1_ID','Forecast_2_ID', 'Source_location_1'))
  .union(cdr_ai_3bii1.select('Forecast_1_ID','Forecast_2_ID', 'Source_location_1'))
)

cumulative_cdr_ai2.createOrReplaceTempView('cumulative_cdr_ai2')
cumulative_cdr_ai2.cache()

# COMMAND ----------

## closing
cdr_ai_3bii2 = spark.sql("""
select distinct
  '3bii2' scenario,
  cdr_ai_3bii1._source,
  Pyramid_Level,
  cdr_ai_3bii1.Forecast_1_ID,
  cdr_ai_3bii1.Source_location_1 Forecast_2_ID,
  'AI' Record_type,
  cdr_ai_3bii1.Source_location_1, 
  cast(5 as numeric(3,0)) Replenishment_lead_time_1,
  cdr_ai_3bii1.Source_location_1 Vendor,
  DRP_planner,
  'M' Make_buy_code,
  cdr_ai_3bii1.Order_multiple,
  Cast(90 as numeric(3,0)) Network_level,
  '' pdhProductFlag,
   30 Order_Quantity_Days
from 
  cdr_ai_3bii1
  left join cumulative_cdr_ai2 on 
      cdr_ai_3bii1.Forecast_1_ID = cumulative_cdr_ai2.Forecast_1_ID 
      and cdr_ai_3bii1.Source_location_1 = cumulative_cdr_ai2.Forecast_2_ID 
      and cdr_ai_3bii1.Source_location_1 = cumulative_cdr_ai2.Source_location_1 
 where 1=1
     and cumulative_cdr_ai2.Forecast_1_ID is null
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3bii2')
cdr_ai_3bii2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3bii2 = spark.read.format('delta').load(target_file)
cdr_ai_3bii2.createOrReplaceTempView('cdr_ai_3bii2')

# cdr_ai_3bii2.display()

# COMMAND ----------

cumulative_cdr_ai3 = (
   cdr_ai_3a.select('Forecast_1_ID','Forecast_2_ID')
  .union(cdr_ai_3bi1.select('Forecast_1_ID','Forecast_2_ID'))
  .union(cdr_ai_3bi2.select('Forecast_1_ID','Forecast_2_ID'))
  .union(cdr_ai_3bi3.select('Forecast_1_ID','Forecast_2_ID'))
  .union(cdr_ai_3bi4.select('Forecast_1_ID','Forecast_2_ID'))
  .union(cdr_ai_3bii1.select('Forecast_1_ID','Forecast_2_ID'))
  .union(cdr_ai_3bii2.select('Forecast_1_ID','Forecast_2_ID'))
)

cumulative_cdr_ai3.createOrReplaceTempView('cumulative_cdr_ai3')
cumulative_cdr_ai3.cache()

# COMMAND ----------

# DBTITLE 1,CDR AI not on source exception list
cdr_ai_3ci = spark.sql("""
with c1 as
(select distinct
  '3ci' scenario,
  '' _source, --main._source, 
  '2'  Pyramid_Level,
  main.productcode Forecast_1_ID, 
  main.DC Forecast_2_ID,
  'AI' Record_type,
  nvl(globalLocationsDC.Defaultsourcingrule, '#Error22' || '-' || main.originId)  Source_location_1, 
  case
    when globalLocationsDC.GTCPOAPortOfArrival is null and left(main.DC,2) = 'DC' and left(globalLocationsDC.Defaultsourcingrule,2) = 'DC' then cast(7 as numeric(3,0)) 
    when globalLocationsDC.GTCPOAPortOfArrival is null then cast(222 as numeric(3,0)) 
    when globalLocationsOrigins.GTCPODPortOfDeparture is null and left(main.DC,2) = 'DC' and left(globalLocationsDC.Defaultsourcingrule,2) = 'DC' then cast(7 as numeric(3,0))
    when globalLocationsOrigins.GTCPODPortOfDeparture is null then cast(222 as numeric(3,0))
    when nvl(FND_FLEX_VALUES_VL.transitTime,0) = 0 and intra_org_transit.from_org is null and left(main.DC,2) = 'DC' and left(globalLocationsDC.Defaultsourcingrule,2) = 'DC' then cast(7 as numeric(3,0)) 
    when nvl(FND_FLEX_VALUES_VL.transitTime,0) = 0 and intra_org_transit.from_org is null then cast(222 as numeric(3,0))  -- '#Error33-' || 'POD= ' || globalLocationsOrigins.GTCPODPortOfDeparture || ', POA = ' || globalLocationsDC.GTCPOAPortOfArrival
    when nvl(globalLocationsOrigins.orderleadtime,0) = 0 and left(main.DC,2) = 'DC' and left(globalLocationsDC.Defaultsourcingrule,2) = 'DC' then cast(7 as numeric(3,0))
    when nvl(globalLocationsOrigins.orderleadtime,0) = 0 then cast(222 as numeric(3,0))  
    when FND_FLEX_VALUES_VL.TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) > 999 and left(main.DC,2) = 'DC' and left(globalLocationsDC.Defaultsourcingrule,2) = 'DC' then cast(7 as numeric(3,0)) 
    when FND_FLEX_VALUES_VL.TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) > 999 then cast(222 as numeric(3,0))
    when intra_org_transit.from_org is not null then cast(intra_org_transit.transitTime as numeric(3,0))
    else cast(FND_FLEX_VALUES_VL.TransitTime +  nvl(globalLocationsOrigins.orderleadtime,0) as numeric(3,0))
  end Replenishment_lead_time_1,
  nvl(globalLocationsDC.Defaultsourcingrule, 'DUMMY22' || '-' || main.originId) Vendor,
  'DEFAULT' DRP_planner,
  'X' Make_buy_code,
  nvl(cast(main.ansStdUomConv as decimal(7,0)), '#Error31') Order_multiple,
  Cast(20 as numeric(3,0)) Network_level,
  main.originId,
  globalLocationsDC.Defaultsourcingrule,
  '' pdhProductFlag,
   30 Order_Quantity_Days,
  globalLocationsDC.GTCPOAPortOfArrival,
  globalLocationsOrigins.GTCPODPortOfDeparture
from 
  all_items main
  join (select 
            Globallocationcode dc, GTCPOAPortOfArrival,GTCPODPortOfDeparture , Defaultsourcingrule
        from 
            locationmaster
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
            locationmaster) globalLocationsOrigins on globalLocationsDC.Defaultsourcingrule = globalLocationsOrigins.GlobalLocationCode
  left join FND_FLEX_VALUES_VL on globalLocationsDC.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival and globalLocationsOrigins.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
  left join intra_org_transit on globalLocationsOrigins.GTCPODPortOfDeparture = intra_org_transit.from_org and globalLocationsDC.GTCPOAPortOfArrival = intra_org_transit.to_org
  left join (select distinct item_number, destination, source from itemsourceexceptions_no_multisource) itemsourceexceptions_no_multisource on 
    main.productcode = itemsourceexceptions_no_multisource.item_number 
    and main.dc = itemsourceexceptions_no_multisource.destination
    and nvl(globalLocationsOrigins.globallocationcode, '#Error22' || '-' || main.originId) = itemsourceexceptions_no_multisource.source
  left join cumulative_cdr_ai3 on  main.productcode = cumulative_cdr_ai3.Forecast_1_ID and main.DC = Forecast_2_ID
 where 1=1
  and main.originId is not null
  and main.productcode || '-' ||  main.DC not in (select productcode || '-' || organizationcode from make_items)
  and itemsourceexceptions_no_multisource.item_number is null
  and cumulative_cdr_ai3.Forecast_1_ID is null
  )
select 
  c1.scenario,
  c1._source, 
  c1.Pyramid_Level,
  c1.Forecast_1_ID, 
  c1.Forecast_2_ID,
  c1.Record_type,
  c1.Source_location_1,
  c1.Replenishment_lead_time_1,
  c1.Vendor,
  c1.DRP_planner,
  c1.Make_buy_code,
  c1.Order_multiple,
  c1.Network_level,
  c1.originId,
  c1.Defaultsourcingrule,
  c1.pdhProductFlag, 
  c1.Order_Quantity_Days,
  c1.GTCPOAPortOfArrival,
  c1.GTCPODPortOfDeparture
from 
    c1
    left join all_items on c1.Forecast_1_ID = all_items.productcode and c1.Source_location_1 = all_items.DC
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3ci')
cdr_ai_3ci.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3ci = spark.read.format('delta').load(target_file)
cdr_ai_3ci.createOrReplaceTempView('cdr_ai_3ci')
# cdr_ai_3ci.display()
# cdr_ai_3ci.count()

# COMMAND ----------

cdr_ai_3cii = spark.sql("""

select 
  '3cii' scenario,
  main._source,
  main.Pyramid_Level,
  main.Forecast_1_ID,
  main.Source_location_1 Forecast_2_ID,
  'AI' Record_type,
  case when main.originId is null then '#Error29' else nvl(globallocationsDC.globallocationcode, '#Error30' || '-' || main.originId) end Source_location_1,
  case
    when globalLocationsSources.GTCPOAPortOfArrival is null and left(main.Source_location_1,2) = 'DC' and left(globallocationsDC.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0)) 
    when globalLocationsSources.GTCPOAPortOfArrival is null then cast(222 as numeric(3,0))
    when globalLocationsDC.GTCPODPortOfDeparture is null and left(main.Source_location_1,2) = 'DC' and left(globallocationsDC.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0))
    when globalLocationsDC.GTCPODPortOfDeparture is null then cast(222 as numeric(3,0))
    when nvl(FND_FLEX_VALUES_VL.transitTime,0) = 0 and left(main.Source_location_1,2) = 'DC' and left(globallocationsDC.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0)) 
    when nvl(FND_FLEX_VALUES_VL.transitTime,0) = 0 then cast(222 as numeric(3,0)) 
    when nvl(globalLocationsDC.orderleadtime,0) = 0 and left(main.Source_location_1,2) = 'DC' and left(globallocationsDC.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0))  
    when nvl(globalLocationsDC.orderleadtime,0) = 0 then cast(222 as numeric(3,0))
    when TransitTime +  nvl(globalLocationsDC.orderleadtime,0) > 999 and left(main.Source_location_1,2) = 'DC' and left(globallocationsDC.globallocationcode,2) = 'DC' then cast(7 as numeric(3,0))  
    when TransitTime +  nvl(globalLocationsDC.orderleadtime,0) > 999 then cast(222 as numeric(3,0))
    else cast(TransitTime +  nvl(globalLocationsDC.orderleadtime,0) as numeric(3,0))
  end Replenishment_lead_time_1,
  case when main.originId is null then '#DUMMY29' else nvl(globallocationsDC.globallocationcode, '#DUMMY30' || '-' || main.originId) end Vendor,
  'DEFAULT' DRP_planner,
  case when globalLocationsDC.locationType = 'Vendor Location' and globalLocationsSources.locationType = 'Distribution Center'
    then 'B'
    else 'X' 
  end Make_buy_code,
  Order_multiple,
  Cast(30 as numeric(3,0)) Network_level,
  globallocationsDC.Defaultsourcingrule,
  --null Defaultsourcingrule,
  main.pdhProductFlag,
   30 Order_Quantity_Days,
  globalLocationsDC.GTCPODPortOfDeparture,
  globalLocationsSources.GTCPOAPortOfArrival
  
from 
  cdr_ai_3ci main
  join (select 
            origin, globallocationcode, GTCPOAPortOfArrival, GTCPODPortOfDeparture, Defaultsourcingrule, 
            INT (orderleadtime) orderleadtime, locationType
        from 
            locationmasterNonPrimary) globalLocationsDC on main.originId= globallocationsDC.origin 
  join (select 
            origin, globallocationcode, GTCPOAPortOfArrival, GTCPODPortOfDeparture, Defaultsourcingrule, locationType
        from 
            locationmaster) globalLocationsSources on main.Source_location_1= globalLocationsSources.globallocationcode 
  left join FND_FLEX_VALUES_VL on globalLocationsSources.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival and globalLocationsDC.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
  left join cumulative_cdr_ai3 on main.Forecast_1_ID = cumulative_cdr_ai3.Forecast_1_ID and main.Source_location_1 =  cumulative_cdr_ai3.Forecast_2_ID

where cumulative_cdr_ai3.Forecast_1_ID is null
""")
target_file = get_file_path(temp_folder, 'cdr_ai_3cii')
cdr_ai_3cii.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3cii = spark.read.format('delta').load(target_file)
cdr_ai_3cii.createOrReplaceTempView('cdr_ai_3cii')
# cdr_ai_3cii.display()
# cdr_ai_3cii.count()

# COMMAND ----------

##-- cdr_ai_3ciii
cdr_ai_3ciii = spark.sql("""
select distinct
  '3ciii' scenario,
   main._source,
  main.Pyramid_Level,
  main.Forecast_1_ID,
  main.Source_location_1 Forecast_2_ID,
  'AI' Record_type,
  main.Source_location_1 Source_location_1,
  Cast(1 as numeric(3,0))  Replenishment_lead_time_1,
  main.Source_location_1 Vendor,
  'DEFAULT' DRP_planner,
  'M' Make_buy_code,
  Order_multiple,
  Cast(90 as numeric(3,0)) Network_level,
  main.pdhProductFlag,
   30 Order_Quantity_Days
from 
  cdr_ai_3cii main
  left join cumulative_cdr_ai3 on main .forecast_1_id = cumulative_cdr_ai3.forecast_1_id and main.Source_location_1 = cumulative_cdr_ai3.forecast_2_id
where defaultsourcingrule is  null
 --and main.Forecast_1_ID || '-' || main.Source_location_1 || '-' ||  main.Source_location_1 not in (select distinct Forecast_1_ID || '-' || Forecast_2_ID || '-' || Source_location_1 from  cdr_ai_3bii2)
 and  cumulative_cdr_ai3.forecast_1_id is null
   """)
target_file = get_file_path(temp_folder, 'cdr_ai_3ciii')
cdr_ai_3ciii.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_3ciii = spark.read.format('delta').load(target_file)
cdr_ai_3ciii.createOrReplaceTempView('cdr_ai_3ciii')
# cdr_ai_3ciii.display()

# COMMAND ----------

cdr_ai = (
  cdr_ai_3a.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days')
  .union(cdr_ai_3bi1.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
  .union(cdr_ai_3bi2.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
  .union(cdr_ai_3bi3.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
  .union(cdr_ai_3bi4.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
  .union(cdr_ai_3bii1.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
  .union(cdr_ai_3bii2.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
  .union(cdr_ai_3ci.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
  .union(cdr_ai_3cii.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
  .union(cdr_ai_3ciii.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
)
cdr_ai.createOrReplaceTempView('cdr_ai')

# COMMAND ----------

cdr_ai_f = spark.sql("""
select distinct 
    cdr_ai.scenario,
    cdr_ai._source,
    cdr_ai.Pyramid_Level,
    cdr_ai.Forecast_1_id,
    cdr_ai.Forecast_2_id,
    cdr_ai.record_Type,
    cdr_ai.Source_Location_1,
    cdr_ai.Replenishment_lead_time_1,
    cdr_ai.Vendor,
    cdr_ai.DRP_planner,
    cdr_ai.Make_buy_code,
    cdr_ai.Order_multiple,
    max(case 
       when cdr_ai.forecast_2_id = cdr_ai.Source_Location_1 and cdr_ai.forecast_2_id  like 'DC%' then '10'
       when cdr_ai.forecast_2_id = cdr_ai.Source_Location_1 and cdr_ai.forecast_2_id  not like 'DC%' then '90'
       when l1.forecast_2_id is null then '10'
       when l1.forecast_2_id is not null and l2.forecast_2_id is null then '20'
       when l1.forecast_2_id is not null and l2.forecast_2_id is not null then '30'
    end) network_level,
    cdr_ai.pdhProductFlag,
    cdr_ai.Order_Quantity_Days
from 
  cdr_ai
  left join (select distinct 
        forecast_1_id, 
        forecast_2_id, 
        Source_Location_1 from cdr_ai) L1 on cdr_ai.forecast_1_id = L1.forecast_1_id
                             and cdr_ai.forecast_2_id = L1.Source_Location_1
  left join (select distinct 
        forecast_1_id, 
        forecast_2_id, 
        Source_Location_1 from cdr_ai) L2 on L1.forecast_1_id = L2.forecast_1_id
                             and L1.forecast_2_id = L2.Source_Location_1
group by 
cdr_ai.scenario,
    cdr_ai._source,
    cdr_ai.Pyramid_Level,
    cdr_ai.Forecast_1_id,
    cdr_ai.Forecast_2_id,
    cdr_ai.record_Type,
    cdr_ai.Source_Location_1,
    cdr_ai.Replenishment_lead_time_1,
    cdr_ai.Vendor,
    cdr_ai.DRP_planner,
    cdr_ai.Make_buy_code,
    cdr_ai.Order_multiple,
    cdr_ai.pdhProductFlag,
    cdr_ai.Order_Quantity_Days
 """)
target_file = get_file_path(temp_folder, 'cdr_ai_f')
cdr_ai_f.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_f = spark.read.format('delta').load(target_file)
cdr_ai_f.createOrReplaceTempView('cdr_ai_f')

# COMMAND ----------

# DBTITLE 1,Add source locations that are not closed
cdr_ai_closing = spark.sql("""
select distinct forecast_1_id , Source_location_1 forecast_2_id 
from cdr_ai 
where forecast_1_id || '-' || Source_location_1
  not in (select distinct forecast_1_id || '-'  || forecast_2_id from cdr_ai_f)
  and Source_location_1 <> ''
""")
cdr_ai_closing.createOrReplaceTempView('cdr_ai_closing')
# cdr_ai_closing.display()

# COMMAND ----------

cdr_ai_closing_add=spark.sql("""
select 
  '4a' scenario,
  '' _source,
  '2' Pyramid_Level,
  Forecast_1_id,
  Forecast_2_id,
  'AI' record_Type,
  Forecast_2_id Source_Location_1,
  cast(5 as numeric(3,0)) Replenishment_lead_time_1,
  Forecast_2_id Vendor,
  'DEFAULT' DRP_planner,
  'M' Make_buy_code,
  pdhProduct.ansStdUomConv Order_multiple,
  Cast(90 as numeric(3,0)) Network_level,
  '' pdhProductFlag,
  30 Order_Quantity_Days
from cdr_ai_closing
  join pdhProduct on Forecast_1_id = pdhProduct.productCode
--   order by 4
""")
# cdr_ai_closing_add.display()
cdr_ai_closing_add.createOrReplaceTempView('cdr_ai_closing_add')

# COMMAND ----------

cdr_ai_semi_final = (
  cdr_ai_f.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days')
  .union(cdr_ai_closing_add.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
)

target_file = get_file_path(temp_folder, 'cdr_ai_semi_final')
cdr_ai_semi_final.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
cdr_ai_semi_final = spark.read.format('delta').load(target_file)
cdr_ai_semi_final.createOrReplaceTempView('cdr_ai_semi_final')

# COMMAND ----------

# DBTITLE 1,60.3.2 SPS_PURCHASE_ORDER - Conversion factor
conv_from_item=spark.sql("""
select b.segment1,  uom_code, unit_of_measure, sum(nvl(conversion_rate,0)) conversion_rate
                     from ebs.MTL_UOM_CONVERSIONS a,
                     ebs.mtl_system_items_b b
                     where a.inventory_item_id = b.inventory_item_id
                     and b.organization_id = 124
                     group by uom_code, unit_of_measure, b.segment1
""")
conv_from_item.createOrReplaceTempView('conv_from_item')
# conv_from_item.display()

# COMMAND ----------

# DBTITLE 1,60.3.3 SPS_PURCHASE_ORDER - Conversion factor
conv_from = spark.sql("""select uom_code, unit_of_measure, sum(conversion_rate) conversion_rate
                     from ebs.MTL_UOM_CONVERSIONS
                     where inventory_item_id = 0
                     group by unit_of_measure, uom_code
""") 
conv_from.createOrReplaceTempView('conv_from')

# COMMAND ----------

# DBTITLE 1,60.3.4 SPS_PURCHASE_ORDER - Conversion factor
conv_to_item=spark.sql("""select  b.segment1, uom_code, unit_of_measure, sum(conversion_rate) conversion_rate
										from ebs.MTL_UOM_CONVERSIONS a,
										ebs.mtl_system_items_b b
										where a.inventory_item_id = b.inventory_item_id
										and b.organization_id = 124
										group by uom_code, unit_of_measure, b.segment1
""") 
conv_to_item.createOrReplaceTempView('conv_to_item')

# COMMAND ----------

# DBTITLE 1,60.3.5 SPS_PURCHASE_ORDER - Conversion factor
conv_to=spark.sql("""select  uom_code,unit_of_measure,
										sum(conversion_rate) conversion_rate
										from ebs.MTL_UOM_CONVERSIONS
										where inventory_item_id = 0
										group by unit_of_measure, uom_code
""") 
conv_to.createOrReplaceTempView('conv_to')

# COMMAND ----------

product_formulae=spark.sql("""
select 
        pitm.attribute15 parentAnsStdUom,
        itm.attribute15 componentAnsStdUom,
        hdr.FORMULA_NO as parent_item_ID,
        'DC' || mp.ORGANIZATION_CODE as Parent_Location,
        round(ln.LINE_NO,0) Structure_Sequence_Number,
        itm.SEGMENT1 as Component_Item_ID,
        'DC' || mp.ORGANIZATION_CODE as Component_Location,
        Pln.QTY as Parent_USAGE_RATE, 
        Pln.DETAIL_UOM as PARENT_USAGE_UOM,    
		ln.QTY as USAGE_RATE, 
        ln.DETAIL_UOM, 
        max(date_format(vr.START_DATE, 'yyyyMMdd')) as Begin_Effective_Date
--         to_char(vr.END_DATE,'YYYYMMDD') as end_effective_date,
--         hdr.FORMULA_VERS,fmtl.FORMULA_DESC1, --hdr.INACTIVE_IND,
--         ln.LINE_TYPE,  
--         itm.description, 
--         itm.ITEM_TYPE
--         decode (pitm.PLANNING_MAKE_BUY_CODE, '1','Make','2','Buy') as PARENT_MAKE_BUY_CODE,
--         decode(hdr.FORMULA_STATUS, 100, 'New', 700, 'Approved for General Use', 800, 'On Hold', 900, 'Frozen', 1000,'Obsolete/Archived') as FORMULA_STATUS
 
from    ebs.FM_FORM_MST_B hdr,    	--Formula hearder
        ebs.FM_MATL_DTL ln,       	--Line Details
        ebs.FM_MATL_DTL Pln,       	--Parent Line Detail
        ebs.FM_FORM_MST_TL fmtl,  	--Language header
        ebs.GMD_RECIPES_B gr,				--Recipes
        ebs.GMD_RECIPE_VALIDITY_RULES vr, --Validity Rules
        ebs.mtl_system_items_b itm, --Item Master ingrediant
        ebs.mtl_system_items_b Pitm, --Item Master parent
        ebs.mtl_parameters mp       --Orgs

where   hdr.formula_id = ln.formula_id  
        and hdr.formula_id = Pln.formula_id 
        and hdr.formula_id = fmtl.formula_id
        and fmtl.language = 'US'
        and hdr.FORMULA_NO = Pitm.SEGMENT1 
        and Pitm.organization_id  = mp.organization_id  
        and pitm.PLANNING_MAKE_BUY_CODE = '1'  -- '1','Make','2','Buy')
        and ln.inventory_item_id = itm.inventory_item_id -- and   ln.organization_id   = itm.organization_id
        and Pln.inventory_item_id = Pitm.inventory_item_id 
        and Pln.LINE_TYPE = '1'
        and itm.organization_id = mp.organization_id
        and gr.RECIPE_ID = vr.RECIPE_ID
	and MP.organization_id = VR.organization_id
	and hdr.formula_id = gr.formula_id   
	and hdr.FORMULA_NO = gr.Recipe_No
        and vr.RECIPE_USE = '0'  
        and ln.LINE_TYPE = '-1'
	and ln.QTY > 0
        and mp.ORGANIZATION_CODE in ('810','822','803','828','814','827','826')
        and itm.INVENTORY_ITEM_STATUS_CODE = 'Active' 
        and Pitm.INVENTORY_ITEM_STATUS_CODE = 'Active'
        and itm.ITEM_TYPE in ('FINISHED GOODS','ACCESSORIES')
        and Pitm.ITEM_TYPE in ('FINISHED GOODS','ACCESSORIES')
        and hdr.FORMULA_STATUS = '700'
        and gr.RECIPE_STATUS = '700'
        and vr.VALIDITY_RULE_STATUS = '700'
        and vr.END_DATE is null
group by
    pitm.attribute15,
    itm.attribute15,
    hdr.FORMULA_NO,
    'DC' || mp.ORGANIZATION_CODE,
    round(ln.LINE_NO,0),
    itm.SEGMENT1,
    Pln.QTY, 
    Pln.DETAIL_UOM,    
	ln.QTY, 
    ln.DETAIL_UOM
""")
# product_formulae.display()
product_formulae.createOrReplaceTempView('product_formulae')
# product_formulae.count()

# COMMAND ----------

# DBTITLE 1,IP PSL #1
df_ip_psl=spark.sql("""
 with q1 as (
  select 
    pf.*,
    nvl(parent_from_item.conversion_rate, parent_from.conversion_rate) parent_from_conversion_rate,
    nvl(parent_to_item.conversion_rate, parent_to.conversion_rate) parent_to_conversion_rate,
    nvl(component_from_item.conversion_rate, component_from.conversion_rate) component_from_conversion_rate,
    nvl(component_to_item.conversion_rate, component_to.conversion_rate) component_to_conversion_rate
  from 
    product_formulae pf
    left join conv_from_item  parent_from_item on pf.parent_item_ID = parent_from_item.segment1 
      and pf.PARENT_USAGE_UOM = parent_from_item.uom_code
    left join conv_from parent_from on pf.PARENT_USAGE_UOM = parent_from.uom_code
    left join conv_from_item  parent_to_item on pf.parent_item_ID = parent_to_item.segment1 
      and pf.parentAnsStdUom = parent_to_item.uom_code
    left join conv_from parent_to on pf.parentAnsStdUom = parent_to.uom_code

    left join conv_from_item  component_from_item on pf.Component_Item_ID = component_from_item.segment1 
      and pf.DETAIL_UOM = component_from_item.uom_code
    left join conv_from component_from on pf.DETAIL_UOM = component_from.uom_code
    left join conv_from_item  component_to_item on pf.Component_Item_ID = component_to_item.segment1 
      and pf.componentAnsStdUom = component_to_item.uom_code
    left join conv_from component_to on pf.componentAnsStdUom = component_to.uom_code
  )
  select 
    q1.Parent_Item_ID,
    q1.Parent_Location,
    q1.Structure_Sequence_Number,
    q1.Component_Item_ID,
    q1.Component_Location,
    cast(Case when PARENT_USAGE_UOM <> DETAIL_UOM then (USAGE_RATE*component_from_conversion_rate)/(Parent_USAGE_RATE*parent_from_conversion_rate)
else 1 end as decimal(20,5)) as Usage_Rate,
    100 as Popularity, 
    q1.Begin_Effective_Date,
     '20400101' as End_Effective_Date
  from q1
  union 
select
  mas.item as Parent_Item_ID,
  make.organizationCode as Parent_Location, 
  1 as Structure_Sequence_Number,
  mas.REPACKING_RELABELING_COMPONENT_CODE as Component_Item_ID, 
  make.organizationCode as Component_Location,
  1 as Usage_Rate, 
  100 as Popularity, 
  '20200101' as Begin_Effective_Date, 
  '20400101' as End_Effective_Date
from pdh.master_records mas
  join make_items make on mas.item = make.productCode
where 
  REPACKING_RELABELING_COMPONENT_CODE is not null 
  AND make.productStatus = 'Active'
  and mas.REPACKING_RELABELING_COMPONENT_CODE || '-' || make.organizationCode  in (select distinct key from all_active_items )
union
select prd.e2eProductCode as Parent_Item_ID,
sps.LOC_INDX as Parent_Location,
'1' as Structure_Sequence_Number,
prd.rePackingCode as Component_Item_ID,
sps.LOC_INDX as Component_Location,
'1' as Usage_Rate,
'100' as Popularity,
date_format(CURRENT_DATE-1, 'yyyyMMdd')  as Begin_Effective_Date,
date_format(last_day(CURRENT_DATE+547), 'yyyyMMdd') as End_Effective_Date --18 months
from pdhProduct prd, g_tembo.sps_item_location sps 
where  prd.e2eProductCode = sps.ITEM_INDX 
and prd.rePackingCode not in ('unknown','N/A','null')
and sps.LOC_INDX in ('DCANV1','DCANV2','DCANV3','DCANV4','DCANV6','DCNLT1','DCAAL1','DCAAL2','DCAAL3','DCAJN1')
and  prd.productStatus = 'Active'
""")
df_ip_psl.createOrReplaceTempView('df_ip_psl')
# df_ip_psl.display()

# COMMAND ----------

last_effective_date_psl=spark.sql("""
select
  Parent_Item_ID,
  Parent_Location,
  max(Begin_Effective_Date) Begin_Effective_Date
from 
  df_ip_psl  
group by 
  Parent_Item_ID,
  Parent_Location
""")
last_effective_date_psl.createOrReplaceTempView('last_effective_date_psl')
# last_effective_date_psl.display()

# COMMAND ----------

df_ip_psl_max=spark.sql("""
select distinct
  df_ip_psl.Parent_Item_ID,
  df_ip_psl.Parent_Location, 
  df_ip_psl.Structure_Sequence_Number,
  df_ip_psl.Component_Item_ID, 
  df_ip_psl.Component_Location,
  df_ip_psl.Usage_Rate, 
  df_ip_psl.Popularity, 
  last_effective_date_psl.Begin_Effective_Date, 
  df_ip_psl.End_Effective_Date
from
  df_ip_psl
  join last_effective_date_psl on df_ip_psl.Parent_Item_ID = last_effective_date_psl.Parent_Item_ID
   and df_ip_psl.Parent_Location =last_effective_date_psl.Parent_Location
""")
df_ip_psl_max.createOrReplaceTempView('df_ip_psl_max')
# last_effective_date_psl.display()

# COMMAND ----------

# DBTITLE 1,IP PSL #2
#IP Interface
file_name = get_file_name('tmp_ip_psl', 'dlt')
target_file = get_file_path(temp_folder, file_name)

print(file_name)
print(target_file)
df_ip_psl_max.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,SP Product Structure #1
df_sp_psl=spark.sql("""
  select distinct
    df_ip_psl.Parent_Item_ID,
    df_ip_psl.Parent_Location, 
    df_ip_psl.Component_Item_ID,
    'REPACK_' || df_ip_psl.Parent_Location  as Resource,
    df_ip_psl.Usage_Rate, 
    5 as Lead_Time_Days,
    df_ip_psl.Begin_Effective_Date, 
    df_ip_psl.End_Effective_Date
from 
  df_ip_psl_max df_ip_psl

""")
df_sp_psl.createOrReplaceTempView('df_sp_psl')
# df_sp_psl.display()

# COMMAND ----------

# DBTITLE 1,SP Product Structure #2
#SP Interface
file_name = get_file_name('tmp_sps_product_structure', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)
df_sp_psl.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

###############################################################################################################################################################################################################################################################################
#                                                                                               S P
###############################################################################################################################################################################################################################################################################

# COMMAND ----------

# DBTITLE 1,60.0.1 Get base selection for SP Customer Order
main_60_0_1 = spark.sql("""
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
    --when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion in ('BR') then 'BR'
    when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.subRegion not in ('MX') and oh._source = 'EBS' then 'OLAC' 
    else  nvl(soldtoTerritory.subRegion, '#Error8-' || soldtoTerritory.territoryCode) 
  end subRegion,
  coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, 'DC' || inv.organizationcode)  DC ,
  orderType.dropShipmentFlag Channel,
  soldtoTerritory.territoryCode,
  soldtoTerritory.subRegionGis,
  case when nvl(soldto.siteCategory,'X') = 'LA' then 'LAC' else nvl(soldtoTerritory.Region, '#Error14') end Region,
  --case when nvl(soldto.siteCategory,'X') = 'LA' and soldtoTerritory.forecastArea not in ('MX','CO') and oh._source = 'EBS' then 'OLAC' else soldtoTerritory.forecastArea end forecastArea,
  ol._SOURCE,
  oh.orderNumber,
  case 
    when pr_org.mtsMtoFlag is null 
      then 'MTS' 
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
  then 
    (ol.quantitycancelled  * ol.ansStdUomConv)
  else
    (nvl(ol.quantityOutstandingOrders,0) + nvl(ol.quantityFutureOrders,0) + nvl(ol.quantityBackordered,0)) * ol.ansStdUomConv
  end quantityOutstandingOrders,
  case when ol.orderlinestatus like '%CANCELLED%'
    then ol.cancelledAmount / nvl(oh.exchangeRateUsd,1)
  else
    (ol.orderAmount / nvl(oh.exchangeRateUsd,1) )
  end orderAmountUsd,
  nvl(acc.customerTier, 'No Tier') customerTier,
  case when ol.requestDeliveryBy < current_date then year(add_months(current_date, -1)) else year(ol.requestDeliveryBy) end yearRequestDeliveryBy,
  case when ol.requestDeliveryBy < current_date then month(add_months(current_date, -1)) else month(ol.requestDeliveryBy) end monthRequestDeliveryBy,
  1 orderLines,
  pr_org.productStatus regionalProductStatus,
  ol.lineNumber,
  case when ol.requestDeliveryBy < current_date then  LAST_DAY(add_months(current_date, -1)) else ol.requestDeliveryBy end requestDeliveryBy,
  ol.pricePerUnit / exchangeRateUsd / nvl(ol.ansStdUomConv,1) pricePerUnitUSD,
  nvl(inv.region,concat('#Error49','-',coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, 'DC' || inv.organizationcode))) orgRegion,
  nvl(ol.createdOn,oh.orderDate) createdOn,
  ol.promiseDate,
  nvl(left(acc.name, 40), 'CUST01') accountName
from 
  s_supplychain.sales_order_lines_agg ol
  join s_supplychain.sales_order_headers_agg oh on ol.salesorder_id = oh._ID
  join s_core.product_agg pr on ol.item_ID = pr._ID
  join s_core.account_agg acc on oh.customer_ID = acc._ID
  left join s_core.customer_location_agg soldto on oh.soldtoAddress_ID = soldto._ID
  join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
  join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
  join s_core.transaction_type_agg orderType on oh.orderType_ID = orderType._id  
  left join sub_region_override on nvl(soldto.partySiteNumber, soldto.addressid) = sub_region_override.partySiteNumber
  left join s_core.territory_agg soldtoTerritory on nvl(sub_region_override.country_ID, soldto.territory_ID) = soldtoTerritory._id  
  left join s_core.account_organization_agg acc_org on oh.customerOrganization_ID = acc_org._id
  left join regionalproductstatus  pr_org on pr.ProductCode = pr_org.e2eProductCode
                                    and ol.inventoryWarehouse_ID = pr_org.organization_ID  
where
  not ol._deleted
  and not oh._deleted
  and oh._source  in  (select sourceSystem from source_systems) 
  and inv.isActive
  and coalesce(ol.e2eDistributionCentre, inv.commonOrganizationCode, inv.organizationcode) not in ('DC325', '000', '250','325', '355', '811', '821', '802', '824', '825', '832', '834')
 -- and year(ol.requestDeliveryBy) * 100 + month(ol.requestDeliveryBy)  >=  (select runPeriod from run_period)
  and (nvl(orderType.e2eFlag, true) or ol.e2eDistributionCentre = 'DC827'
    or CONCAT(orderType.transactionId , '-' , nvl(UPPER(pr.packagingCode),'')) in (select CONCAT(KEY_VALUE,'-',CVALUE)
                                                                                 from smartsheets.edm_control_table 
                                                                                 WHERE table_id = 'ADDITIONAL_ORDER_TYPE_PACK_TYPE')
       )
  and nvl(pr.itemType, 'X') not in  ('Service')
  and (nvl(ol.orderlinestatus, 'Include') not like ('%CANCELLED%')
    --or cancelReason like '%COVID-19%' 
      or cancelReason like '%PLANT DIRECT BILLING%'
      or cancelReason like '%Plant Direct Billing Cancellation%')
  and nvl(pr.productdivision, 'Include') not in ('SH&WB')
  and nvl(acc.customerType, 'External') not in ('Internal')
  and oh.customerId is not null
  and pr.productCode not like 'PALLET%'
  and pr.itemType in ('FINISHED GOODS', 'ACCESSORIES','ZPRF')
  and orderType.name not like 'AIT_DIRECT SHIPMENT%'
  and ol.bookedFlag = 'Y'
  and ol.orderlineStatus not in ('')
  and OH._SOURCE || '-' || upper(nvl(pr_org.productStatus,'X')) not in ('EBS-INACTIVE', 'EBS-OBSOLETE', 'KGD-INACTIVE', 'KGD-OBSOLETE','TOT-INACTIVE', 'TOT-OBSOLETE','COL-INACTIVE', 'COL-OBSOLETE','SAP-INACTIVE')
  and org.organizationcode not in (select organization from exclude_organizations)
  and ol.reasonForRejection is null
  and nvl(ol.orderlineholdtype,'Logility Hold') = 'Logility Hold'
  and oh.orderHoldType is null
  and orderLineStatus not in ('CLOSED')
 -- and oh.ordernumber = '1811140'
 --and oh.ordernumber = '107319'
 --  and pr.productcode = '826903'
  and nvl(ol.cancelledFlag,'N') = 'N' 
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
 -- Region,
 -- forecastArea,
  _SOURCE,
  orderNumber,
  mtsMtoFlag,
  sum(quantityOrdered) quantityOrdered,
  sum(quantityOutstandingOrders) quantityOutstandingOrders,
  sum(orderAmountUsd) orderAmountUsd,
  customerTier,
  yearRequestDeliveryBy,
  monthRequestDeliveryBy,
  sum(orderlines) orderlines,
  regionalProductStatus,
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  orgRegion,
  min(createdOn) createdOn, 
  min(promiseDate) promiseDate,
  accountName
from MAIN_1
where quantityOutstandingOrders > 0
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
--  Region,
--  forecastArea,
  _SOURCE,
  orderNumber,
  mtsMtoFlag,
  customerTier,
  yearRequestDeliveryBy,
  monthRequestDeliveryBy,
  regionalProductStatus,
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  orgRegion,
  accountName
""")
  
main_60_0_1.createOrReplaceTempView('main_60_0_1')

# COMMAND ----------

# DBTITLE 1,60.0.2  Find substitution if available
main_60_0_2 = spark.sql("""
select 
  main._SOURCE,
  --case when nvl(HistoricalProductSubstitution.final_successor, main.productCode) = ''
  --  then '#Error37'
  --else --nvl(HistoricalProductSubstitution.final_successor, main.productCode) end productCode,
   coalesce(main_mpx_l0.SuccessorCode, HistoricalProductSubstitution.final_Successor, main.productCode) productCode,
  case when HistoricalProductSubstitution.final_successor is not null
    then Predecessor_of_Final_successor
  end predecessorCode,
  main._SOURCE,
  nvl(main.subRegion, '#Error8-' || main.territoryCode) subRegion,
  nvl(case when main.DC in ('DC813') then 'DC822' else main.DC end, '#Error9') DC,
  nvl(main.channel, '#Error10') channel,
  --nvl(main.region, '#Error14') region,
  nvl(main.subRegionGis, '#Warning15') subRegionGis,
  main.customerGbu,
  main.forecastGroup,
  main.Forecast_Planner,
 -- main.forecastArea,
  main.orderNumber,
  main.mtsMtoFlag,
  main.quantityOrdered,
  main.quantityOutstandingOrders,
  main.orderAmountUsd,
  main.yearRequestDeliveryBy,
  main.monthRequestDeliveryBy,
  main.orderlines,
  main.customerTier,
  main.key,
  regionalProductStatus,
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  orgRegion,
  createdOn,
  promiseDate,
  main.accountName
from 
   main_60_0_1 main
   left join HistoricalProductSubstitution on main.key =   HistoricalProductSubstitution.key
   left join (select distinct predecessorCode, DC, SuccessorCode from main_mpx_l0) main_mpx_l0 on main.productCode = main_mpx_l0.predecessorCode and main.DC = main_mpx_l0.DC
where nvl(HistoricalProductSubstitution.final_successor, 'Include') not in ('dis')
""")
  
main_60_0_2.createOrReplaceTempView('main_60_0_2')

# COMMAND ----------

# DBTITLE 1,60.0.3. replace GTC product code with PDH product Code
main_60_0_3 = spark.sql("""
select 
  main_2._SOURCE,
  nvl(gtc_replacement.item,main_2.productCode) productCode,
  main_2.predecessorCode,
  main_2.subRegion,
 -- case
 --   when main_2.subRegion = 'UK-E' and main_2.DC = 'DCNLT1'
 --       then main_2.DC
 --   when main_2.subRegion = 'UK-E' and main_2.DC <> 'DCNLT1'  and _source = 'SAP' then main_2.DC 
 --   when main_2.subRegion <> 'UK-E' and main_2.DC = 'DCNLT1'  and _source = 'SAP' then 'DCANV1'
 --   else main_2.DC
 -- end DC,
  main_2.DC,
  main_2.channel,
 -- main_2.region,
  main_2.subRegionGis,
  main_2.customerGbu,
  main_2.forecastGroup,
  main_2.Forecast_Planner,
--  main_2.forecastArea,
  main_2.orderNumber,
  main_2.mtsMtoFlag,
  main_2.quantityOrdered,
  main_2.quantityOutstandingOrders,
  main_2.orderAmountUsd,
  main_2.yearRequestDeliveryBy,
  main_2.monthRequestDeliveryBy,
  main_2.customerTier,
  case when gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID is null then false else true end gtcReplacementFlag,
  gtc_replacement.ITEM_STATUS,
  main_2.orderlines,
  regionalProductStatus,
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  orgRegion,
  createdOn,
  promiseDate,
  main_2.accountName
from
  main_60_0_2 main_2
  left join gtc_replacement on main_2.productCode = gtc_replacement.ORACLE_PRODUCT_ID_OR_GTC_ID
where 
  nvl(gtc_replacement.ITEM_STATUS, 'Active') not in ('Inactive')
""")
main_60_0_3.createOrReplaceTempView('main_60_0_3')

# COMMAND ----------

# DBTITLE 1,60.0.4. Add Product data from source systems and PDH data - Current
main_60_0_4 = spark.sql("""
select 
  main_2._SOURCE,
  main_2.productCode,
  main_2.predecessorCode,
  main_2.subRegion,
  main_2.DC,
  main_2.channel,
 -- main_2.region,
  main_2.subRegionGis,
  main_2.customerGbu,
  main_2.forecastGroup,
  main_2.Forecast_Planner,
 -- main_2.forecastArea,
  main_2.orderNumber,
  main_2.mtsMtoFlag,
  main_2.quantityOrdered,
  main_2.quantityOutstandingOrders,
  main_2.orderAmountUsd,
  main_2.yearRequestDeliveryBy,
  main_2.monthRequestDeliveryBy,
  case when left(pdhProduct.productStyle,1) = '0' then substr(pdhProduct.productStyle, 2, 50) else pdhProduct.productStyle end productStylePdh,
  case when pdhProduct.productCode is null then '#Error1' end pdhProductFlag,
  left(nvl(pdhProduct.e2eItemDescription, pdhProduct.name),36) description,
  Cast(pdhProduct.caseVolume as numeric(9,4)) Unit_cube,
  Cast(pdhProduct.caseGrossWeight as numeric(9,4)) Unit_weight,
  nvl(left(pdhProduct.sizeDescription, 40), ' NO DATA')  sizeDescription,
  nvl(left(pdhProduct.gbu, 40), '#Error5' ) productGbu,
  nvl(left(pdhProduct.productSbu, 40), '#Error6' ) productSbu,
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
  lineNumber,
  requestDeliveryBy,
  pricePerUnitUSD,
  pdhProduct.packagingCode,
  pdhProduct.baseProduct,
  orgRegion,
  main_2.createdOn,
  promiseDate,
  main_2.accountName
from
  main_60_0_3 main_2
  left join pdhProduct on main_2.productCode = pdhProduct.productCode 
  join locationmaster lm on main_2.DC = lm.globallocationcode
where
  1=1
""")
main_60_0_4.createOrReplaceTempView('main_60_0_4')

# COMMAND ----------

# DBTITLE 1,60.1 SPS_CUSTOMER_ORDER
SPS_CUSTOMER_ORDER = spark.sql("""
with q1 as
(select
  'Order' source,
  productCode Item_ID,
  dc Location_ID,
  orderNumber Customer_Order_ID,
  round(lineNumber,0) Customer_Order_Line_Number,
  dc || '-F' Flow_Resource_ID,
  requestDeliveryBy Order_Ship_Date,
  accountName Customer_Location_ID,
  quantityOrdered Customer_Order_Quantity,
  0 Order_Ship_Quantity,
  nvl(pricePerUnitUSD,0) Selling_Price_Value,
  0.01 Objective_Backorder_Penalty_Factor,
  0 Backorder_Allowed_Code,
  case
    when channel = 'DS' then 0
    else 1
  end Order_Split_Indicator,
  accountName Cust_Index,
  date_format(createdOn, 'yyyyMMdd') User_Text_1,
  nvl(date_format(promiseDate, 'yyyyMMdd'), '')  User_Text_2
  --'' User_Text_1,
  --'' User_Text_2
from
  main_60_0_4
where quantityOrdered > 0
UNION ALL
SELECT
  'Shipment' source,
  productCode Item_ID,
  dc Location_ID,
  ordernumber Customer_Order_ID,
  round(linenumber,0) Customer_Order_Line_Number,
  dc || '-F' Flow_Resource_ID,
  case when requestDeliveryBy < last_day(add_months(current_date, -1)) 
    then last_day(add_months(current_date, -1)) 
  else requestDeliveryBy 
  end order_ship_date,
  accountName Customer_Location_ID,
  quantityShipped Customer_Order_Quantity,
  quantityShipped Order_Ship_Quantity,
  nvl(pricePerUnitUsd,0) Selling_Price_Value,
  0.01 Objective_Backorder_Penalty_Factor,
  0 Backorder_Allowed_Code,
  case
    when channel = 'DS' then 0
    else 1
  end Order_Split_Indicator,
  accountName Cust_Index,
  '' User_Text_1,
  '' User_Text_2
FROM
main_20_3
  where
    quantityShipped > 0
  and year(requestDeliveryBy) * 100 + month(requestDeliveryBy) =   (select runPeriod from run_period))
    
select
  Item_ID,
  Location_ID,
  Customer_Order_ID,
  Customer_Order_Line_Number,
  Flow_Resource_ID,
  case 
    when 
      q1.Order_Ship_Date < current_date 
       then current_date - 1
    else
      q1.Order_Ship_Date
  end Order_Ship_Date,
  Customer_Location_ID,
  sum(Customer_Order_Quantity) Customer_Order_Quantity,
  sum(Order_Ship_Quantity) Order_Ship_Quantity,
  Selling_Price_Value,
  Objective_Backorder_Penalty_Factor,
  Backorder_Allowed_Code,
  Order_Split_Indicator,
  Cust_Index,
  User_Text_1,
  User_Text_2
from 
  q1
where 
   year(q1.Order_Ship_Date) * 100 + month(q1.Order_Ship_Date) <= (select toPeriod from latestOrderPeriod)
group by
  Item_ID,
  Location_ID,
  Customer_Order_ID,
  Customer_Order_Line_Number,
  Flow_Resource_ID,
  Order_Ship_Date,
  Customer_Location_ID,
  Selling_Price_Value,
  Objective_Backorder_Penalty_Factor,
  Backorder_Allowed_Code,
  Order_Split_Indicator,
  Cust_Index,
  User_Text_1,
  User_Text_2
having
  sum(Customer_Order_Quantity) > 0
  or sum(Order_Ship_Quantity) > 0
""")
SPS_CUSTOMER_ORDER.createOrReplaceTempView('SPS_CUSTOMER_ORDER')

# COMMAND ----------

#SP Customer Order Interface
file_name = get_file_name('tmp_sps_customer_order', 'dlt')
target_file = get_file_path(temp_folder, file_name)

print(file_name)
print(target_file)
SPS_CUSTOMER_ORDER.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

sps_production_order = spark.sql("""
with q1 as 
(SELECT
    pitm.segment1 AS Item_ID,
    'DC' || org.organization_code   AS Location_ID,
    hdr.batch_no AS Production_Order_ID,
    round(pln.line_no,0) AS Production_Order_Line_Number,
    hdr.due_date AS Order_Due_Date ,
    'REPACK_DC' || org.organization_code AS production_resource_id, --this must either come from the Capaciy Master or default a simple resource ID for all Warehouse REpack that us unlimited capacity
    decode(hdr.batch_status,  '1', '1', '2', '3',  '3', '5') AS Production_Order_Status_Code,
    pln.plan_qty Current_Production_Order_Quantity

FROM
    ebs.gme_batch_header     hdr,
    ebs.mtl_system_items_b   pitm,
    ebs.mtl_parameters       org,
    ebs.gme_material_details pln
WHERE
        hdr.organization_id = org.organization_id
    AND hdr.batch_id = pln.batch_id
    AND pln.inventory_item_id = pitm.inventory_item_id
    AND pln.organization_id = pitm.organization_id
    AND org.organization_code IN ( '803', '822', '804', '823', '400',
                                   '403', '828', '826', '514' )
    AND hdr.batch_status IN ( 1, 2 ) -- 1=PEND, 2=RELEASED/WIP, 3=COMPLETE
    AND pln.line_type = '1'
ORDER BY
    org.organization_code,
    hdr.batch_no)
SELECT  
    Item_ID,
    Location_ID,
    Production_Order_ID,
    Production_Order_Line_Number,
    Order_Due_Date,
    production_resource_id,
    Production_Order_Status_Code,
    Current_Production_Order_Quantity
from 
  q1
  join locationmaster lm on Location_ID = lm.globalLocationCode
""")
# SPS_PRODUCTION_ORDER.display()
sps_production_order.createOrReplaceTempView('sps_production_order')
# sps_production_order.display()

# COMMAND ----------

#SP Production Order Interface
file_name = get_file_name('tmp_sps_production_order', 'dlt')
target_file = get_file_path(temp_folder, file_name)

print(file_name)
print(target_file)
sps_production_order.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)


# COMMAND ----------

# DBTITLE 1,60.3.1 SPS_PURCHASE_ORDER - External Supplier Codes
# EBS_supplier_codes=spark.sql("""
# select 'EBS' _SOURCE, EBSSUPPLIERCODE supplierNumber, min(GlobalLocationCode)  GlobalLocationCode
#   from locationmaster 
# where 
#   nvl(EBSSUPPLIERCODE, 'N/A') not in ('N/A')
#   and GlobalLocationCode like 'EM%'
# group by 
#   EBSSUPPLIERCODE
# """)
# EBS_supplier_codes.createOrReplaceTempView('EBS_supplier_codes')

# COMMAND ----------

location_codes=spark.sql("""

select 'SAP' _SOURCE, 
  SAPGOODSSUPPLIERCODE supplierNumber, 
  min(GlobalLocationCode)  GlobalLocationCode,
  OrderLeadTime
from locationmaster 
where 
  nvl(SAPGOODSSUPPLIERCODE, 'N/A') not in ('N/A')
group by 
  SAPGOODSSUPPLIERCODE,
  OrderLeadTime
  
union

select 'EBS' _SOURCE, 
  split(replace(EBSSUPPLIERCODE, ' ', ','),',')[0] supplierNumber,
  min(GlobalLocationCode)  GlobalLocationCode,
  OrderLeadTime
from locationmaster 
where 
  nvl(EBSSUPPLIERCODE, 'N/A') not in ('N/A')
group by 
  EBSSUPPLIERCODE,
  OrderLeadTime
  
union 

select 'EBS' _SOURCE, 
  split(replace(EBSSUPPLIERCODE, ' ', ','),',')[1] supplierNumber,
  min(GlobalLocationCode)  GlobalLocationCode,
  OrderLeadTime
from locationmaster 
where 
  nvl(EBSSUPPLIERCODE, 'N/A') not in ('N/A')
  and split(replace(EBSSUPPLIERCODE, ' ', ','),',')[1] is not null
group by 
  EBSSUPPLIERCODE,
  OrderLeadTime 
  
union

select 'KGD' _SOURCE, 
  KINGDEESUPPLIERCODE supplierNumber, 
  min(GlobalLocationCode)  GlobalLocationCode,
  OrderLeadTime
from locationmaster 
where 
  nvl(KINGDEESUPPLIERCODE, 'N/A') not in ('N/A')
group by 
  KINGDEESUPPLIERCODE,
  OrderLeadTime
  
union

select 'TOT' _SOURCE, 
  MICROSIGASUPPLIERCODE supplierNumber, 
  min(GlobalLocationCode)  GlobalLocationCode,
  OrderLeadTime
from locationmaster 
where 
  nvl(MICROSIGASUPPLIERCODE, 'N/A') not in ('N/A')
group by 
  MICROSIGASUPPLIERCODE,
  OrderLeadTime

union

select 'COL' _SOURCE, 
  DYNAMICSNAVSUPPLIERCODECOLOMBIA supplierNumber, 
  min(GlobalLocationCode)  GlobalLocationCode,
  OrderLeadTime
from locationmaster 
where 
  nvl(DYNAMICSNAVSUPPLIERCODECOLOMBIA, 'N/A') not in ('N/A')
group by 
  DYNAMICSNAVSUPPLIERCODECOLOMBIA,
  OrderLeadTime
  
""")
location_codes.createOrReplaceTempView('location_codes')

# COMMAND ----------

ebs_suppliers_number=spark.sql("""
select distinct
  vendor_site_code, sup.SEGMENT1 AS supplierNumber
from 
  EBS.AP_SUPPLIER_SITES_ALL sus
  join ebs.AP_SUPPLIERS sup on sus.vendor_id = sup.vendor_id
""")
ebs_suppliers_number.createOrReplaceTempView('ebs_suppliers_number')

# COMMAND ----------

ds1=spark.sql("""
SELECT DISTINCT
         h.order_number,
         l.line_number                  SO_Line_number,
         ottt.name,
         ph.creation_date,
         ph.segment1                    PO_Number,
         pl.line_num                    PO_Line_Number,
         MTLI.SEGMENT1                  ITEM,
         MTLI.Primary_UOM_Code,
         MTLI.secondary_uom_code,
         PL.ITEM_DESCRIPTION,
         ph.authorization_status,
         ph.closed_date,
         ph.closed_code,
         pl.closed_code                 LINE_CLOSED_CODE,
         prh.interface_source_code,
         prh.segment1                   Requisition_number,
         prl.line_num                   Requisition_line_number,
         mtli.attribute15               ansell_std_uom,    
         pll.quantity quantity_ordered,  --(pl.UNIT_MEAS_LOOKUP_CODE , mtli.attribute15 )
         pll.quantity_received quantity_received,
         pll.quantity_billed   quantity_billed,
         org.organization_code,
         cast(pll.need_by_date as date) need_by_date, 
         cast(pll.promised_date as date) promised_date,
         ph.vendor_site_id,
         pv.vendor_name,
         CASE
             WHEN org.organization_code = '400' OR (org.organization_code = '325' AND ottt.name = 'CA_Drop Shipment Order')
             THEN 'DSNA'
             WHEN org.organization_code IN ('514', '518')OR (org.organization_code = '325' AND (ottt.name = 'MX_Drop Shipment Order' OR ottt.name='US_HP_LA_Drop Shipment Order'))
             THEN 'DSLAC'
             WHEN org.organization_code = '803' OR (org.organization_code = '325' AND ottt.name = 'US_HP_Drop Shipment Order')
             THEN 'DSNA'
             WHEN org.organization_code = '724' OR (org.organization_code = '325' AND ottt.name = 'IN_PP_Drop Shipment Order')
             THEN 'DSAPAC' 
             WHEN org.organization_code = '325' AND ottt.name = 'ASIA_Drop Shipment Order'
             THEN 'DSAPAC'
             ELSE
                 NULL
         END
             AS Channel_ID,
         pl.UNIT_MEAS_LOOKUP_CODE orderUom,
         mtli.attribute15 ansstduom,
         pv.vendor_id,
         pv.segment1 supplier_number,
          pll.last_update_date
    FROM ebs.OE_DROP_SHIP_SOURCES        ods,
         ebs.oe_order_headers_all        h,
         ebs.oe_transaction_types_tl     ottt,
         ebs.oe_order_lines_all          l,
         ebs.po_line_locations_all       pll,
         ebs.po_lines_all                pl,
         ebs.po_headers_all              ph,
         ebs.po_requisition_headers_all  prh,
         ebs.po_requisition_lines_all    prl,
         ebs.MTL_SYSTEM_ITEMS_B          mtli,
         ebs.mtl_parameters org,
         ebs.ap_suppliers            pv
   WHERE     h.header_id = l.header_id
         AND h.header_id = ods.header_id
         AND pl.item_id = mtli.inventory_item_id
         AND ods.line_location_id = pll.line_location_id
         AND ods.po_header_id = ph.po_header_id
         AND ods.po_line_id = pl.po_line_id
         AND ph.po_header_id = pl.po_header_id
         AND prh.requisition_header_id = ods.requisition_header_id
         AND prl.requisition_line_id = ods.requisition_line_id
         AND prh.requisition_header_id = prl.requisition_header_id
         AND PH.CLOSED_CODE NOT LIKE '%CLOSED%'
         AND pl.closed_code NOT LIKE '%CLOSED%'
         AND org.organization_id = Pll.Ship_To_Organization_Id
         AND pv.vendor_id = ph.vendor_id
         AND ods.line_id = l.line_id
         AND org.organization_id = mtli.organization_id
         AND ottt.transaction_type_id = h.order_type_id
         AND h.order_number < '8000000'
         AND org.organization_code NOT IN ('450','553','502','800')
       --  and ph.segment1 like '%15225608%'
       --  and mtli.segment1 = '107203'
ORDER BY ORDER_NUMBER
""")
ds1.createOrReplaceTempView('ds1')

# COMMAND ----------

ds2=spark.sql("""
select 
    order_number,
    SO_Line_number,
    name,
    creation_date,
    PO_Number,
    PO_Line_Number,
    ITEM,
    Primary_UOM_Code,
    secondary_uom_code,
    ITEM_DESCRIPTION,
    authorization_status,
    closed_date,
    closed_code,
    LINE_CLOSED_CODE,
    interface_source_code,
    Requisition_number,
    Requisition_line_number,
    quantity_ordered * nvl(conv_from_item.conversion_rate, conv_from.conversion_rate) / nvl(conv_to_item.conversion_rate, conv_to.conversion_rate) quantity_ordered,
    quantity_received * nvl(conv_from_item.conversion_rate, conv_from.conversion_rate) / nvl(conv_to_item.conversion_rate, conv_to.conversion_rate) quantity_received,
    quantity_billed * nvl(conv_from_item.conversion_rate, conv_from.conversion_rate) / nvl(conv_to_item.conversion_rate, conv_to.conversion_rate) quantity_billed,
    need_by_date,
    promised_date,
    vendor_site_id,
    vendor_name,
    Channel_ID,
    supplier_Number supplierNumber
  from 
  ds1
  left join conv_from_item on ds1.item = conv_from_item.segment1
    and ds1.orderUom = conv_from_item.unit_of_measure
  left join conv_from on ds1.orderUom = conv_from.unit_of_measure
  left join conv_to_item on ds1.item = conv_to_item.segment1
    and ds1.ansstduom = conv_to_item.uom_code
  left join conv_to on ds1.ansstduom = conv_to.uom_code
""")
ds2.createOrReplaceTempView('ds2')

# COMMAND ----------

ds3=spark.sql("""
select 
  ds.ITEM Item_ID,
  ds.channel_Id Location_ID,
  ds.PO_Number PO_Order_ID,
  ds.PO_Line_Number,
  ds.channel_Id || '-F' Flow_Resource_ID,
  coalesce(location_codes.GlobalLocationCode,  '#Error27-' || ds.supplierNumber) Vendor_Location_ID,
  coalesce(location_codes.GlobalLocationCode || '-F', '#Error27-' || ds.supplierNumber)  Vendor_Flow_Resource_ID,
  nvl(ds.promised_date, ds.need_by_date) Order_Due_Date,
  '0' Order_Status_Code,
  ds.quantity_ordered - ds.quantity_received Order_Quantity,
  0 Ship_Quantity,
  0 Received_Quantity,
  ds.PO_Number User_Text_1,
  ds.need_by_date PO_Ship_Date,
  '0' Pseudo_Indicator,
  ds.promised_date Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  ds2 ds
  left join location_codes on ds.supplierNumber = location_codes.supplierNumber
  join pdhproduct on ds.Item = pdhProduct.productCode
where 
  nvl(location_codes._source,'EBS') ='EBS'
 and coalesce(location_codes.GlobalLocationCode,  '#Error27-' || ds.supplierNumber)  like 'EM%'
 and ds.quantity_ordered - ds.quantity_received > 0
 and ds.channel_Id is not null
""")
ds3.createOrReplaceTempView('ds3')

# COMMAND ----------

ds4=spark.sql("""
select 
  ds.ITEM Item_ID,
  ds.channel_Id Destination_Location_ID,
  ds.PO_Number Transfer_Order_ID,
  ds.PO_Line_Number Transfer_Order_Line_Number,
  coalesce(location_codes.GlobalLocationCode,  '#Error27-' || ds.supplierNumber)  Source_Location_ID,
  coalesce(location_codes.GlobalLocationCode || '-F', '#Error27-' || ds.supplierNumber)  Source_Flow_Resource,
  ds.channel_Id || '-F'  Destination_Flow_Resource,
  nvl(ds.promised_date, ds.need_by_date) Order_Due_Date,
  ds.need_by_date  Order_Ship_Date,
  '0' Order_Status_Code,
  ds.quantity_ordered - ds.quantity_received  Order_Quantity,
  0 Ship_Quantity,
  0 Received_Quantity,
  ds.PO_Number User_Text_1,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  ds2 ds
  left join location_codes on ds.supplierNumber = location_codes.supplierNumber
  join pdhproduct on ds.Item = pdhProduct.productCode
where
  nvl(location_codes._source,'EBS') ='EBS'
  and coalesce(location_codes.GlobalLocationCode,  '#Error27-' || ds.supplierNumber)  like 'PL%'
  and ds.quantity_ordered - ds.quantity_received > 0
  and ds.channel_Id is not null
""")
ds4.createOrReplaceTempView('ds4')

# COMMAND ----------

po1=spark.sql("""
with q1 as
(
select 
   ITEM_NUMBER Item_ID,
   'DC' || INVENTORY_ORG Location_ID,
   case 
     when instr(ORDER_NUMBER,'(') > 0 then left(ORDER_NUMBER, instr(ORDER_NUMBER,'(')-1)
     when instr(ORDER_NUMBER,'.') = 0 then ORDER_NUMBER
    else left(ORDER_NUMBER, instr(ORDER_NUMBER,'.')-1)
   end PO_Order_ID,
   case when instr(ORDER_NUMBER,'-') = 0 then 1
     else trim(substr(ORDER_NUMBER, instr(ORDER_NUMBER,'-')+1,10))
   end PO_Line_Number,
   'DC' || INVENTORY_ORG || '-F' Flow_Resource_ID,
   DUE_DATE Order_Due_Date,
   '0' Order_Status_Code,
   cast(PO_QTY_STD_UOM as decimal(32,2)) Order_Quantity,
   0 Ship_Quantity,
   0 Received_Quantity,
   coalesce(retd, cetd, DUE_DATE) PO_Ship_Date,
   '0' Pseudo_Indicator,
   DUE_DATE Available_Inventory_Date,
   retd,
   cetd,
   ebs_suppliers_number.supplierNumber 
from  
  g_fin_qv.WC_INTRANSIT_EXTRACT_FS
  join ebs_suppliers_number on WC_INTRANSIT_EXTRACT_FS.supplier_site_code = ebs_suppliers_number.vendor_site_code
where 
  order_type_text in ('ON ORDER')
--  and source_org_code is  null
  and transaction_date = (select max(transaction_date) from g_fin_qv.WC_INTRANSIT_EXTRACT_FS )
  and not _deleted
  and supplier_site_code not like 'PRD%'
   
union

select 
   ITEM_NUMBER Item_ID,
   'DC' || INVENTORY_ORG Location_ID,
   case 
     when instr(ORDER_NUMBER,'(') > 0 then left(ORDER_NUMBER, instr(ORDER_NUMBER,'(')-1)
     when instr(ORDER_NUMBER,'.') = 0 then ORDER_NUMBER
    else left(ORDER_NUMBER, instr(ORDER_NUMBER,'.')-1)
   end PO_Order_ID,
   case when instr(ORDER_NUMBER,'-') = 0 then 1
     else trim(substr(ORDER_NUMBER, instr(ORDER_NUMBER,'-')+1,10))
   end PO_Line_Number,
   'DC' || INVENTORY_ORG || '-F' Flow_Resource_ID,
   DUE_DATE Order_Due_Date,
   '0' Order_Status_Code,
   cast(PO_QTY_STD_UOM as decimal(32,2))  Order_Quantity,
   0 Ship_Quantity,
   0 Received_Quantity,
   coalesce(retd, cetd, DUE_DATE) PO_Ship_Date,
   '0' Pseudo_Indicator,
   DUE_DATE  Available_Inventory_Date,
   retd,
   cetd,
   'DC' || substring(supplier_site_code, 5,3) 
from  
  g_fin_qv.WC_INTRANSIT_EXTRACT_FS
where 
  order_type_text in ('ON ORDER')
  and transaction_date = (select max(transaction_date) from g_fin_qv.WC_INTRANSIT_EXTRACT_FS )
  and not _deleted
  and supplier_site_code like 'PRD%'

)
select 
   Item_ID,
   Location_ID,
   PO_Order_ID,
   PO_Line_Number,
   Flow_Resource_ID,
   Order_Due_Date,
   Order_Status_Code,
   sum(Order_Quantity) Order_Quantity, 
   sum(Ship_Quantity) Ship_Quantity, 
   sum(Received_Quantity) Received_Quantity,
   PO_Order_ID User_Text_1,
   PO_Ship_Date,
   Pseudo_Indicator,
   Available_Inventory_Date,
   retd,
   cetd,
   suppliernumber
 from 
  q1
group by
Item_ID,
   Location_ID,
   PO_Order_ID,
   PO_Line_Number,
   Flow_Resource_ID,
   Order_Due_Date,
   Order_Status_Code,
   User_Text_1,
   PO_Ship_Date,
   Pseudo_Indicator,
   Available_Inventory_Date,
   retd,
   cetd,
   suppliernumber
""")
po1.createOrReplaceTempView('po1')


# COMMAND ----------

po2=spark.sql("""
select 
  it.Item_ID,
  it.Location_ID,
  it.PO_Order_ID,
  it.PO_Line_Number,
  it.Flow_Resource_ID,
  coalesce(location_codes.GlobalLocationCode,  '#Error27-' || it.supplierNumber) Vendor_Location_ID,
  coalesce(location_codes.GlobalLocationCode || '-F', '#Error27-' || it.supplierNumber)  Vendor_Flow_Resource_ID,
  it.Order_Due_Date,
  it.Order_Status_Code,
  it.Order_Quantity,
  it.Ship_Quantity,
  it.Received_Quantity,
  it.User_Text_1,
  it.PO_Ship_Date,
  it.Pseudo_Indicator,
  it.Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  po1 it
  left join location_codes on it.supplierNumber = location_codes.supplierNumber
  join pdhproduct on it.Item_id = pdhProduct.productCode
where 
  nvl(location_codes._source,'EBS') ='EBS'
 -- and po_order_id = '4007689'
""")
po2.createOrReplaceTempView('po2')

# COMMAND ----------

po3=spark.sql("""
select 
  it.Item_ID,
  it.Location_ID Destination_Location_ID,
  it.PO_Order_ID Transfer_Order_ID,
  it.PO_Line_Number Transfer_Order_Line_Number,
  coalesce(location_codes.GlobalLocationCode,  '#Error27-' || it.supplierNumber)  Source_Location_ID,
  coalesce(location_codes.GlobalLocationCode || '-F', '#Error27-' || it.supplierNumber) Source_Flow_Resource,
  it.Flow_Resource_ID Destination_Flow_Resource,
  it.Order_Due_Date,
  it.PO_Ship_Date Order_Ship_Date,
  it.Order_Status_Code,
  it.Order_Quantity,
  it.Ship_Quantity,
  it.Received_Quantity,
  it.User_Text_1,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  po1 it
  left join location_codes on it.supplierNumber = location_codes.supplierNumber
  join pdhproduct on it.Item_id = pdhProduct.productCode
""")
po3.createOrReplaceTempView('po3')

# COMMAND ----------

# DBTITLE 1,60.3.6 SPS_PURCHASE_ORDER - external purchase orders main
# purchase_order_1=spark.sql("""
# select 
#     msi.segment1 Item_ID,
#    'DC' || mp.organization_code Location_ID,
#    poh.segment1 PO_Order_ID,
#    int(pla.line_num) PO_Line_Number,
#    'DC' || mp.organization_code || '-F'  Flow_Resource_ID,
#    nvl(EBS_supplier_codes.GlobalLocationCode, '#Error27-' || pv.segment1) Vendor_Location_ID,
#    nvl(EBS_supplier_codes.GlobalLocationCode || '-F', '#Error27-' || pv.segment1) Vendor_Flow_Resource_ID,
#    nvl(pll.promised_date, pll.need_by_date) Order_Due_Date,
#    '0' Order_Status_Code,
#    case when   uom.UOM_CODE	 <> MSI.ATTRIBUTE15
#                      then round((pll.quantity - pll.quantity_received)  * nvl(conv_from_item.conversion_rate, conv_from.conversion_rate) / nvl(conv_to_item.conversion_rate, conv_to.conversion_rate),3)
#                      when  uom.UOM_CODE = MSI.ATTRIBUTE15 then  pll.quantity - pll.quantity_received
#    end as Order_Quantity,
#    0 Ship_Quantity,
#    0 Received_Quantity,
#    poh.segment1 User_Text_1,
#    '' PO_Ship_Date,
#    '0' Pseudo_Indicator,
#    poh.creation_date Available_Inventory_Date,
#    DATE_ADD(poh.creation_date, int(msi.SHELF_LIFE_DAYS)) Expire_Date
# from
#   ebs.PO_HEADERS_ALL poh
#    join ebs.po_lines_all pla on poh.po_header_id = pla.po_header_id
#   join ebs.mtl_system_items_b msi on msi.inventory_item_id = pla.item_id
#   join ebs.mtl_parameters mp on msi.organization_id = mp.organization_id
#   join ebs.po_line_locations_all pll on pll.po_line_id = pla.po_line_id
#   join ebs.ap_suppliers pv on poh.vendor_id=pv.vendor_id
#   left join EBS_supplier_codes on pv.segment1 = EBS_supplier_codes.supplierNumber
#   join ebs.MTL_UNITS_OF_MEASURE_TL uom on pll.unit_meas_lookup_code = uom.UNIT_OF_MEASURE
#   left join conv_from_item on  msi.segment1 = conv_from_item.segment1
# 						and uom.UOM_CODE = conv_from_item.uom_code 
#   left join conv_from on uom.UOM_CODE = conv_from.uom_code   
#   left join conv_to_item on msi.segment1 = conv_to_item.segment1 
# 						and msi.attribute15 = CONV_TO_ITEM.uom_code
#   left join conv_to on msi.attribute15 = conv_to.uom_code
# where 1=1
#   and msi.organization_id = mp.organization_id
#   AND pll.ship_to_organization_id = mp.organization_id
#   and pla.cancel_flag <> 'Y'
#   and mp.organization_code<>'325'
#   and (nvl(pll.closed_code,'OPEN')) in ('OPEN')
#   and uom.language = 'US'
# """)
# purchase_order_1.createOrReplaceTempView('purchase_order_1')

# COMMAND ----------

# DBTITLE 1,60.3.7 SPS_PURCHASE_ORDER - Apply historical product substitution
# purchase_order_2=spark.sql("""
# select 
#    nvl(HistoricalProductSubstitution.final_Successor, po.Item_ID) Item_ID,
#    po.Location_ID,
#    po.PO_Order_ID,
#    po.PO_Line_Number,
#    po.Flow_Resource_ID,
#    po.Vendor_Location_ID,
#    po.Vendor_Flow_Resource_ID,
#    po.Order_Due_Date,
#    po.Order_Status_Code,
#    po.Order_Quantity,
#    po.Ship_Quantity,
#    po.Received_Quantity,
#    po.User_Text_1,
#    po.PO_Ship_Date,
#    po.Pseudo_Indicator,
#    po.Available_Inventory_Date,
#    po.Expire_Date
# from 
#   purchase_order_1 po
#   left join HistoricalProductSubstitution on po.Item_id = HistoricalProductSubstitution.PROD_CD and po.Location_ID = HistoricalProductSubstitution.DC
#   join (select globallocationCode from locationmaster) lm on po.Location_ID = lm.globallocationCode
# where nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
# """)
# purchase_order_2.createOrReplaceTempView('purchase_order_2')
# # purchase_order_2.display()

# COMMAND ----------

# DBTITLE 1,60.3.8 SPS_PURCHASE_ORDER - Apply Current product substitution
# purchase_order_3=spark.sql("""
# select 
#    nvl(main_MPX_L0.SuccessorCode, po.Item_ID) Item_ID,
#    po.Location_ID,
#    po.PO_Order_ID,
#    po.PO_Line_Number,
#    po.Flow_Resource_ID,
#    po.Vendor_Location_ID,
#    po.Vendor_Flow_Resource_ID,
#    po.Order_Due_Date,
#    po.Order_Status_Code,
#    po.Order_Quantity,
#    po.Ship_Quantity,
#    po.Received_Quantity,
#    po.User_Text_1,
#    po.PO_Ship_Date,
#    po.Pseudo_Indicator,
#    po.Available_Inventory_Date,
#    po.Expire_Date
# from 
#   purchase_order_2 po
#   left join main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Location_ID = main_MPX_L0.DC
#   where 1=1
# """)
# purchase_order_3.createOrReplaceTempView('purchase_order_3')

# COMMAND ----------

# DBTITLE 1,60.3.9 SPS_PURCHASE_ORDER - Check if in PDH
# purchase_order_4=spark.sql("""
# select 
#    coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, po.Item_id || '-#Error1' ) Item_ID,
#    po.Location_ID,
#    po.PO_Order_ID,
#    po.PO_Line_Number,
#    po.Flow_Resource_ID,
#    po.Vendor_Location_ID,
#    po.Vendor_Flow_Resource_ID,
#    po.Order_Due_Date,
#    po.Order_Status_Code,
#    po.Order_Quantity,
#    po.Ship_Quantity,
#    po.Received_Quantity,
#    po.User_Text_1,
#    po.PO_Ship_Date,
#    po.Pseudo_Indicator,
#    po.Available_Inventory_Date,
#    po.Expire_Date
# from 
#   purchase_order_3 po
#   left join pdhProduct on po.Item_id = pdhProduct.productCode 
# where 1=1
#   and not (po.Order_Quantity = 0
#    and po.Ship_Quantity = 0 
#    and po.Received_Quantity = 0)
# """)
# purchase_order_4.createOrReplaceTempView('purchase_order_4')

# COMMAND ----------

# DBTITLE 1,60.3.10 Intransit main extract - External
in_transit_1=spark.sql("""
select 
   ITEM_NUMBER Item_ID,
   'DC' || INVENTORY_ORG Location_ID,
   case when instr(ORDER_NUMBER,'.') = 0 then ORDER_NUMBER
    else left(ORDER_NUMBER, instr(ORDER_NUMBER,'.')-1)
   end PO_Order_ID,
   case when instr(ORDER_NUMBER,'-') = 0 then 1
     else trim(substr(ORDER_NUMBER, instr(ORDER_NUMBER,'-')+1,10))
   end PO_Line_Number,
   'DC' || INVENTORY_ORG || '-F' Flow_Resource_ID,
--    po.Vendor_Location_ID,
--    po.Vendor_Flow_Resource_ID,
   DUE_DATE Order_Due_Date,
   '1' Order_Status_Code,
   sum(IN_TRANSIT_QTY_STD_UOM) Order_Quantity,
   sum(IN_TRANSIT_QTY_STD_UOM) Ship_Quantity,
   0 Received_Quantity,
   case when instr(ORDER_NUMBER,'.') = 0 then ORDER_NUMBER
    else left(ORDER_NUMBER, instr(ORDER_NUMBER,'.')-1)
   end User_Text_1,
   coalesce(retd, cetd, DUE_DATE) PO_Ship_Date,
   '0' Pseudo_Indicator,
   add_months(current_date, -1)  Available_Inventory_Date,
   retd,
   cetd,
   ebs_suppliers_number.supplierNumber
from  
  g_fin_qv.WC_INTRANSIT_EXTRACT_FS
  join ebs_suppliers_number on WC_INTRANSIT_EXTRACT_FS.supplier_site_code = ebs_suppliers_number.vendor_site_code
where 
  order_type_text in ('IN TRANSIT', 'Intransit shipment')
--  and source_org_code is  null
  and transaction_date = (select max(transaction_date) from g_fin_qv.WC_INTRANSIT_EXTRACT_FS )
  and not _deleted
  and supplier_site_code not like 'PRD%'
--  and order_number like '%15222497%'
group by 
   ITEM_NUMBER,
   'DC' || INVENTORY_ORG, 
   case when instr(ORDER_NUMBER,'.') = 0 then ORDER_NUMBER
    else left(ORDER_NUMBER, instr(ORDER_NUMBER,'.')-1)
   end,
   case when instr(ORDER_NUMBER,'-') = 0 then 1
     else trim(substr(ORDER_NUMBER, instr(ORDER_NUMBER,'-')+1,10))
   end,
   'DC' || INVENTORY_ORG || '-F',
   DUE_DATE,
   coalesce(retd, cetd, DUE_DATE) ,
   DUE_DATE,
   retd,
   cetd,
   ebs_suppliers_number.supplierNumber
   
union

select 
   ITEM_NUMBER Item_ID,
   'DC' || INVENTORY_ORG Location_ID,
   case when instr(ORDER_NUMBER,'.') = 0 then ORDER_NUMBER
    else left(ORDER_NUMBER, instr(ORDER_NUMBER,'.')-1)
   end PO_Order_ID,
   case when instr(ORDER_NUMBER,'-') = 0 then 1
     else trim(substr(ORDER_NUMBER, instr(ORDER_NUMBER,'-')+1,10))
   end PO_Line_Number,
   'DC' || INVENTORY_ORG || '-F' Flow_Resource_ID,
--    po.Vendor_Location_ID,
--    po.Vendor_Flow_Resource_ID,
   DUE_DATE Order_Due_Date,
   '1' Order_Status_Code,
   sum(IN_TRANSIT_QTY_STD_UOM) Order_Quantity,
   sum(IN_TRANSIT_QTY_STD_UOM) Ship_Quantity,
   0 Received_Quantity,
   case when instr(ORDER_NUMBER,'.') = 0 then ORDER_NUMBER
    else left(ORDER_NUMBER, instr(ORDER_NUMBER,'.')-1)
   end User_Text_1,
   coalesce(retd, cetd, DUE_DATE) PO_Ship_Date,
   '0' Pseudo_Indicator,
   add_months(current_date, -1)  Available_Inventory_Date,
   retd,
   cetd,
   'DC' || substring(supplier_site_code, 5,3) DC
from  
  g_fin_qv.WC_INTRANSIT_EXTRACT_FS
  
where 
  order_type_text in ('IN TRANSIT', 'Intransit shipment')
--  and source_org_code is  null
  and transaction_date = (select max(transaction_date) from g_fin_qv.WC_INTRANSIT_EXTRACT_FS )
  and not _deleted
  and supplier_site_code like 'PRD%'
--  and order_number like '%15219776%'
group by 
   ITEM_NUMBER,
   'DC' || INVENTORY_ORG, 
   case when instr(ORDER_NUMBER,'.') = 0 then ORDER_NUMBER
    else left(ORDER_NUMBER, instr(ORDER_NUMBER,'.')-1)
   end,
   case when instr(ORDER_NUMBER,'-') = 0 then 1
     else trim(substr(ORDER_NUMBER, instr(ORDER_NUMBER,'-')+1,10))
   end,
   'DC' || INVENTORY_ORG || '-F',
   DUE_DATE,
   coalesce(retd, cetd, DUE_DATE) ,
   DUE_DATE,
   retd,
   cetd,
   'DC' || substring(supplier_site_code, 5,3) 
   
""")
in_transit_1.createOrReplaceTempView('in_transit_1')

# COMMAND ----------

# DBTITLE 1,60.3.11 Intransit  - Add External Vendor Code
in_transit_2=spark.sql("""
select 
  it.Item_ID,
  it.Location_ID,
  it.PO_Order_ID,
  it.PO_Line_Number,
  it.Flow_Resource_ID,
  coalesce(location_codes.GlobalLocationCode, '#Error27-' || it.supplierNumber) Vendor_Location_ID,
  coalesce(location_codes.GlobalLocationCode || '-F', '#Error27-' || it.supplierNumber)  Vendor_Flow_Resource_ID,
  it.Order_Due_Date,
  it.Order_Status_Code,
  it.Order_Quantity,
  it.Ship_Quantity,
  it.Received_Quantity,
  it.User_Text_1,
  it.PO_Ship_Date,
  it.Pseudo_Indicator,
  it.Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  in_transit_1 it
  left join location_codes on it.supplierNumber = location_codes.supplierNumber
  join pdhproduct on it.Item_id = pdhProduct.productCode
where 
  nvl(location_codes._source,'EBS') ='EBS'
 -- and po_order_id = '4007689'
""")
in_transit_2.createOrReplaceTempView('in_transit_2')

# COMMAND ----------

# DBTITLE 1,60.3.12 Intransit  external - Apply historical product substitution
in_transit_3=spark.sql("""
select 
  it.Item_ID,
  it.Location_ID Destination_Location_ID,
  it.PO_Order_ID Transfer_Order_ID,
  it.PO_Line_Number Transfer_Order_Line_Number,
  coalesce(location_codes.GlobalLocationCode,  '#Error27-' || it.supplierNumber)  Source_Location_ID,
  coalesce(location_codes.GlobalLocationCode || '-F', '#Error27-' || it.supplierNumber) Source_Flow_Resource,
  it.Flow_Resource_ID Destination_Flow_Resource,
  it.Order_Due_Date,
  it.PO_Ship_Date Order_Ship_Date,
  it.Order_Status_Code,
  it.Order_Quantity,
  it.Ship_Quantity,
  it.Received_Quantity,
  it.User_Text_1,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  in_transit_1 it
  left join location_codes on it.supplierNumber = location_codes.supplierNumber
  join pdhproduct on it.Item_id = pdhProduct.productCode
 -- where it.PO_Order_ID = '15219714'
""")
in_transit_3.createOrReplaceTempView('in_transit_3')


# COMMAND ----------

EBS_Purchase_orders_EM=spark.sql("""
select * from po2
where Vendor_Location_ID like 'EM%'  
union 
select * from in_transit_2
where Vendor_Location_ID like 'EM%'
""")
EBS_Purchase_orders_EM.createOrReplaceTempView('EBS_Purchase_orders_EM')

# COMMAND ----------

EBS_purchase_orders_PL=spark.sql("""
select * from po3
where Source_Location_ID not like 'EM%'  
union 
select * from in_transit_3
where Source_Location_ID not like 'EM%'  
""")
EBS_purchase_orders_PL.createOrReplaceTempView('EBS_purchase_orders_PL')

# COMMAND ----------

# DBTITLE 1,60.3.13 Intransit external - Apply Current product substitution
# in_transit_4=spark.sql("""
# select 
#   nvl(main_MPX_L0.SuccessorCode, it.Item_ID) Item_ID,
#   it.Location_ID,
#   it.PO_Order_ID,
#   it.PO_Line_Number,
#   it.Flow_Resource_ID,
#   it.Vendor_Location_ID,
#   it.Vendor_Flow_Resource_ID,
#   it.Order_Due_Date,
#   it.Order_Status_Code,
#   it.Order_Quantity,
#   it.Ship_Quantity,
#   it.Received_Quantity,
#   it.User_Text_1,
#   it.PO_Ship_Date,
#   it.Pseudo_Indicator,
#   it.Available_Inventory_Date,
#   it.Expire_Date
# from 
#   in_transit_3 it
#   left join main_MPX_L0 on it.Item_id = main_MPX_L0.predecessorCode and it.Location_ID = main_MPX_L0.DC
# """)
# in_transit_4.createOrReplaceTempView('in_transit_4')

# COMMAND ----------

# DBTITLE 1,60.3.14 Intransit external - Check if in PDH
# in_transit_5=spark.sql("""
# select 
#    coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, it.Item_id || '-#Error1' ) Item_ID,
#    it.Location_ID,
#    it.PO_Order_ID,
#    it.PO_Line_Number,
#    it.Flow_Resource_ID,
#    it.Vendor_Location_ID,
#    it.Vendor_Flow_Resource_ID,
#    it.Order_Due_Date,
#    it.Order_Status_Code,
#    it.Order_Quantity,
#    it.Ship_Quantity,
#    it.Received_Quantity,
#    it.User_Text_1,
#    it.PO_Ship_Date,
#    it.Pseudo_Indicator,
#    it.Available_Inventory_Date,
#    it.Expire_Date
# from 
#   in_transit_4 it
#   left join pdhProduct on it.Item_id = pdhProduct.productCode 
# where 1=1
# and not (it.Order_Quantity = 0
#    and it.Ship_Quantity = 0
#    and it.Received_Quantity = 0)
# """)
# in_transit_5.createOrReplaceTempView('in_transit_5')

# COMMAND ----------

# DBTITLE 1,60.3.20 SAP Goods Supplier Codes
# SAP_supplier_codes=spark.sql("""
# select 'SAP' _SOURCE, SAPGOODSSUPPLIERCODE supplierNumber, min(GlobalLocationCode)  GlobalLocationCode
#   from locationmaster 
# where 
#   nvl(SAPGOODSSUPPLIERCODE, 'N/A') not in ('N/A')
#   and GlobalLocationCode like 'EM%'
# group by 
#   SAPGOODSSUPPLIERCODE
# """)
# SAP_supplier_codes.createOrReplaceTempView('SAP_supplier_codes')
# # SAP_supplier_codes.display()

# COMMAND ----------

# DBTITLE 1,60.3.21 SAP Plant Codes
# SAP_plant_codes=spark.sql("""
# select 'SAP' _SOURCE, SAPGOODSSUPPLIERCODE supplierNumber, min(GlobalLocationCode)  GlobalLocationCode
#   from locationmaster 
# where 
#   nvl(SAPGOODSSUPPLIERCODE, 'N/A') not in ('N/A')
#   and GlobalLocationCode like 'PL%'
# group by 
#   SAPGOODSSUPPLIERCODE
# """)
# SAP_plant_codes.createOrReplaceTempView('SAP_plant_codes')
# # SAP_plant_codes.display()

# COMMAND ----------

# DBTITLE 1,60.3.22 SAP Warehouse codes
r_werks=spark.sql("""
SELECT distinct Z_LOW WERKS FROM sapp01.zbc_dev_param 
    WHERE id_program = 'ZMM_IMPORT_ORDERS'
      AND id_param   = 'IMPORT_ORDERS'
      AND param1     = 'WERKS'
""")
r_werks.createOrReplaceTempView('r_werks')

# COMMAND ----------

# DBTITLE 1,60.3.23 SAP PO #1
# wt_ekpo=spark.sql("""
# SELECT '0' || ekpo.ebelp vgpos,
#          ekpo.matnr,
#          ekpo.loekz,
#          ekpo.werks,
#          ekpo.elikz ,                          
#          ekko.bsart,
#          ekko.BSTYP,
#          eket.eindt,
#          eket.menge,
#          eket.wemng,
#          ekpo.umrez,
#          ekpo.umren,
#          ekko.aedat,
#          ekko.ebeln,
#          ekko.ekgrp,
#          ekko.ekorg,
#          ekko.llief
# FROM sapp01.ekpo AS ekpo
#     INNER JOIN sapp01.ekko AS ekko ON ekko.ebeln = ekpo.ebeln
#     INNER JOIN sapp01.eket AS eket ON eket.ebeln = ekpo.ebeln
#            AND eket.ebelp = ekpo.ebelp
#     INNER JOIN r_werks on ekpo.werks = r_werks.werks
#     WHERE 1=1
#        AND eket.menge > eket.wemng
#        AND ekpo.elikz <> 'X'
#        AND ekpo.loekz = ' '
#        AND ekpo.matnr not like 'PALLET%'
#        and year(eket.eindt) > 2018
# """)
# wt_ekpo.createOrReplaceTempView('wt_ekpo')
# # wt_ekpo.display()

# COMMAND ----------

# DBTITLE 1,60.3.24 SAP Org's
r_ekorg=spark.sql("""
SELECT distinct 
  Z_LOW EKORG,
  case when Z_LOW in ('ZAPO','ZJPO') then 'APAC'
  else 'EMEA' 
 end region
FROM sapp01.zbc_dev_param
    WHERE id_program = 'ZMM_IMPORT_ORDERS'
      AND id_param   = 'IMPORT_ORDERS'
      AND param1     = 'EKORG'
""")
r_ekorg.createOrReplaceTempView('r_ekorg')

# COMMAND ----------

# DBTITLE 1,60.3.25 SAP PO #2
# sap_po=spark.sql("""
# select 
#   ekpo.mandt, 
#   ekpo.ebeln, 
#   ekpo.ebelp, 
#   ekpo.werks,
#   case when ekko.bsart in ('ZPO', 'ZAIT') then 'DS' || r_ekorg.region else 'DC' || ekpo.werks end DC,
#   nvl(cast(ekpo.matnr as integer),ekpo.matnr) matnr,
#   ekpo.aedat creationDate,
#   eket.eindt dueDate,
#   sum(ekpo.menge * ekpo.umrez / ekpo.umren) orderQuantity,
#   DATE_ADD(ekpo.aedat, int(MARA.MHDHB)) Expire_Date
# from 
#   sapp01.ekpo
#   join sapp01.ekko on ekpo.mandt = ekko.mandt
#     and ekpo.ebeln = ekko.ebeln
#   join sapp01.mara on ekpo.mandt = mara.mandt
#     and ekpo.matnr = mara.matnr
#   join r_werks on ekpo.werks = r_werks.werks
#   join r_ekorg on ekko.ekorg = r_ekorg.ekorg
#   left join sapp01.eket on ekpo.mandt =  eket.mandt
#     and ekpo.ebeln = eket.ebeln
#     and ekpo.ebelp = eket.ebelp
# where
#   1=1
#   and nvl(ekpo.menge * ekpo.umrez / ekpo.umren,0) <> 0
#   and ekpo.matnr <> ' '
#   and ekpo.matnr not like ('PALLET%')
#   and mara.mtart not in ('ZPRM', 'ZEMB', 'ZMAR', 'ZCSB', 'ZSEM')
#   and ekpo.elikz <> 'X'
  
#   AND ekpo.loekz = ' '
#   AND eket.menge > eket.wemng
#   and year(ekpo.aedat) > 2018 
#   and nvl(ekpo._deleted,0) = 0
#   and nvl(ekko._deleted,0) = 0
#   and nvl(mara._deleted,0) = 0
#   and nvl(eket._deleted,0) = 0
# group by 
#   ekpo.mandt, 
#   ekpo.ebeln, 
#   ekpo.ebelp, 
#   ekpo.werks,
#   eket.eindt, 
#   ekpo.aedat,
#   nvl(cast(ekpo.matnr as integer),ekpo.matnr),
#   case when ekko.bsart in ('ZPO', 'ZAIT') then 'DS' || r_ekorg.region else 'DC' || ekpo.werks end,
#   DATE_ADD(ekpo.aedat, int(MARA.MHDHB))
# order by 2,3
# """)
# sap_po.createOrReplaceTempView('sap_po')

# COMMAND ----------

# DBTITLE 1,60.3.26 Add historical product substitution
# sap_po1 = spark.sql("""
# select 
#   po.mandt, 
#   po.ebeln, 
#   po.ebelp, 
#   po.werks,
#   po.DC,
#   nvl(HistoricalProductSubstitution.final_Successor, po.matnr) matnr,
#   po.creationDate,
#   po.dueDate,
#   po.orderQuantity,
#   po.Expire_Date
# from sap_po po
# left join HistoricalProductSubstitution on po.matnr = HistoricalProductSubstitution.PROD_CD and po.DC = HistoricalProductSubstitution.DC
# where nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
# """)
# sap_po1.createOrReplaceTempView('sap_po1')

# COMMAND ----------

# DBTITLE 1,60.3.27 Add current product substitution
# sap_po2 = spark.sql("""
# select 
# po.mandt, 
#   po.ebeln, 
#   po.ebelp, 
#   po.werks,
#   po.DC,
#   nvl(main_MPX_L0.SuccessorCode, po.matnr) matnr,
#   po.creationDate,
#   po.dueDate,
#   po.orderQuantity,
#   po.Expire_Date
# from 
#   sap_po1 po
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on po.matnr = main_MPX_L0.predecessorCode and po.DC = main_MPX_L0.DC
# """)
# sap_po2.createOrReplaceTempView('sap_po2')

# COMMAND ----------

# DBTITLE 1,60.3.28 SAP Shipped PO's
# sap_shipped_po=spark.sql("""
#     select  
#       ekes.mandt, 
#       ekes.ebeln, 
#       ekes.ebelp, 
#       min(ekes.erdat) shipDate,
#       min(ekes.eindt) DueDate,
#       sum(ekes.menge * ekpo.umrez / ekpo.umren) shippedQuantity  
#     from 
#       sapp01.ekpo
#       join sapp01.ekko on ekpo.mandt = ekko.mandt
#         and ekpo.ebeln = ekko.ebeln
#       join sapp01.ekes on ekpo.mandt = ekes.mandt 
#         and ekpo.ebeln = ekes.ebeln
#         and ekpo.ebelp = ekes.ebelp
#       join sapp01.mara on ekpo.mandt = mara.mandt
#         and ekpo.matnr = mara.matnr
#       join r_werks on ekpo.werks = r_werks.werks
#       join r_ekorg on ekko.ekorg = r_ekorg.ekorg
#     where 1=1
#       and ekpo.matnr <> ' '
#       and mara.mtart not in ('ZPRM', 'ZEMB', 'ZMAR', 'ZCSB', 'ZSEM')
#       and ekpo.elikz <> 'X'
#       AND ekpo.loekz = ' '
#       and ekes.ebtyp = 'LA'
#     group by 
#       ekes.mandt, 
#       ekes.ebeln, 
#       ekes.ebelp
#     order by 2, 3
# """)
# sap_shipped_po.createOrReplaceTempView('sap_shipped_po')
# # sap_shipped_po.display()

# COMMAND ----------

# DBTITLE 1,60.3.29 SAP Receipts
# sap_receipts=spark.sql("""
# SELECT
#   lips.mandt,
#   lips.vgbel,
#   lips.vgpos,
#   sum(lips.lfimg * lips.umvkz /lips.umvkn) receivedQuantity
# FROM
#   sapp01.lips
#   JOIN sapp01.likp ON likp.vbeln = lips.vbeln
#   JOIN sapp01.vbup ON vbup.vbeln = lips.vbeln
#     AND vbup.posnr = lips.posnr 
#   JOIN r_werks on lips.werks = r_werks.werks
# WHERE 1=1
#   AND likp.vbtyp = '7'
#   AND vbup.wbsta NOT IN  ('C')
# group by 
#   lips.mandt,
#   lips.vgbel,
#   lips.vgpos
# order by 1,2,3
# """)
# sap_receipts.createOrReplaceTempView('sap_receipts')

# COMMAND ----------

# DBTITLE 1,60.3.30 SAP Vendors
sap_po_vendor=spark.sql("""
select 
  ekpa.mandt,
  ekpa.ebeln,
  lfa1.lifnr
from 
  sapp01.ekpa
  JOIN sapp01.lfa1 lfa1 ON lfa1.lifnr = ekpa.lifn2
where ekpa.ebelp = '00000'
  AND  ekpa.parvw in ('WL')
""")
sap_po_vendor.createOrReplaceTempView('sap_po_vendor')

# COMMAND ----------

# DBTITLE 1,60.3.31 SAP Purchase Orders
# sap_purchase_orders=spark.sql("""
# select 
#   sap_po.matnr Item_ID,
#   sap_po.DC Location_ID,
#   sap_po.ebeln PO_Order_ID,
#   cast(sap_po.ebelp as integer) PO_Line_number,
#   sap_po.DC || '-F' Flow_Resource_ID,
#   nvl(SAP_supplier_codes.globalLocationCode, '#Error28-' || sap_po_vendor.lifnr)  Vendor_Location_ID, 
#   nvl(SAP_supplier_codes.globalLocationCode || '-F', '#Error28-' || sap_po_vendor.lifnr)  Vendor_Flow_Resource_ID,
#   sap_po.dueDate Order_Due_Date,
#   '0' Order_Status_Code,
#   sap_po.orderQuantity - nvl(sap_shipped_po.shippedQuantity,0) Order_Quantity , 
#   0 Ship_Quantity,
#   0 Received_Quantity, 
#   sap_po.ebeln User_Text_1,
#   ''  PO_Ship_Date,
#   '0' Pseudo_Indicator,
#   sap_po.creationDate Available_Inventory_Date,
#   sap_po.Expire_Date Expire_Date
# from 
#   sap_po2 sap_po
#   left join sap_shipped_po on sap_po.mandt = sap_shipped_po.mandt 
#     and sap_po.ebeln = sap_shipped_po.ebeln
#     and sap_po.ebelp = sap_shipped_po.ebelp
#   join sap_po_vendor on sap_po.mandt = sap_po_vendor.mandt
#     and sap_po.ebeln = sap_po_vendor.ebeln
#   left join SAP_supplier_codes on cast(cast(sap_po_vendor.lifnr as integer) as string) = SAP_supplier_codes.supplierNumber
#   join pdhProduct on sap_po.matnr = pdhProduct.productCode
# where sap_po.orderQuantity - nvl(sap_shipped_po.shippedQuantity,0) <> 0
# """)
# sap_purchase_orders.createOrReplaceTempView('sap_purchase_orders')
# # sap_purchase_orders.display()

# COMMAND ----------

# DBTITLE 1,60.3.32 SAP Intransit orders
# sap_purchase_intransit_orders=spark.sql("""
# select 
#     sap_po.matnr Item_ID,
#     sap_po.DC Location_ID,
#     sap_po.ebeln PO_Order_ID,
#     cast(sap_po.ebelp as integer) PO_Line_number,
#     sap_po.DC || '-F' Flow_Resource_ID,
#     nvl(SAP_supplier_codes.globalLocationCode, '#Error28-' || sap_po_vendor.lifnr) Vendor_Location_ID, 
#     nvl(SAP_supplier_codes.globalLocationCode || '-F', '#Error28-' || sap_po_vendor.lifnr) Vendor_Flow_Resource_ID,
#     sap_po.dueDate Order_Due_Date,
#     '1' Order_Status_Code,
#     sap_shipped_po.shippedQuantity - nvl(sap_receipts.receivedQuantity,0) order_Quantity,
#     sap_shipped_po.shippedQuantity - nvl(sap_receipts.receivedQuantity,0) Ship_Quantity,
#     cast(0 as integer) as Received_Quantity,
#     sap_po.ebeln User_Text_1,
#     sap_shipped_po.shipDate PO_Ship_Date,
#     '0' as Pseudo_Indicator,
#     sap_shipped_po.dueDate Available_Inventory_Date,
#     sap_po.Expire_Date Expire_Date
# from 
#     sap_po2 sap_po
#     join sap_shipped_po on sap_po.mandt = sap_shipped_po.mandt 
#       and sap_po.ebeln = sap_shipped_po.ebeln
#       and sap_po.ebelp = sap_shipped_po.ebelp
#     left join sap_receipts on sap_po.mandt = sap_receipts.mandt 
#       and sap_po.ebeln = sap_receipts.vgbel
#       and sap_po.ebelp = sap_receipts.vgpos
#     join sap_po_vendor on sap_po.mandt = sap_po_vendor.mandt
#       and sap_po.ebeln = sap_po_vendor.ebeln
#     left join SAP_supplier_codes on cast(cast(sap_po_vendor.lifnr as integer) as string) = SAP_supplier_codes.supplierNumber
#     join pdhProduct on sap_po.matnr = pdhProduct.productCode
#  where 
#       sap_shipped_po.shippedQuantity <> 0
#       and sap_shipped_po.shippedQuantity - nvl(sap_receipts.receivedQuantity,0) <> 0
# """)
# sap_purchase_intransit_orders.createOrReplaceTempView('sap_purchase_intransit_orders')

# # sap_purchase_intransit_orders.display()

# COMMAND ----------

tot_suppliers=spark.sql("""
select 
  company, 
  supplier_name, 
  supplier_cod, 
  supplier_id  
from 
  tot.tb_PBI_Supplier
where not  _deleted
""")
tot_suppliers.createOrReplaceTempView('tot_suppliers')

# COMMAND ----------

tot_pbi_transit=spark.sql("""
select 
  company,
  prod_cd,
  chan_id,
  to_date(due_date, 'yyyy/MM/dd') due_date,
  cast(good_qty as integer) good_qty,
  to_date(create_date, 'yyyy/MM/dd') create_date,
  cast(orig_qty as integer) orig_qty,
  po_num,
  line_num,
  to_date(sched_date, 'yyyy/MM/dd') sched_date, 
  source_ref,
  userstrin_0,
  case when trim(userstrin_2)<> '' then userstrin_2 end userstrin_2,
  userstrin_3,
  warehouse,
  uom_std_ansell,
  cast(qtt_converted as integer) qtt_converted
from
 tot.tb_PBI_transit it
where 
  not it._deleted
""")
tot_pbi_transit.createOrReplaceTempView('tot_pbi_transit')
# tot_pbi_transit.display()

# COMMAND ----------

tot_purchase_orders1=spark.sql("""
select 
  pr.Oracle_cod Item_ID,
  Case 
    when it.company = '5210' then 'DCABL'
    else 'DCHER' 
  end Location_ID,
  it.PO_NUM PO_Order_ID,
  it.LINE_NUM PO_Line_Number,
   Case 
    when it.company = '5210' then 'DCABL-F'
    else 'DCHER-F'
  end  Flow_Resource_ID,
  nvl(lm.GlobalLocationCode, '#Error39-' || coalesce(cast(it.USERSTRIN_2 as integer), cast(tot_suppliers.supplier_cod as integer), it.userstrin_3)) as Vendor_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error39-' || coalesce(cast(it.USERSTRIN_2 as integer), cast(tot_suppliers.supplier_cod as integer), it.userstrin_3)) as Vendor_Flow_Resource_ID,
  IT.DUE_DATE as Order_Due_Date ,
  case when USERSTRIN_0 in  ('OPEN PO', 'PO REQUEST') 
    then '0'
  else '1'
  end Order_Status_Code,
  QTT_CONVERTED Order_Quantity,
  case when USERSTRIN_0  in ('OPEN PO', 'PO REQUEST') 
    then 0
    else QTT_CONVERTED 
  end Ship_Quantity,
  0 Received_Quantity,
  it.PO_NUM User_Text_1,
  IT.SCHED_DATE PO_Ship_Date,
  '0' Pseudo_Indicator,
--   IT.SCHED_DATE Available_Inventory_Date,
--   IT.SCHED_DATE Expire_Date
   
   add_months(current_date, -1)  Available_Inventory_Date,
   --add_months(current_date, -1) Expire_Date
   DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  --tot.tb_PBI_transit it
  tot_pbi_transit it
  left join tot_suppliers on it.company = tot_suppliers.company and it.userstrin_2 = tot_suppliers.supplier_id
  join tot.tb_PBI_product pr on it.PROD_CD = pr.Product_cod
  left join locationmaster lm on cast(tot_suppliers.supplier_cod as integer) = lm.MICROSIGASUPPLIERCODE 
  join pdhProduct on it.prod_cd = pdhProduct.productCode
where 1=1
  --not it._deleted
  and not pr._deleted
  and it.COMPANY = '5210'
  and nvl(lm.GlobalLocationCode, 'EM') like 'EM%'
""")
tot_purchase_orders1.createOrReplaceTempView('tot_purchase_orders1')

# COMMAND ----------

tot_transfer_orders1=spark.sql("""
select
  pr.Oracle_cod Item_ID,
  Case 
    when it.company = '5210' then 'DCABL'
    else 'DCHER' 
  end Destination_Location_ID,
  it.PO_NUM Transfer_Order_ID,
  it.LINE_NUM Transfer_Order_Line_Number,
  nvl(lm.GlobalLocationCode, '#Error39-' || coalesce(cast(it.USERSTRIN_2 as integer), cast(tot_suppliers.supplier_cod as integer), it.userstrin_3)) as Source_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error39-' || coalesce(cast(it.USERSTRIN_2 as integer), cast(tot_suppliers.supplier_cod as integer), it.userstrin_3)) as Source_Flow_Resource,
  Case 
    when it.company = '5210' then 'DCABL-F'
    else 'DCHER-F' 
  end Destination_Flow_Resource,
  IT.DUE_DATE as Order_Due_Date,
  IT.DUE_DATE as Order_Ship_Date,
  case when USERSTRIN_0 in  ('OPEN PO', 'PO REQUEST') 
    then '0'
  else '1'
  end Order_Status_Code,
  QTT_CONVERTED  Order_Quantity,
  case when USERSTRIN_0 in  ('OPEN PO', 'PO REQUEST') 
    then 0
    else QTT_CONVERTED 
  end Ship_Quantity,
  0 Received_Quantity,
  it.PO_NUM User_Text_1,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  tot_pbi_transit it
  left join tot_suppliers on it.company = tot_suppliers.company and it.userstrin_2 = tot_suppliers.supplier_id
  join tot.tb_PBI_product pr on it.PROD_CD = pr.Product_cod
  left join locationmaster lm on cast(tot_suppliers.supplier_cod as integer) = cast(lm.MICROSIGASUPPLIERCODE as integer)
  join pdhProduct on it.prod_cd = pdhProduct.productCode
where 1=1
  and not pr._deleted
  and it.COMPANY = '5210'
  and left(nvl(lm.GlobalLocationCode, 'PL'),2) in ('PL', 'DC', 'DS')
""")
tot_transfer_orders1.createOrReplaceTempView('tot_transfer_orders1')

# COMMAND ----------

# tot_purchase_orders3=spark.sql("""
# select
#   nvl(main_MPX_L0.SuccessorCode, po.Item_ID) Item_ID,
#   Location_ID,
#   PO_Order_ID,
#   PO_Line_Number,
#   Flow_Resource_ID,
#   Vendor_Location_ID ,
#   Vendor_Flow_Resource_ID ,
#   Order_Due_Date ,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   PO_Ship_Date,
#   Pseudo_Indicator,
#   Available_Inventory_Date,
#   Expire_Date
# from 
#   tot_purchase_orders2 po
# --   left join main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Location_ID = main_MPX_L0.DC
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Location_ID = main_MPX_L0.DC
# """)
# tot_purchase_orders3.createOrReplaceTempView('tot_purchase_orders3')
# # tot_purchase_orders3.display()

# COMMAND ----------

# tot_purchase_orders4=spark.sql("""
# select
#   coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, po.Item_id || '-#Error1' ) Item_ID,
#   Location_ID,
#   PO_Order_ID,
#   PO_Line_Number,
#   Flow_Resource_ID,
#   Vendor_Location_ID ,
#   Vendor_Flow_Resource_ID ,
#   Order_Due_Date ,
#   '0' Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   PO_Ship_Date,
#   Pseudo_Indicator,
#   Available_Inventory_Date,
#   Expire_Date
# from 
#   tot_purchase_orders3 po
#   left join pdhProduct on po.Item_id = pdhProduct.productCode 
# where  1=1
# and not (Order_Quantity = 0
#    and Ship_Quantity = 0
#    and Received_Quantity = 0)
# """)
# tot_purchase_orders4.createOrReplaceTempView('tot_purchase_orders4')
# # tot_purchase_orders4.display()

# COMMAND ----------

dbo_v_tembo_open_orders=spark.sql("""
select 
  dbo_v_tembo_open_orders.PROD_CD,
  CHAN_ID,
  to_date(DUE_DATE, 'dd/MM/yyyy') DUE_DATE,
  GOOD_QTY,
  to_date(CREATE_DATE, 'dd/MM/yyyy') CREATE_DATE,
  ORIG_QTY,
  PO_Num,
  to_date(SCHED_DATE, 'dd/MM/yyyy') SCHED_DATE,
  SOURCE_REF,
  USERSTRING_0,
  USERSTRING_2,
  USERSTRING_3,
  USERSTRING_1,
  FInterID,
  FEntryID,
  ShipDate,
  FQty,
  FCommitQty,
  CETA,
  CETD,
  ROW_NUMBER() OVER(PARTITION BY po_num ORDER BY Prod_cd DESC) PO_Line_Number
from  
  kgd.dbo_v_tembo_open_orders
where 
  not _deleted
""")
dbo_v_tembo_open_orders.createOrReplaceTempView('dbo_v_tembo_open_orders')

# COMMAND ----------

kgd_purchase_orders1=spark.sql("""
select 
  pr.ASPN Item_ID,
  'DCASWH' Location_ID,
  case 
    when USERSTRING_0 = ''
      then poh.FInterid
    else poh.PONumber
  end PO_Order_ID,
  oo.PO_Line_Number,
  'DCASWH-F' Flow_Resource_ID,
  nvl(lm.GlobalLocationCode, '#Error40-' || poh.fnumber) as Vendor_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error40-' || poh.fnumber) as Vendor_Flow_Resource_ID,
  oo.due_date as Order_Due_Date ,
  case when USERSTRING_0 = '' then 
    '0'
  else '1' end Order_Status_Code,
  good_qty Order_Quantity,
  case when USERSTRING_0 = '0' then 0
    else good_qty 
  end Ship_Quantity,
  0 Received_Quantity,
  oo.po_num User_Text_1,
  date_add(oo.due_date, -28) as PO_Ship_Date,
  '0' Pseudo_Indicator,
  oo.due_date Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  dbo_v_tembo_open_orders oo
  join kgd.dbo_tembo_poorder poh on oo.po_num = poh.ponumber and oo.finterid = poh.finterid
  join kgd.dbo_tembo_product pr on oo.Prod_Cd = pr.productCode
  left join locationmaster lm on poh.fnumber = lm.KINGDEESUPPLIERCODE
  join pdhProduct on pr.aspn = pdhProduct.productCode
where 1=1
  and not poh._deleted
  and not pr._deleted
  and nvl(lm.GlobalLocationCode , 'EM') like 'EM%'
""")
kgd_purchase_orders1.createOrReplaceTempView('kgd_purchase_orders1')

# COMMAND ----------

# kgd_purchase_orders1=spark.sql("""
# select 
#   pr.ASPN Item_ID,
#   'DCASWH' Location_ID,
#   poh.PONumber PO_Order_ID,
#   pol.FEntryID PO_Line_Number,
#   'DCASWH-F' Flow_Resource_ID,
#   nvl(lm.GlobalLocationCode, '#Error40-' || poh.fnumber) as Vendor_Location_ID,
#   nvl(lm.GlobalLocationCode || '-F', '#Error40-' || poh.fnumber) as Vendor_Flow_Resource_ID,
#   nvl(pol.CETD, pol.ShipDate) as Order_Due_Date ,
#   '0' Order_Status_Code,
#   pol.FQty - pol.FCommitQty  Order_Quantity,
#   0 Ship_Quantity,
#   0 Received_Quantity,
#   poh.PONumber User_Text_1,
#   pol.ShipDate PO_Ship_Date,
#   '0' Pseudo_Indicator,
#   pol.ShipDate Available_Inventory_Date,
#   pol.ShipDate Expire_Date
# from 
#   kgd.dbo_v_tembo_open_orders oo
#   join kgd.dbo_tembo_poorder poh on oo.po_num = poh.ponumber
#   join kgd.dbo_tembo_poorderentry pol on poh.finterid = pol.finterid
#   join kgd.dbo_tembo_product pr on pol.itemnumber = pr.productCode
#   left join locationmaster lm on poh.fnumber = lm.KINGDEESUPPLIERCODE
# where
#   not pol._deleted
#   and not poh._deleted
#   and not pr._deleted
#   and pol.FQty - pol.FCommitQty > 0
#   and pol.linestatus in ('Open')
#   and nvl(lm.GlobalLocationCode , 'EM') like 'EM%'
#   and oo.USERSTRING_0 = ''
  
# """)
# kgd_purchase_orders1.createOrReplaceTempView('kgd_purchase_orders1')
# # kgd_purchase_orders1.display()

# COMMAND ----------

kgd_transfer_orders1=spark.sql("""
select 
  pr.ASPN Item_ID,
  'DCASWH' Destination_Location_ID,
  case 
    when USERSTRING_0 = ''
      then poh.FInterid
    else poh.PONumber
  end Transfer_Order_ID,
  oo.PO_Line_Number Transfer_Order_Line_Number,
  nvl(lm.GlobalLocationCode, '#Error40-' || poh.fnumber) as Source_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error40-' || poh.fnumber) as Source_Flow_Resource,
  'DCASWH-F' Destination_Flow_Resource,
  oo.due_date as Order_Due_Date,
  date_add(oo.due_date, -28) as Order_Ship_Date,
  case when USERSTRING_0 = '' then 
    '0'
  else '1' end Order_Status_Code,
  good_qty  Order_Quantity,
  case when USERSTRING_0 = '0' then 0
    else good_qty 
  end Ship_Quantity,
  0 Received_Quantity,
  poh.PONumber User_Text_1,
  DATE_ADD(current_date, nvl(pdhproduct.shelfLife,0)) Expire_Date
from 
  dbo_v_tembo_open_orders oo
  join kgd.dbo_tembo_poorder poh on oo.po_num = poh.ponumber
  join kgd.dbo_tembo_product pr on  oo.Prod_Cd = pr.productCode
  left join locationmaster lm on poh.fnumber = lm.KINGDEESUPPLIERCODE
  join pdhProduct on pr.aspn = pdhProduct.productCode
where 1=1
  and not poh._deleted
  and not pr._deleted
  and left(nvl(lm.GlobalLocationCode,'PL'),2) in ('PL', 'DC', 'DS')
""")
kgd_transfer_orders1.createOrReplaceTempView('kgd_transfer_orders1')

# COMMAND ----------

# kgd_purchase_orders3=spark.sql("""
# select
#   nvl(main_MPX_L0.SuccessorCode, po.Item_ID) Item_ID,
#   Location_ID,
#   PO_Order_ID,
#   PO_Line_Number,
#   Flow_Resource_ID,
#   Vendor_Location_ID ,
#   Vendor_Flow_Resource_ID ,
#   Order_Due_Date ,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   PO_Ship_Date,
#   Pseudo_Indicator,
#   Available_Inventory_Date,
#   Expire_Date
# from 
#   kgd_purchase_orders2 po
# --   left join main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Location_ID = main_MPX_L0.DC
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Location_ID = main_MPX_L0.DC
# """)
# kgd_purchase_orders3.createOrReplaceTempView('kgd_purchase_orders3')
# # kgd_purchase_orders3.display()

# COMMAND ----------

# kgd_purchase_orders4=spark.sql("""
# select
#   coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, po.Item_id || '-#Error1' ) Item_ID,
#   Location_ID,
#   PO_Order_ID,
#   PO_Line_Number,
#   Flow_Resource_ID,
#   Vendor_Location_ID ,
#   Vendor_Flow_Resource_ID ,
#   Order_Due_Date ,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   PO_Ship_Date,
#   Pseudo_Indicator,
#   Available_Inventory_Date,
#   Expire_Date
# from 
#   kgd_purchase_orders3 po
#   left join pdhProduct on po.Item_id = pdhProduct.productCode 
# where  1=1
# and not (Order_Quantity = 0
#    and Ship_Quantity = 0
#    and Received_Quantity = 0)
# """)
# kgd_purchase_orders4.createOrReplaceTempView('kgd_purchase_orders4')
# # kgd_purchase_orders4.display()

# COMMAND ----------

sapopenorders= spark.sql("""
select 
  ProductCode,
  ChannelId,
  case 
    when ChannelId = 'ANZ-DS' then 'DSAPAC'
    when ChannelId in ('20-11', '20-12') then 'DCANV1'
    when ChannelId in ('20-13') then 'DCANV2'
    when ChannelId in ('20-14') then 'DCANV3'
    when ChannelId in ('ANV4' ) then 'DCANV4'
    when ChannelId in ('20-DS', 'NLT1-DS') then 'DSEMEA'
    when ChannelId in ('AAL1' ) then 'DCAAL1'
    when ChannelId in ('AAL2' ) then 'DCAAL2'
    when ChannelId in ('AJN1' ) then 'DCAJN1'
    when ChannelId in ('NLT1' ) then 'DCNLT1'
  end locationId,
  to_date(DueDate, 'dd/MM/yyyy') DueDate,
  cast(goodsQty as decimal (32,2)),
  to_date(CreationDate, 'dd/MM/yyyy') CreationDate,
  cast(originalQty as decimal(32,2)) originalQty,
  PONumber,
  ROW_NUMBER() OVER(PARTITION BY PONumber ORDER BY sapopenorders.ProductCode DESC) PO_Line_Number,
  to_date(ScheduleDate, 'dd/MM/yyyy') ScheduleDate,
  SOURCE_REF,
  OriginalPONumber,
  case when nvl(vendor, 'X') = 'GTC'
     then '200336'
  else vendor
  end vendor,
  vendorName,
  sourceWarehouse
from 
  sap.sapopenorders
where 
  not _deleted
""")
sapopenorders.createOrReplaceTempView('sapopenorders')

# COMMAND ----------

sap_purchase_orders_temp=spark.sql("""
select 
  sap_po.ProductCode Item_ID,
  locationId Location_ID,
  sap_po.PONumber PO_Order_ID,
  sap_po.PO_Line_Number,
  locationId || '-F' Flow_Resource_ID,
  nvl(lm.GlobalLocationCode, '#Error41-' || cast(vendor as integer)) as Vendor_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || vendor) as Vendor_Flow_Resource_ID,
  sap_po.duedate as Order_Due_Date ,
  '0' Order_Status_Code,
  goodsQty Order_Quantity,
  0 Ship_Quantity,
  0 Received_Quantity,
  sap_po.PONumber User_Text_1,
  -- sap_po.ScheduleDate PO_Ship_Date,
  date_add(sap_po.duedate, -35 ) as PO_Ship_Date,
  '0' Pseudo_Indicator,
  sap_po.ScheduleDate Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  sapopenorders sap_po
  join pdhProduct pr on sap_po.ProductCode = pr.productCode 
  left join location_codes lm  on cast (vendor as integer) = lm.supplierNumber
 where 
  1=1
  and vendorName not in ('20-IT')
  and vendor is not null
  and nvl(lm.GlobalLocationCode, 'EM') like 'EM%'
  and channelid is not null
  
union
  
select 
  sap_po.ProductCode Item_ID,
  locationId Location_ID,
  sap_po.PONumber PO_Order_ID,
  sap_po.PO_Line_Number,
  locationId || '-F' Flow_Resource_ID,
  nvl(lm.GlobalLocationCode, '#Error41-' || cast (ekko.lifnr as integer)) as Vendor_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || cast (ekko.lifnr as integer)) as Vendor_Flow_Resource_ID,
  sap_po.duedate as Order_Due_Date ,
  '0' Order_Status_Code,
  goodsQty Order_Quantity,
  0 Ship_Quantity,
  0 Received_Quantity,
  sap_po.PONumber User_Text_1,
  date_add(sap_po.duedate, -35 ) PO_Ship_Date,
  '0' Pseudo_Indicator,
  sap_po.ScheduleDate Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  sapopenorders sap_po
  left join sapp01.ekko on sap_po.PONumber = cast(ekko.ebeln as integer)
  join pdhProduct pr on sap_po.ProductCode = pr.productCode 
  left join location_codes lm  on cast (ekko.lifnr as integer) = lm.supplierNumber
  
 where 
  1=1
  and vendor is  null
  and vendorName  is null
  and case 
    when ChannelId = 'ANZ-DS' then 'DSAPAC'
    when ChannelId in ('20-11', '20-12') then 'DCANV1'
    when ChannelId in ('20-13') then 'DCANV2'
    when ChannelId in ('20-14') then 'DCANV3'
    when ChannelId in ('ANV4' ) then 'DCANV4'
    when ChannelId in ('20-DS', 'NLT1-DS') then 'DSEMEA'
    when ChannelId in ('AAL1' ) then 'DCAAL1'
    when ChannelId in ('AAL2' ) then 'DCAAL2'
    when ChannelId in ('AJN1' ) then 'DCAJN1'
    when ChannelId in ('NLT1' ) then 'DCNLT1'
  end <> lm.GlobalLocationCode
  and nvl(lm.GlobalLocationCode, 'EM') like 'EM%'
  and channelid is not null
  
union

select
   sap_po.ProductCode Item_ID,
  locationId Location_ID,
  sap_po.OriginalPONumber PO_Order_ID,
  sap_po.PO_Line_Number,
  locationId || '-F' Flow_Resource_ID,
  nvl(lm.GlobalLocationCode, '#Error41-' || cast (sap_po_vendor.lifnr as integer)) as Vendor_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || cast (sap_po_vendor.lifnr as integer)) as Vendor_Flow_Resource_ID,
  sap_po.duedate as Order_Due_Date ,
  '1' Order_Status_Code,
  goodsQty as Order_Quantity,
  goodsQty as Ship_Quantity,
  0 as Received_Quantity,
  sap_po.OriginalPONumber as User_Text_1,
  date_add(sap_po.duedate, -35 ) as PO_Ship_Date,
  '0' as Pseudo_Indicator,
  sap_po.ScheduleDate as Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  sapopenorders sap_po
  join pdhProduct pr on sap_po.ProductCode = pr.productCode 
  join sapp01.ekko on sap_po.OriginalPONumber = cast(ekko.ebeln as integer)
  left join sap_po_vendor on sap_po.OriginalPONumber = cast(sap_po_vendor.ebeln as integer)
  left join location_codes lm  on cast (nvl(sap_po_vendor.lifnr, ekko.lifnr) as integer) = lm.supplierNumber
where 
  1=1
  and vendorName  in ('20-IT')
  and nvl(lm.GlobalLocationCode, 'EM') like 'EM%'
  and channelid is not null
""")
sap_purchase_orders_temp.createOrReplaceTempView('sap_purchase_orders_temp')

# COMMAND ----------

sap_transfer_orders_temp=spark.sql("""
select 
  sap_po.ProductCode Item_ID,
  locationId Destination_Location_ID,
  sap_po.PONumber Transfer_Order_ID,
  sap_po.PO_Line_Number Transfer_Order_Line_Number,
  nvl(lm.GlobalLocationCode, '#Error41-' || cast(vendor as integer)) as Source_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || cast(vendor as integer)) as Source_Flow_Resource,
  locationId || '-F' Destination_Flow_Resource,
  sap_po.duedate as Order_Due_Date ,
  -- sap_po.ScheduleDate Order_Ship_Date,
  date_add(sap_po.duedate, -49 ) as Order_Ship_Date,
  '0' Order_Status_Code,
  goodsQty Order_Quantity,
  0 Ship_Quantity,
  0 Received_Quantity,
  sap_po.PONumber User_Text_1,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  sapopenorders sap_po
  join pdhProduct pr on sap_po.ProductCode = pr.productCode 
  left join location_codes lm  on cast (vendor as integer) = lm.supplierNumber
 where 
  1=1
  and vendorName not in ('20-IT')
  and vendor is not null
  and nvl(lm.GlobalLocationCode, 'PL') not like 'EM%'
  and channelid is not null
  
union
  
select 
  sap_po.ProductCode Item_ID,
  LocationId Destination_Location_ID,
  sap_po.PONumber Transfer_Order_ID,
  sap_po.PO_Line_Number Transfer_Order_Line_Number,
  nvl(lm.GlobalLocationCode, '#Error41-' || cast (ekko.lifnr as integer)) as Source_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || cast (ekko.lifnr as integer)) as Source_Flow_Resource,
  LocationId || '-F' Destination_Flow_Resource,
  sap_po.duedate as Order_Due_Date,
  date_add(sap_po.duedate, -49 ) Order_Ship_Date,
  '0' Order_Status_Code,
  goodsQty Order_Quantity,
  0 Ship_Quantity,
  0 Received_Quantity,
  sap_po.PONumber User_Text_1,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  sapopenorders sap_po
  left join sapp01.ekko on sap_po.PONumber = cast(ekko.ebeln as integer)
  join pdhProduct pr on sap_po.ProductCode = pr.productCode 
  left join location_codes lm  on cast (ekko.lifnr as integer) = lm.supplierNumber
  
 where 
  1=1
  and vendor is  null
  and vendorName  is null
  and locationId <> lm.GlobalLocationCode
  and nvl(lm.GlobalLocationCode, 'PL') not like 'EM%'
  and channelid is not null
  
union

select
  sap_po.ProductCode Item_ID,
  locationId Destination_Location_ID,
  sap_po.OriginalPONumber Transfer_Order_ID,
  sap_po.PO_Line_Number Transfer_Order_Line_Number,
  nvl(lm.GlobalLocationCode, '#Error41-' || cast (sap_po_vendor.lifnr as integer)) as Source_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || cast (sap_po_vendor.lifnr as integer)) as Source_Flow_Resource,
  locationId || '-F' Destination_Flow_Resource,
  sap_po.duedate as Order_Due_Date ,
  date_add(sap_po.duedate, -49 ) Order_Ship_Date,
  '1' Order_Status_Code,
  goodsQty as Order_Quantity,
  goodsQty as Ship_Quantity,
  0 Received_Quantity,
  sap_po.OriginalPONumber User_Text_1,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  sapopenorders sap_po
  join pdhProduct pr on sap_po.ProductCode = pr.productCode 
  join sapp01.ekko on sap_po.OriginalPONumber = cast(ekko.ebeln as integer)
  left join sap_po_vendor on sap_po.OriginalPONumber = cast(sap_po_vendor.ebeln as integer)
  left join location_codes lm  on cast (nvl(sap_po_vendor.lifnr, ekko.lifnr) as integer) = lm.supplierNumber
where 
  1=1
  and vendorName  in ('20-IT')
  and nvl(lm.GlobalLocationCode, 'PL') not like 'EM%'
  and channelid is not null
""")
sap_transfer_orders_temp.createOrReplaceTempView('sap_transfer_orders_temp')

# COMMAND ----------

vw_mle_open_orders=spark.sql("""
select 
  PROD_CD,
  DUE_DATE,
  GOOD_QTY,
  ORIG_QTY,
  'PO' || PO_NUM PO_NUM,
  ROW_NUMBER() OVER(PARTITION BY po_num ORDER BY Prod_cd DESC) PO_Line_Number,
  nvl(COLUMBIA_SUPPLIER_CODES.cValue, USERSTRING__2) USERSTRING__2 ,
  USERSTRING__3
from col.vw_mle_open_orders po
  left join COLUMBIA_SUPPLIER_CODES on USERSTRING__2 = COLUMBIA_SUPPLIER_CODES.keyValue
""")
vw_mle_open_orders.createOrReplaceTempView('vw_mle_open_orders')

# COMMAND ----------

vw_mle_in_transit=spark.sql("""
select 
  PROD_CD,
  DUE_DATE,
  GOOD_QTY,
  ORIG_QTY,
  'PO' || PO_NUM PO_NUM,
  ROW_NUMBER() OVER(PARTITION BY po_num ORDER BY Prod_cd DESC) PO_Line_Number,
  nvl(COLUMBIA_SUPPLIER_CODES.cValue, USERSTRING__2) USERSTRING__2,
  USERSTRING__3
 from col.vw_mle_in_transit po
  left join COLUMBIA_SUPPLIER_CODES on USERSTRING__2 = COLUMBIA_SUPPLIER_CODES.keyValue
 where not _deleted
""")
vw_mle_in_transit.createOrReplaceTempView('vw_mle_in_transit')

# COMMAND ----------

col_purchase_orders1=spark.sql("""
select 
  po.PROD_CD Item_ID,
  'DC819' Location_ID,
  po.PO_NUM PO_Order_ID,
  po.PO_Line_Number,
  'DC819-F' Flow_Resource_ID,
  nvl(lm.GlobalLocationCode, '#Error41-' || po.USERSTRING__2) as Vendor_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || po.USERSTRING__2) as Vendor_Flow_Resource_ID,
  po.DUE_DATE as Order_Due_Date ,
  '0' Order_Status_Code,
  ORIG_QTY Order_Quantity,
  0 Ship_Quantity,
  0 Received_Quantity,
  po.PO_NUM User_Text_1,
  po.DUE_DATE PO_Ship_Date,
  '0' Pseudo_Indicator,
  add_months(current_date, -1)  Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  vw_mle_open_orders po
  left join locationmaster lm on po.USERSTRING__2 = lm.KINGDEESUPPLIERCODE
  join pdhProduct pr on po.PROD_CD = pr.productCode
where
  1=1
  and nvl(lm.GlobalLocationCode , 'EM') like 'EM%'
  and orig_qty <> 0

union

select 
  po.PROD_CD Item_ID,
  'DC819' Location_ID,
  po.PO_NUM PO_Order_ID,
  po.PO_Line_Number,
  'DC819-F' Flow_Resource_ID,
  nvl(lm.GlobalLocationCode, '#Error41-' || po.USERSTRING__2) as Vendor_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || po.USERSTRING__2) as Vendor_Flow_Resource_ID,
  po.DUE_DATE as Order_Due_Date ,
  '1' Order_Status_Code,
  ORIG_QTY Order_Quantity,
  ORIG_QTY Ship_Quantity,
  0 Received_Quantity,
  po.PO_NUM User_Text_1,
  po.DUE_DATE PO_Ship_Date,
  '0' Pseudo_Indicator,
  add_months(current_date, -1)  Available_Inventory_Date,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  vw_mle_in_transit po
  left join locationmaster lm on po.USERSTRING__2 = lm.KINGDEESUPPLIERCODE
  join pdhProduct pr on po.PROD_CD = pr.productCode
where
  1=1
  and nvl(lm.GlobalLocationCode , 'EM') like 'EM%'
  and orig_qty <> 0
  
""")
col_purchase_orders1.createOrReplaceTempView('col_purchase_orders1')

# COMMAND ----------

col_transfer_orders1=spark.sql("""
select 
  po.PROD_CD Item_ID,
  'DC819' Destination_Location_ID,
  po.PO_NUM Transfer_Order_ID,
  po.PO_Line_Number Transfer_Order_Line_Number,
  nvl(lm.GlobalLocationCode, '#Error41-' || po.USERSTRING__2) as Source_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || po.USERSTRING__2) as Source_Flow_Resource,
  'DC819-F' Destination_Flow_Resource,
  po.DUE_DATE as Order_Due_Date ,
  po.DUE_DATE as Order_Ship_Date,
  '0' Order_Status_Code,
  ORIG_QTY Order_Quantity,
  0 Ship_Quantity,
  0 Received_Quantity,
  po.PO_NUM User_Text_1,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  vw_mle_open_orders po
  left join locationmaster lm on po.USERSTRING__2 = lm.KINGDEESUPPLIERCODE
  join pdhProduct pr on po.PROD_CD = pr.productCode
where
  1=1
  and left(nvl(lm.GlobalLocationCode , 'PL'),2) in  ('PL', 'DC', 'DS')

union

select 
  po.PROD_CD Item_ID,
  'DC819' Destination_Location_ID,
  po.PO_NUM Transfer_Order_ID,
  po.PO_Line_Number Transfer_Order_Line_Number,
  nvl(lm.GlobalLocationCode, '#Error41-' || po.USERSTRING__2) as Vendor_Location_ID,
  nvl(lm.GlobalLocationCode || '-F', '#Error41-' || po.USERSTRING__2) as Vendor_Flow_Resource_ID,
  'DC819-F' Destination_Flow_Resource,
  po.DUE_DATE as Order_Due_Date,
  po.DUE_DATE as Order_Ship_Date,
  '1' Order_Status_Code,
  ORIG_QTY Order_Quantity,
  ORIG_QTY Ship_Quantity,
  0 Received_Quantity,
  po.PO_NUM User_Text_1,
  DATE_ADD(current_date, nvl(pr.shelfLife,0)) Expire_Date
from 
  vw_mle_in_transit po
  left join locationmaster lm on po.USERSTRING__2 = lm.KINGDEESUPPLIERCODE
  join pdhProduct pr on po.PROD_CD = pr.productCode
where
  1=1
  and left(nvl(lm.GlobalLocationCode , 'PL'),2) in  ('PL', 'DC', 'DS')
  
""")
col_transfer_orders1.createOrReplaceTempView('col_transfer_orders1')

# COMMAND ----------

# col_purchase_orders3=spark.sql("""
# select
#   nvl(main_MPX_L0.SuccessorCode, po.Item_ID) Item_ID,
#   Location_ID,
#   PO_Order_ID,
#   PO_Line_Number,
#   Flow_Resource_ID,
#   Vendor_Location_ID ,
#   Vendor_Flow_Resource_ID ,
#   Order_Due_Date ,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   PO_Ship_Date,
#   Pseudo_Indicator,
#   Available_Inventory_Date,
#   Expire_Date
# from 
#   col_purchase_orders2 po
# --   left join main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Location_ID = main_MPX_L0.DC
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Location_ID = main_MPX_L0.DC
# """)
# col_purchase_orders3.createOrReplaceTempView('col_purchase_orders3')
# # col_purchase_orders3.display()

# COMMAND ----------

# col_purchase_orders4=spark.sql("""
# select
#   coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, po.Item_id || '-#Error1' ) Item_ID,
#   Location_ID,
#   PO_Order_ID,
#   PO_Line_Number,
#   Flow_Resource_ID,
#   Vendor_Location_ID ,
#   Vendor_Flow_Resource_ID ,
#   Order_Due_Date ,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   PO_Ship_Date,
#   Pseudo_Indicator,
#   Available_Inventory_Date,
#   Expire_Date
# from 
#   col_purchase_orders3 po
#   left join pdhProduct on po.Item_id = pdhProduct.productCode 
# """)
# col_purchase_orders4.createOrReplaceTempView('col_purchase_orders4')
# # col_purchase_orders4.display()

# COMMAND ----------

# DBTITLE 1,60.3.33 build SPS_PURCHASE_ORDERS
# sps_purchase_orders_all = ( 
#   purchase_order_4.select('*')
#   .union(in_transit_5.select('*'))
#   .union(sap_purchase_orders.select('*'))
#   .union(kgd_purchase_orders4.select('*'))
#   .union(tot_purchase_orders4.select('*'))
#   .union(col_purchase_orders4.select('*'))
#   .union(sap_purchase_intransit_orders.select('*')))
# sps_purchase_orders_all.createOrReplaceTempView('sps_purchase_orders_all')

# COMMAND ----------

# DBTITLE 1,60.3.34 SPS_PURCHASE_ORDER - create intermediate file
# # SETUP PARAMETERS
# file_name = get_file_name('tmp_sps_purchase_order', 'dlt') 
# target_file = get_file_path(temp_folder, file_name)
# print(file_name)
# print(target_file)

# # LOAD
# sps_purchase_orders_all.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,60.4.1 SPS_TRANSFER_ORDER - Internal Plant Codes
plant_codes=spark.sql("""
select 'EBS' _SOURCE, EBSSUPPLIERCODE supplierNumber, GlobalLocationCode 
  from locationmaster 
where 
  nvl(EBSSUPPLIERCODE, 'N/A') not in ('N/A')
  and GlobalLocationCode like 'PL%'
""")
plant_codes.createOrReplaceTempView('plant_codes')

# COMMAND ----------

# DBTITLE 1,60.4.2 SPS_TRANSFER_ORDER - internal purchase orders main
# internal_purchase_order_1=spark.sql("""
# select 
#     msi.segment1 Item_ID,
#    'DC' || mp.organization_code Destination_Location_ID,
#    poh.segment1 Transfer_Order_ID,
#    int(pla.line_num) Transfer_Order_Line_Number,
#      plant_codes.GlobalLocationCode Source_Location_ID,
#    plant_codes.GlobalLocationCode || '-F' Source_Flow_Resource,
#    'DC' || mp.organization_code || '-F'  Destination_Flow_Resource,
#    'DEFAULT_VEHICLE' Vehicle_Type, 
#    'AIR' Transport_Service,
#    nvl(pll.promised_date, pll.need_by_date) Order_Due_Date,
#    nvl(pll.promised_date, pll.need_by_date) Order_Ship_Date,
#    '0' Order_Status_Code,
#    case when   uom.UOM_CODE	 <> MSI.ATTRIBUTE15
#                      then round((pll.quantity - pll.quantity_received)  * nvl(conv_from_item.conversion_rate, conv_from.conversion_rate) / nvl(conv_to_item.conversion_rate, conv_to.conversion_rate),3)
#                      when  uom.UOM_CODE = MSI.ATTRIBUTE15 then  pll.quantity - pll.quantity_received
#    end as Order_Quantity,
#    0 Ship_Quantity,
#    0 Received_Quantity,
#    poh.segment1 User_Text_1,
#    DATE_ADD(poh.creation_date, int(msi.SHELF_LIFE_DAYS)) Expire_Date
# from
#   ebs.PO_HEADERS_ALL poh
#    join ebs.po_lines_all pla on poh.po_header_id = pla.po_header_id
#   join ebs.mtl_system_items_b msi on msi.inventory_item_id = pla.item_id
#   join ebs.mtl_parameters mp on msi.organization_id = mp.organization_id
#   join ebs.po_line_locations_all pll on pll.po_line_id = pla.po_line_id
#   join ebs.ap_suppliers pv on poh.vendor_id=pv.vendor_id
#   join plant_codes on pv.segment1 = plant_codes.supplierNumber
#   join ebs.MTL_UNITS_OF_MEASURE_TL uom on pll.unit_meas_lookup_code = uom.UNIT_OF_MEASURE
#   left join conv_from_item on  msi.segment1 = conv_from_item.segment1
# 						and uom.UOM_CODE = conv_from_item.uom_code 
#   left join conv_from on uom.UOM_CODE = conv_from.uom_code   
#   left join conv_to_item on msi.segment1 = conv_to_item.segment1 
# 						and msi.attribute15 = CONV_TO_ITEM.uom_code
#   left join conv_to on msi.attribute15 = conv_to.uom_code
# where 1=1
#   and msi.organization_id = mp.organization_id
#   AND pll.ship_to_organization_id = mp.organization_id
#   and pla.cancel_flag <> 'Y'
#   and mp.organization_code<>'325'
#   and (nvl(pll.closed_code,'OPEN')) in ('OPEN')
#   and uom.language = 'US'
# """)
# internal_purchase_order_1.createOrReplaceTempView('internal_purchase_order_1')
# # internal_purchase_order_1.display()

# COMMAND ----------

# DBTITLE 1,60.4.3 SPS_TRANSFER_ORDER internal- Apply historical product substitution
# internal_purchase_order_2=spark.sql("""
# select 
#   nvl(HistoricalProductSubstitution.final_Successor, po.Item_ID) Item_ID,
#   HistoricalProductSubstitution.final_Successor, po.Item_ID o,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   internal_purchase_order_1 po
#   left join HistoricalProductSubstitution on po.Item_id = HistoricalProductSubstitution.PROD_CD and po.Destination_Location_ID = HistoricalProductSubstitution.DC
#   join (select globallocationCode from locationmaster) lm on po.Destination_Location_ID = lm.globallocationCode
# where nvl(HistoricalProductSubstitution.final_Successor, po.Item_ID) not in ('dis')
# """)
# internal_purchase_order_2.createOrReplaceTempView('internal_purchase_order_2')

# COMMAND ----------

# DBTITLE 1,60.4.4 SPS_TRANSFER_ORDER  internal - Apply Current product substitution
# internal_purchase_order_3=spark.sql("""
# select 
#   nvl(main_MPX_L0.SuccessorCode, po.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   internal_purchase_order_2 po
# --   left join main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Destination_Location_ID = main_MPX_L0.DC
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Destination_Location_ID = main_MPX_L0.DC
# """)
# internal_purchase_order_3.createOrReplaceTempView('internal_purchase_order_3')

# COMMAND ----------

# DBTITLE 1,60.4.5 SPS_TRANSFER_ORDER internal- Check if in PDH
# internal_purchase_order_4=spark.sql("""
# select 
#     coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, po.Item_id || '-#Error1' ) Item_ID,
#     Destination_Location_ID,
#     Transfer_Order_ID,
#     Transfer_Order_Line_Number,
#     Source_Location_ID,
#     Source_Flow_Resource,
#     Destination_Flow_Resource,
#     Vehicle_Type, 
#     Transport_Service,
#     Order_Due_Date,
#     Order_Ship_Date,
#     Order_Status_Code,
#     Order_Quantity,
#     Ship_Quantity,
#     Received_Quantity,
#     User_Text_1,
#     Expire_Date
# from 
#   internal_purchase_order_3 po
#   left join pdhProduct on po.Item_id = pdhProduct.productCode 
# """)
# internal_purchase_order_4.createOrReplaceTempView('internal_purchase_order_4')

# COMMAND ----------

# DBTITLE 1,60.4.6 Intransit  - Add internal Vendor Code
# internal_in_transit_2=spark.sql("""
# select 
#   it.Item_ID,
#   it.Location_ID Destination_Location_ID,
#   it.PO_Order_ID Transfer_Order_ID,
#   it.PO_Line_Number Transfer_Order_Line_Number,
#   plant_codes.GlobalLocationCode Source_Location_ID,
#   plant_codes.GlobalLocationCode || '-F' Source_Flow_Resource,
#   it.Flow_Resource_ID Destination_Flow_Resource,
#   'DEFAULT_VEHICLE' Vehicle_Type, 
#   'AIR' Transport_Service,
#   it.Order_Due_Date,
#   '' Order_Ship_Date,
#   it.Order_Status_Code,
#   it.Order_Quantity,
#   it.Ship_Quantity,
#   it.Received_Quantity,
#   it.User_Text_1,
#   poh.creation_date Expire_Date
# from 
#   in_transit_1 it
#   join ebs.po_headers_all poh on it.PO_Order_ID = poh.segment1
#   join ebs.ap_suppliers pv on poh.vendor_id=pv.vendor_id
#   join plant_codes on pv.segment1 = plant_codes.supplierNumber
# """)
# internal_in_transit_2.createOrReplaceTempView('internal_in_transit_2')
# # internal_in_transit_2.display()

# COMMAND ----------

# DBTITLE 1,60.4.7 Intransit  internal - Apply historical product substitution
# internal_in_transit_3=spark.sql("""
# select 
#   nvl(HistoricalProductSubstitution.PROD_CD, it.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   internal_in_transit_2 it
#   left join HistoricalProductSubstitution on it.Item_id = HistoricalProductSubstitution.PROD_CD and it.Destination_Location_ID = HistoricalProductSubstitution.DC
# """)
# internal_in_transit_3.createOrReplaceTempView('internal_in_transit_3')

# COMMAND ----------

# DBTITLE 1,60.4.8 Intransit  internal - Apply Current product substitution
# internal_in_transit_4=spark.sql("""
# select 
#   nvl(main_MPX_L0.SuccessorCode, it.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   internal_in_transit_3 it
# --   left join main_MPX_L0 on it.Item_id = main_MPX_L0.predecessorCode and it.Destination_Location_ID = main_MPX_L0.DC
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on it.Item_id = main_MPX_L0.predecessorCode and it.Destination_Location_ID = main_MPX_L0.DC
# """)
# internal_in_transit_4.createOrReplaceTempView('internal_in_transit_4')

# COMMAND ----------

# DBTITLE 1,60.4.9 Intransit internal - Check if in PDH
# internal_in_transit_5=spark.sql("""
# select 
#     coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, it.Item_id || '-#Error1' ) Item_ID,
#     Destination_Location_ID,
#     Transfer_Order_ID,
#     Transfer_Order_Line_Number,
#     Source_Location_ID,
#     Source_Flow_Resource,
#     Destination_Flow_Resource,
#     Vehicle_Type, 
#     Transport_Service,
#     Order_Due_Date,
#     Order_Ship_Date,
#     Order_Status_Code,
#     Order_Quantity,
#     Ship_Quantity,
#     Received_Quantity,
#     User_Text_1,
#     Expire_Date
# from 
#   internal_in_transit_4 it
#   left join pdhProduct on it.Item_id = pdhProduct.productCode 
# """)
# internal_in_transit_5.createOrReplaceTempView('internal_in_transit_5')

# COMMAND ----------

# DBTITLE 1,60.4.10 SAP Transfer Orders
# sap_transfer_orders=spark.sql("""
# select 
#   sap_po.matnr Item_ID,
#   DC Destination_Location_ID,
#   sap_po.ebeln Transfer_Order_ID,
#   cast(sap_po.ebelp as integer) Transfer_Order_Line_Number,
# --   SAP_plant_codes.globalLocationCode  Source_Location_ID,
# --   SAP_plant_codes.globalLocationCode || '-F' Source_Flow_Resource,
  
  
#   nvl(SAP_plant_codes.globalLocationCode, '#Error28-' || sap_po_vendor.lifnr)  Vendor_Location_ID, 
#   nvl(SAP_plant_codes.globalLocationCode || '-F', '#Error28-' || sap_po_vendor.lifnr)  Vendor_Flow_Resource_ID,
  
  
#   sap_po.DC || '-F' Destination_Flow_Resource,
#   'DEFAULT_VEHICLE' Vehicle_Type, 
#   'AIR' Transport_Service,
#   sap_po.dueDate Order_Due_Date,
#   sap_po.dueDate as Order_Ship_Date,
#   '0' Order_Status_Code,
#   sap_po.orderQuantity - nvl(sap_shipped_po.shippedQuantity,0) Order_Quantity , 
#   0 Ship_Quantity,
#   0 Received_Quantity, 
#   sap_po.ebeln User_Text_1,
#   sap_po.Expire_Date Expire_Date
# from 
#   sap_po2 sap_po
#   left join sap_shipped_po on sap_po.mandt = sap_shipped_po.mandt 
#     and sap_po.ebeln = sap_shipped_po.ebeln
#     and sap_po.ebelp = sap_shipped_po.ebelp
#   join sap_po_vendor on sap_po.mandt = sap_po_vendor.mandt
#     and sap_po.ebeln = sap_po_vendor.ebeln
#   left join SAP_plant_codes on cast(cast(sap_po_vendor.lifnr as integer) as string) = SAP_plant_codes.supplierNumber
#   join pdhProduct on sap_po.matnr = pdhProduct.productCode
# where sap_po.orderQuantity - nvl(sap_shipped_po.shippedQuantity,0) <> 0
# """)
# sap_transfer_orders.createOrReplaceTempView('sap_transfer_orders')


# COMMAND ----------

# DBTITLE 1,60.4.11 SAP Transfer Intransit
# sap_transfer_intransit_orders=spark.sql("""
# select 
#     sap_po.matnr Item_ID,
#     sap_po.DC Destination_Location_ID,
#     sap_po.ebeln Transfer_Order_ID,
#     cast(sap_po.ebelp as integer) Transfer_Order_Line_Number,
#     nvl(SAP_plant_codes.globalLocationCode, '#Error28-' || sap_po_vendor.lifnr)  Vendor_Location_ID, 
#     nvl(SAP_plant_codes.globalLocationCode || '-F', '#Error28-' || sap_po_vendor.lifnr)  Vendor_Flow_Resource_ID,
#     sap_po.DC || '-F' Destination_Flow_Resource,
#     'DEFAULT_VEHICLE' Vehicle_Type, 
#     'AIR' Transport_Service,
#     sap_po.dueDate Order_Due_Date,
#     null as Order_Ship_Date,
#     '0' Order_Status_Code,
#     sap_shipped_po.shippedQuantity - nvl(sap_receipts.receivedQuantity,0) order_Quantity,
#     0 Ship_Quantity,
#     cast(0 as integer) as Received_Quantity,
#     sap_po.ebeln User_Text_1,
#     sap_po.Expire_Date Expire_Date
# from 
#     sap_po2 sap_po
#     join sap_shipped_po on sap_po.mandt = sap_shipped_po.mandt 
#       and sap_po.ebeln = sap_shipped_po.ebeln
#       and sap_po.ebelp = sap_shipped_po.ebelp
#     left join sap_receipts on sap_po.mandt = sap_receipts.mandt 
#       and sap_po.ebeln = sap_receipts.vgbel
#       and sap_po.ebelp = sap_receipts.vgpos
#     join sap_po_vendor on sap_po.mandt = sap_po_vendor.mandt
#       and sap_po.ebeln = sap_po_vendor.ebeln
#     left join SAP_plant_codes on cast(cast(sap_po_vendor.lifnr as integer) as string) = SAP_plant_codes.supplierNumber
#     join pdhProduct on sap_po.matnr = pdhProduct.productCode
#     where 
#       sap_shipped_po.shippedQuantity <> 0
#       and sap_shipped_po.shippedQuantity - nvl(sap_receipts.receivedQuantity,0) <> 0
# """)
# sap_transfer_intransit_orders.createOrReplaceTempView('sap_transfer_intransit_orders')

# COMMAND ----------

# tot_transfer_orders1=spark.sql("""
# select
#   pr.Oracle_cod Item_ID,
#   Case 
#     when it.company = '5210' then 'DCABL'
#     else 'DCHER' 
#   end Destination_Location_ID,
#   it.PO_NUM Transfer_Order_ID,
#   it.LINE_NUM Transfer_Order_Line_Number,
#   nvl(lm.GlobalLocationCode, '#Error39-' || it.USERSTRIN_2) as Source_Location_ID,
#   nvl(lm.GlobalLocationCode || '-F', '#Error39-' || it.USERSTRIN_2) as Source_Flow_Resource,
#   Case 
#     when it.company = '5210' then 'DCABL-F'
#     else 'DCHER-F' 
#   end Destination_Flow_Resource,
#   'DEFAULT_VEHICLE' Vehicle_Type, 
#   'AIR' Transport_Service,
#   IT.DUE_DATE as Order_Due_Date,
#   IT.DUE_DATE as Order_Ship_Date,
#   '1' Order_Status_Code,
#   QTT_CONVERTED  Order_Quantity,
#   case when USERSTRIN_0 = 'OPEN PO'
#     then 0
#     else QTT_CONVERTED 
#   end Ship_Quantity,
#   0 Received_Quantity,
#   it.PO_NUM User_Text_1,
#   IT.DUE_DATE Expire_Date
# from 
#   tot.tb_PBI_transit it
#   join tot.tb_PBI_product pr on it.PROD_CD = pr.Product_cod
#   left join locationmaster lm on it.USERSTRIN_2 = lm.MICROSIGASUPPLIERCODE
# where
#   not it._deleted
#   and not pr._deleted
#   and it.COMPANY = '5210'
#   and nvl(lm.GlobalLocationCode, 'PL') like 'PL%'

# """)
# tot_transfer_orders1.createOrReplaceTempView('tot_transfer_orders1')
# # tot_transfer_orders1.display()

# COMMAND ----------

# tot_transfer_orders2=spark.sql("""
# select
#   nvl(HistoricalProductSubstitution.final_Successor, po.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   tot_transfer_orders1 po
#   left join HistoricalProductSubstitution on po.Item_id = HistoricalProductSubstitution.PROD_CD and po.Destination_Location_ID = HistoricalProductSubstitution.DC
# where nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
# """)
# tot_transfer_orders2.createOrReplaceTempView('tot_transfer_orders2')
# # tot_transfer_orders2.display()

# COMMAND ----------

# tot_transfer_orders3=spark.sql("""
# select
#   nvl(main_MPX_L0.SuccessorCode, po.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   tot_transfer_orders2 po
# --   left join main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Destination_Location_ID = main_MPX_L0.DC
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Destination_Location_ID = main_MPX_L0.DC
# """)
# tot_transfer_orders3.createOrReplaceTempView('tot_transfer_orders3')
# # tot_transfer_orders3.display()

# COMMAND ----------

# tot_transfer_orders4=spark.sql("""
# select
#   coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, po.Item_id || '-#Error1' ) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   tot_transfer_orders3 po
#   left join pdhProduct on po.Item_id = pdhProduct.productCode 
# """)
# tot_transfer_orders4.createOrReplaceTempView('tot_transfer_orders4')
# # tot_transfer_orders4.display()

# COMMAND ----------

# kgd_transfer_orders1=spark.sql("""
# select 
#   pr.ASPN Item_ID,
#   'DCASWH' Destination_Location_ID,
#   poh.PONumber Transfer_Order_ID,
#   pol.FEntryID Transfer_Order_Line_Number,
#   nvl(lm.GlobalLocationCode, '#Error40-' || poh.fnumber) as Source_Location_ID,
#   nvl(lm.GlobalLocationCode || '-F', '#Error40-' || poh.fnumber) as Source_Flow_Resource,
#   'DCASWH-F' Destination_Flow_Resource,
#   'DEFAULT_VEHICLE' Vehicle_Type, 
#   'AIR' Transport_Service,
#   nvl(pol.CETD, pol.ShipDate) as Order_Due_Date,
#   pol.ShipDate as Order_Ship_Date,
#   '0' Order_Status_Code,
#   FQty - FCommitQty  Order_Quantity,
#   0 Ship_Quantity,
#   0 Received_Quantity,
#   poh.PONumber User_Text_1,
#   pol.ShipDate Expire_Date
# from 
#   kgd.dbo_tembo_poorder poh
#   join kgd.dbo_tembo_poorderentry pol on poh.finterid = pol.finterid
#   join kgd.dbo_tembo_product pr on pol.itemnumber = pr.productCode
#   left join locationmaster lm on poh.fnumber = lm.KINGDEESUPPLIERCODE
# where
#   not pol._deleted
#   and not poh._deleted
#   and not pr._deleted
#   and FQty - FCommitQty > 0
#   and linestatus not in ('Cancelled')
#   and lm.GlobalLocationCode like 'PL%'
# """)
# kgd_transfer_orders1.createOrReplaceTempView('kgd_transfer_orders1')
# # kgd_transfer_orders1.display()

# COMMAND ----------

# kgd_transfer_orders2=spark.sql("""
# select
#   nvl(HistoricalProductSubstitution.final_Successor, po.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   kgd_transfer_orders1 po
#   left join HistoricalProductSubstitution on po.Item_id = HistoricalProductSubstitution.PROD_CD and po.Destination_Location_ID = HistoricalProductSubstitution.DC
# where nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
# """)
# kgd_transfer_orders2.createOrReplaceTempView('kgd_transfer_orders2')
# # kgd_transfer_orders2.display()

# COMMAND ----------

# kgd_transfer_orders3=spark.sql("""
# select
#   nvl(main_MPX_L0.SuccessorCode, po.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   kgd_transfer_orders2 po
# --   left join main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Destination_Location_ID = main_MPX_L0.DC
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Destination_Location_ID = main_MPX_L0.DC
# """)
# kgd_transfer_orders3.createOrReplaceTempView('kgd_transfer_orders3')
# # kgd_transfer_orders3.display()

# COMMAND ----------

# kgd_transfer_orders4=spark.sql("""
# select
#   coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, po.Item_id || '-#Error1' ) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   kgd_transfer_orders3 po
#   left join pdhProduct on po.Item_id = pdhProduct.productCode 
# """)
# kgd_transfer_orders4.createOrReplaceTempView('kgd_transfer_orders4')
# # kgd_transfer_orders3.display()

# COMMAND ----------

# col_transfer_orders1=spark.sql("""
# select 
#   po.PROD_CD Item_ID,
#   'DC819' Destination_Location_ID,
#   po.PO_NUM Transfer_Order_ID,
#   po.PROD_CD Transfer_Order_Line_Number,
#   nvl(lm.GlobalLocationCode, '#Error41-' || po.USERSTRING__2) as Source_Location_ID,
#   nvl(lm.GlobalLocationCode || '-F', '#Error41-' || po.USERSTRING__2) as Source_Flow_Resource,
#   'DC819-F' Destination_Flow_Resource,
#   'DEFAULT_VEHICLE' Vehicle_Type, 
#   'AIR' Transport_Service,
#   po.DUE_DATE as Order_Due_Date ,
#   null as Order_Ship_Date,
#   '0' Order_Status_Code,
#   ORIG_QTY Order_Quantity,
#   0 Ship_Quantity,
#   0 Received_Quantity,
#   po.PO_NUM User_Text_1,
#   po.DUE_DATE Expire_Date
# from 
#   col.vw_mle_open_orders po
#   left join locationmaster lm on po.USERSTRING__2 = lm.KINGDEESUPPLIERCODE
# where
#   not po._deleted
#   and nvl(lm.GlobalLocationCode , 'PL') like 'PL%'

# union

# select 
#   po.PROD_CD Item_ID,
#   'DC819' Destination_Location_ID,
#   po.PO_NUM Transfer_Order_ID,
#   po.PROD_CD Transfer_Order_Line_Number,
#   nvl(lm.GlobalLocationCode, '#Error41-' || po.USERSTRING__2) as Vendor_Location_ID,
#   nvl(lm.GlobalLocationCode || '-F', '#Error41-' || po.USERSTRING__2) as Vendor_Flow_Resource_ID,
#   'DC819-F' Destination_Flow_Resource,
#   'DEFAULT_VEHICLE' Vehicle_Type, 
#   'AIR' Transport_Service,
#   po.DUE_DATE as Order_Due_Date,
#   null as Order_Ship_Date,
#   '1' Order_Status_Code,
#   ORIG_QTY Order_Quantity,
#   ORIG_QTY Ship_Quantity,
#   0 Received_Quantity,
#   po.PO_NUM User_Text_1,
#   po.DUE_DATE Expire_Date
# from 
#   col.vw_mle_in_transit po
#   left join locationmaster lm on po.USERSTRING__2 = lm.KINGDEESUPPLIERCODE
# where
#   not po._deleted
#   and nvl(lm.GlobalLocationCode , 'PL') like 'PL%'
  
# """)
# col_transfer_orders1.createOrReplaceTempView('col_transfer_orders1')
# # col_transfer_orders1.display()

# COMMAND ----------

# col_transfer_orders2=spark.sql("""
# select
#   nvl(HistoricalProductSubstitution.final_Successor, po.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   col_transfer_orders1 po
#   left join HistoricalProductSubstitution on po.Item_id = HistoricalProductSubstitution.PROD_CD and po.Destination_Location_ID = HistoricalProductSubstitution.DC
# where nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
# """)
# col_transfer_orders2.createOrReplaceTempView('col_transfer_orders2')
# # col_transfer_orders2.display()

# COMMAND ----------

# col_transfer_orders3=spark.sql("""
# select
#   nvl(main_MPX_L0.SuccessorCode, po.Item_ID) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   nvl(Order_Ship_Date,Order_Due_Date) Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   col_transfer_orders2 po
# --   left join main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Destination_Location_ID = main_MPX_L0.DC
#   left join (select max(successorcode) successorcode, predecessorcode, dc from main_mpx_l0 group by predecessorcode, dc) main_MPX_L0 on po.Item_id = main_MPX_L0.predecessorCode and po.Destination_Location_ID = main_MPX_L0.DC
# """)
# col_transfer_orders3.createOrReplaceTempView('col_transfer_orders3')
# # col_transfer_orders3.display()

# COMMAND ----------

# col_transfer_orders4=spark.sql("""
# select
#   coalesce(pdhProduct.e2eProductCode,pdhProduct.productCode, po.Item_id || '-#Error1' ) Item_ID,
#   Destination_Location_ID,
#   Transfer_Order_ID,
#   Transfer_Order_Line_Number,
#   Source_Location_ID,
#   Source_Flow_Resource,
#   Destination_Flow_Resource,
#   Vehicle_Type, 
#   Transport_Service,
#   Order_Due_Date,
#   nvl(Order_Ship_Date, Order_Due_Date) Order_Ship_Date,
#   Order_Status_Code,
#   Order_Quantity,
#   Ship_Quantity,
#   Received_Quantity,
#   User_Text_1,
#   Expire_Date
# from 
#   col_transfer_orders3 po
#   left join pdhProduct on po.Item_id = pdhProduct.productCode 
# """)
# col_transfer_orders4.createOrReplaceTempView('col_transfer_orders4')
# # col_transfer_orders4.display()

# COMMAND ----------

# DBTITLE 1,60.4.12 build SPS_TRANSFER_ORDERS
# sps_transfer_orders_all = (
#   internal_purchase_order_4.select('*')
#   .union(internal_in_transit_5.select('*'))
#   .union(sap_transfer_orders.select('*'))
#   .union(sap_transfer_intransit_orders.select('*'))
#   .union(kgd_transfer_orders4.select('*'))
#   .union(tot_transfer_orders4.select('*'))
#   .union(col_transfer_orders4.select('*'))
# )
# sps_transfer_orders_all.createOrReplaceTempView('sps_transfer_orders_all')

# COMMAND ----------



# COMMAND ----------

sap_purchase_orders_final=spark.sql("""
with q1 as
(select * from ebs_purchase_orders_em
union 
select * from ds3
union 
select * from tot_purchase_orders1
union 
select * from kgd_purchase_orders1
union 
select * from col_purchase_orders1
union
select * from sap_purchase_orders_temp
)
select 
   coalesce(main_mpx_l0.SuccessorCode, HistoricalProductSubstitution.final_Successor, q1.Item_ID) Item_ID,
   q1.Location_ID,
   q1.PO_Order_ID,
   q1.PO_Line_Number,
   q1.Flow_Resource_ID,
   q1.Vendor_Location_ID,
   q1.Vendor_Flow_Resource_ID,
   case 
    when q1.Order_Due_Date < current_date 
      then 
        current_date - 1
    else 
       q1.Order_Due_Date
   end Order_Due_Date,
   q1.Order_Status_Code,
   q1.Order_Quantity,
   q1.Ship_Quantity,
   q1.Received_Quantity,
   q1.User_Text_1,
   q1.PO_Ship_Date,
   q1.Pseudo_Indicator,
   q1.Available_Inventory_Date,
   case 
     when 
       q1.Expire_Date > add_months(current_date, 18) 
         then add_months(current_date, 18) + 186
         else 
           q1.Expire_Date
   end Expire_Date
from 
  q1 
  left join HistoricalProductSubstitution on q1.Item_id = HistoricalProductSubstitution.PROD_CD and q1.Location_ID = HistoricalProductSubstitution.DC
  left join (select distinct predecessorCode, DC, SuccessorCode from main_mpx_l0) main_mpx_l0 on q1.Item_id = main_mpx_l0.predecessorCode and q1.Location_ID = main_mpx_l0.DC
  join locationmaster lm on q1.Location_ID = lm.globalLocationCode
where 
  nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
  and year(q1.Order_Due_Date) * 100 + month(q1.Order_Due_Date) <= (select toPeriod from latestOrderPeriod)
""")
sap_purchase_orders_final.createOrReplaceTempView('sap_purchase_orders_final')

# COMMAND ----------

sap_transfer_orders_final=spark.sql("""
with q1 as
(--select * from sap_purchase_orders_PL
--union 
--select * from sap_transit_orders_PL
--union
select * from ebs_purchase_orders_pl
union
select * from ds4
union
select * from tot_transfer_orders1
union
select * from kgd_transfer_orders1
union 
select * from col_transfer_orders1
union
select * from sap_transfer_orders_temp)
select
  coalesce(main_mpx_l0.SuccessorCode, HistoricalProductSubstitution.final_Successor, q1.Item_ID) Item_ID,
  q1.Destination_Location_ID,
  q1.Transfer_Order_ID,
  q1.Transfer_Order_Line_Number,
  q1.Source_Location_ID,
  q1.Source_Flow_Resource,
  q1.Destination_Flow_Resource,
  case 
    when q1.Order_Due_Date < current_date 
      then 
        current_date - 1
    else 
       q1.Order_Due_Date
  end Order_Due_Date,
  case 
    when 
      q1.Order_Ship_Date < current_date 
       then current_date - 1
    else
      q1.Order_Ship_Date
  end Order_Ship_Date,
  q1.Order_Status_Code,
  q1.Order_Quantity, 
  q1.Ship_Quantity,
  q1.Received_Quantity,
  q1.User_Text_1,
  case 
     when 
       q1.Expire_Date > add_months(current_date, 18) 
         then add_months(current_date, 18) + 186
         else 
           q1.Expire_Date
  end Expire_Date
from q1
left join HistoricalProductSubstitution on q1.Item_id = HistoricalProductSubstitution.PROD_CD and q1.Destination_Location_ID = HistoricalProductSubstitution.DC
left join (select distinct predecessorCode, DC, SuccessorCode from main_mpx_l0) main_mpx_l0 on q1.Item_id = main_mpx_l0.predecessorCode and q1.Destination_Location_ID = main_mpx_l0.DC
join locationmaster lm on q1.Destination_Location_ID = lm.globalLocationCode
where 
  nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
  and year(q1.Order_Due_Date) * 100 + month(q1.Order_Due_Date) <= (select toPeriod from latestOrderPeriod)
""")
sap_transfer_orders_final.createOrReplaceTempView('sap_transfer_orders_final')

# COMMAND ----------

# DBTITLE 1,60.3.34 SPS_PURCHASE_ORDER - create intermediate file
# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_purchase_order', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sap_purchase_orders_final.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,60.4.13 SPS_TRANSFER_ORDER - create intermediate file
# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_transfer_order', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sap_transfer_orders_final.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

ip_purchase_orders = spark.sql("""
with ippo as (
      select '1' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from ebs_purchase_orders_em 
      where order_status_code = '0'
      group by Item_ID, Location_ID
      union 
      select  '2' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from ds3
      where order_status_code = '0'
      group by Item_ID, Location_ID
      union 
      select  '3' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from sap_purchase_orders_temp
      where order_status_code = '0'
      group by Item_ID, Location_ID
      union
      select  '4' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from ebs_purchase_orders_pl
      where order_status_code = '0'
      group by  Item_ID, Destination_Location_ID
      union
      select  '5' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from ds4
      where order_status_code = '0'
      group by  Item_ID, Destination_Location_ID
      union
      select  '6' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from sap_transfer_orders_temp
      where order_status_code = '0'
      group by  Item_ID, Destination_Location_ID
      union 
      select  '7' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from tot_purchase_orders1
      where order_status_code = '0'
      group by Item_ID, Location_ID
      union
      select  '8' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from kgd_purchase_orders1
      where order_status_code = '0'
      group by Item_ID, Location_ID
      union
      select  '9' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from col_purchase_orders1
      where order_status_code = '0'
      group by Item_ID, Location_ID
      union
      select  '10' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from tot_transfer_orders1
      where order_status_code = '0'
      group by  Item_ID, Destination_Location_ID
      union
      select  '10' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from kgd_transfer_orders1
      where order_status_code = '0'
      group by  Item_ID, Destination_Location_ID
      union
      select  '10' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from col_transfer_orders1
      where order_status_code = '0'
      group by  Item_ID, Destination_Location_ID
)
    select 
      source,
      Item_id, 
      Location_ID,
      sum(Order_Quantity) Order_Quantity
    from ippo
    join locationmaster lm on Location_ID = lm.globalLocationCode
    group by 
      Item_id, 
      Location_ID,
      source
""")
# ip_purchase_orders.display()
ip_purchase_orders.createOrReplaceTempView('ip_purchase_orders')

# COMMAND ----------

ip_intransit_orders=spark.sql("""
  with ippo as (
      select '1' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from ebs_purchase_orders_em 
      where order_status_code = '1'
      group by Item_ID, Location_ID
      union 
      select  '2' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from ds3
      where order_status_code = '1'
      group by Item_ID, Location_ID
      union 
      select  '3' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from sap_purchase_orders_temp
      where order_status_code = '1'
      group by Item_ID, Location_ID
      union
      select  '4' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from ebs_purchase_orders_pl
      where order_status_code = '1'
      group by  Item_ID, Destination_Location_ID
      union
      select  '5' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from ds4
      where order_status_code = '1'
      group by  Item_ID, Destination_Location_ID
      union
      select  '6' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from sap_transfer_orders_temp
      where order_status_code = '1'
      group by  Item_ID, Destination_Location_ID
      union 
      select  '7' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from tot_purchase_orders1
      where order_status_code = '1'
      group by Item_ID, Location_ID
      union
      select  '8' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from kgd_purchase_orders1
      where order_status_code = '1'
      group by Item_ID, Location_ID
      union
      select  '9' source, Item_ID, Location_ID, sum(Order_Quantity) Order_Quantity from col_purchase_orders1
      where order_status_code = '1'
      group by Item_ID, Location_ID
      union
      select  '10' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from tot_transfer_orders1
      where order_status_code = '1'
      group by  Item_ID, Destination_Location_ID
      union
      select  '10' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from kgd_transfer_orders1
      where order_status_code = '1'
      group by  Item_ID, Destination_Location_ID
      union
      select  '10' source, Item_ID, Destination_Location_ID Location_ID, sum(Order_Quantity) Order_Quantity from col_transfer_orders1
      where order_status_code = '1'
      group by  Item_ID, Destination_Location_ID
  )
    select 
      Item_id, 
      Location_ID,
      sum(Order_Quantity) Intransit_Quantity
    from ippo
    join locationmaster lm on Location_ID = lm.globalLocationCode
    group by 
      Item_id, 
      Location_ID
""")
# ip_intransit_orders.display()
ip_intransit_orders.createOrReplaceTempView('ip_intransit_orders')

# COMMAND ----------

inventory = spark.sql("""
 select
     nvl(pr.e2eproductcode, '#Error1-' || pr.e2eproductcode) Forecast_1_ID,
     nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode)  Forecast_2_ID,
     sum(inventory.ansStdQty) On_hand_quantity,
     0 On_Hold_quantity
   from
     s_supplychain.inventory_agg inventory
     join s_core.product_agg pr on inventory.item_id = pr._id
     join s_core.organization_agg comp
        ON inventory.owningBusinessUnit_ID = comp._id
     join s_core.organization_agg
        ON inventory.inventoryWarehouse_ID = organization_agg._id
     left join pdhProduct on pr.e2eproductcode = pdhProduct.productCode
     join locationmaster lm on nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode) = lm.GlobalLocationCode
     join s_core.product_org_agg pr_org on inventory.item_id = pr_org.item_id
       and inventory.inventoryWarehouse_ID = pr_org.organization_ID
   where 
     1=1
     and not pr._deleted
     and not comp._deleted
     and not organization_agg._deleted
     and organization_agg.isActive
     and comp.organizationCode not in (select organization from exclude_organizations)
     and year(inventory.inventoryDate) * 100 + month(inventory.inventoryDate) = (select runPeriod from run_period)
     and not inventory._deleted
     and inventory._source in (select sourceSystem from source_systems)
     and nvl(upper(pr.itemType), 'FINISHED GOODS') in ('FINISHED GOODS', 'FINISHED GOOD', 'FERT', 'ZPRF', 'ACCESSORIES')
     and inventory.subInventoryCode not in (select subInventoryCode from TEMBO_HOLD_SUBINVENTORY_CODES)
     and not case when pr_org.lotControlCode = 1 then current_date else nvl(inventory.lotExpirationDate, current_date) end < current_date
   group by
     nvl(pr.e2eproductcode, '#Error1-' || pr.e2eproductcode),
     nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode)
union all
  select
     nvl(pr.e2eproductcode, '#Error1-' || pr.e2eproductcode) Forecast_1_ID,
     nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode)  Forecast_2_ID,
     0 On_hand_quantity,
     sum(inventory.ansStdQty)  On_Hold_quantity
   from
     s_supplychain.inventory_agg inventory
     join s_core.product_agg pr on inventory.item_id = pr._id
     join s_core.organization_agg comp
        ON inventory.owningBusinessUnit_ID = comp._id
     join s_core.organization_agg
        ON inventory.inventoryWarehouse_ID = organization_agg._id
     left join pdhProduct on pr.e2eproductcode = pdhProduct.productCode
     join locationmaster lm on nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode) = lm.GlobalLocationCode
     join s_core.product_org_agg pr_org on inventory.item_id = pr_org.item_id
       and inventory.inventoryWarehouse_ID = pr_org.organization_ID
   where 
     1=1
     and not pr._deleted
     and not comp._deleted
     and not organization_agg._deleted
     and organization_agg.isActive
     and comp.organizationCode not in (select organization from exclude_organizations)
     and year(inventory.inventoryDate) * 100 + month(inventory.inventoryDate) = (select runPeriod from run_period)
     and not inventory._deleted
     and inventory._source in (select sourceSystem from source_systems)
     and nvl(upper(pr.itemType), 'FINISHED GOODS') in ('FINISHED GOODS', 'FINISHED GOOD', 'FERT', 'ZPRF', 'ACCESSORIES')
     and inventory.subInventoryCode in (select subInventoryCode from TEMBO_HOLD_SUBINVENTORY_CODES)
     and not case when pr_org.lotControlCode = 1 then current_date else nvl(inventory.lotExpirationDate, current_date) end < current_date
   group by
     nvl(pr.e2eproductcode, '#Error1-' || pr.e2eproductcode),
     nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode)
""")
inventory.createOrReplaceTempView('inventory')

# COMMAND ----------

IP_CDR_AS_INV = spark.sql("""
select 
  coalesce(main_mpx_l0.SuccessorCode, HistoricalProductSubstitution.final_Successor, q1.Forecast_1_ID) Forecast_1_ID,
  Forecast_2_ID,
  sum(On_hand_quantity) On_hand_quantity,
  sum(On_Hold_quantity) On_Hold_quantity
from inventory Q1
  join locationmaster lm on Forecast_2_id = lm.GlobalLocationCode
  left join HistoricalProductSubstitution on q1.Forecast_1_ID = HistoricalProductSubstitution.PROD_CD and q1.Forecast_2_ID = HistoricalProductSubstitution.DC
  left join (select distinct predecessorCode, DC, SuccessorCode from main_mpx_l0) main_mpx_l0 on q1.Forecast_1_ID = main_mpx_l0.predecessorCode and q1.Forecast_2_ID = main_mpx_l0.DC
where 
  nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
  and not (On_hand_quantity = 0 and On_Hold_quantity = 0)
group by 
   coalesce(main_mpx_l0.SuccessorCode, HistoricalProductSubstitution.final_Successor, q1.Forecast_1_ID),
   Forecast_2_ID   
""")
IP_CDR_AS_INV.createOrReplaceTempView('IP_CDR_AS_INV')

# COMMAND ----------

IP_CDR_AS = spark.sql("""
with cdr_as as(
select
--     '1' source,
    Forecast_1_ID,
    Forecast_2_ID,
    round(nvl(On_hand_quantity,0),0) On_hand_quantity,
    0 On_order_quantity,
    0 In_transit_quantity,
    round(nvl(On_Hold_quantity,0),0) On_Hold_quantity
from
  IP_CDR_AS_INV
where
  round(nvl(On_hand_quantity,0),0)  > 0
  or round(nvl(On_Hold_quantity,0),0) > 0

union 

select
--    '2' source,
   Item_ID Forecast_1_ID,
   Location_ID Forecast_2_ID, 
   0 On_hand_quantity,
   nvl(Order_quantity,0) On_order_quantity,
   0 In_transit_quantity,
   0 On_Hold_quantity
from 
    ip_purchase_orders
where
    nvl(Order_quantity,0) > 0

union

select 
--    '3' source,
   Item_ID Forecast_1_ID, 
   Location_ID Forecast_2_ID, 
   0 On_hand_quantity,
   0 On_order_quantity,
   nvl(intransit_quantity,0) intransit_quantity,
   0 On_Hold_quantity
from 
    ip_intransit_orders
where 
    nvl(intransit_quantity,0) > 0
)
select
   '2' Pyramid_Level,
   Forecast_1_ID,
   Forecast_2_ID, 
   'AS' Record_type,
   case when sum(On_hand_quantity) < 0 then sum(On_hand_quantity) * -1 else  sum(On_hand_quantity) end On_hand_quantity,
   case when sum(On_hand_quantity) < 0 then '-' else ' ' end On_hand_quantity_sign,
   case when sum(On_order_quantity)  < 0 then sum(On_order_quantity) * -1 else  sum(On_order_quantity)  end On_order_quantity,
   case when sum(On_order_quantity)  < 0 then '-' else ' ' end On_Order_quantity_sign,
   case when sum(In_transit_quantity) < 0 then sum(In_transit_quantity) * -1 else sum(In_transit_quantity) end In_transit_quantity,
   case when sum(In_transit_quantity) < 0 then '-' else ' ' end In_transit_quantity_sign,
   case when sum(On_Hold_quantity) < 0 then sum(On_Hold_quantity) * -1 else  sum(On_Hold_quantity) end On_Hold_quantity,
   case when sum(On_Hold_quantity) < 0 then '-' else ' ' end On_Hold_quantity_sign
 from 
   cdr_as
   join locationmaster lm on Forecast_2_ID = lm.GlobalLocationCode
 group by
   Forecast_1_ID,
   Forecast_2_ID
""")
target_file = get_file_path(temp_folder, 'IP_CDR_AS')
IP_CDR_AS.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
IP_CDR_AS = spark.read.format('delta').load(target_file)
IP_CDR_AS.createOrReplaceTempView('IP_CDR_AS')

# IP_CDR_AS.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_ip_cdr_as_l2', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
IP_CDR_AS.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

cdr_ai_for_cdr=spark.sql("""
with IP_CDR_AI as (
  select
    '5a' scenario,
    forecast_1_id,
    forecast_2_id,
  case 
          when forecast_2_id in ('DCABL', 'DCHER') then ('TOT')
          when forecast_2_id in ('DC819') then ('COL')
          when forecast_2_id in ('DCASWH') then ('KGD')
          when forecast_2_id like 'DCAAL%' or  forecast_2_id like 'DCANV%' then ('SAP')
          when forecast_2_id in ('DSEMEA', 'DSAPAC') then 'SAP'
          else 'EBS'
     end source 
  from
    ip_cdr_as
  where
    forecast_1_id || '-' || forecast_2_id not in (
      select
        distinct forecast_1_id || '-' || forecast_2_id
      from
        cdr_ai_semi_final)
  union
  select distinct
  '5b' scenario, 
  Parent_Item_ID forecast_1_id,
  Parent_Location forecast_2_id,
  case 
          when Parent_Location in ('DCABL', 'DCHER') then ('TOT')
          when Parent_Location in ('DC819') then ('COL')
          when Parent_Location in ('DCASWH') then ('KGD')
          when Parent_Location like 'DCAAL%' or  Parent_Location like 'DCANV%' then ('SAP')
          when Parent_Location in ('DSEMEA', 'DSAPAC') then 'SAP'
          else 'EBS'
     end source 
from df_ip_psl_max
where
    Parent_Item_ID || '-' || Parent_Location not in (
      select
        distinct forecast_1_id || '-' || forecast_2_id
      from
        cdr_ai_semi_final)
 union
  select distinct
  '5c' scenario, 
  Component_Item_ID forecast_1_id,
  Component_Location forecast_2_id,
  case 
          when Component_Location in ('DCABL', 'DCHER') then ('TOT')
          when Component_Location in ('DC819') then ('COL')
          when Component_Location in ('DCASWH') then ('KGD')
          when Component_Location like 'DCAAL%' or  Component_Location like 'DCANV%' then ('SAP')
          when Component_Location in ('DSEMEA', 'DSAPAC') then 'SAP'
          else 'EBS'
     end source 
from df_ip_psl_max
where
    Component_Item_ID || '-' || Component_Location not in (
      select
        distinct forecast_1_id || '-' || forecast_2_id
      from
        cdr_ai_semi_final)
)
select 
  scenario,
  '' _source,
  '2' Pyramid_Level,
  Forecast_1_id,
  Forecast_2_id,
  'AI' record_Type,
  case when lm_dc.defaultSourcingRule = 'Origin' 
    then lm.globallocationcode 
    else lm_dc.defaultSourcingRule
  end Source_Location_1,
  case when lm_dc.defaultSourcingRule = 'Origin' 
    then cast(lm.orderleadtime as numeric(3,0)) 
    else cast(lm_dc.orderleadtime as numeric(3,0))
  end Replenishment_lead_time_1,
  case when lm_dc.defaultSourcingRule = 'Origin'
    then lm.globallocationcode
    else lm_dc.defaultSourcingRule 
  end Vendor,
  'DEFAULT' DRP_planner,
  case 
    when lm.globallocationcode like 'PL%' then 'X'
    when lm.globallocationcode like 'EM%' then 'B'
  end Make_buy_code,
  nvl(cast(pdhProduct.ansStdUomConv as decimal(7,0)), '#Error31') Order_multiple,
  Cast(90 as numeric(3,0)) Network_level,
  '' pdhProductFlag,
  30 Order_Quantity_Days
from 
  IP_CDR_AI
  join pdhProduct on Forecast_1_id = pdhProduct.productCode
  join locationmaster lm on pdhProduct.originid = lm.origin
  join locationmaster lm_dc on forecast_2_id = lm_dc.globalLocationCode
where 1=1
and lm.mainorigin = 'Y'
""")
# cdr_ai_for_cdr.display()
cdr_ai_for_cdr.createOrReplaceTempView('cdr_ai_for_cdr')

# COMMAND ----------

# cdr_ai_final = (
#   cdr_ai_f.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days')
#   .union(cdr_ai_closing_add.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
#   .union(cdr_ai_for_cdr.select('scenario', '_source', 'Pyramid_Level', 'Forecast_1_ID','Forecast_2_ID', 'Record_type','Source_location_1', 'Replenishment_lead_time_1', 'Vendor',  'DRP_planner','Make_buy_code',  'Order_multiple',  'Network_level','pdhProductFlag', 'Order_Quantity_Days'  ))
# )
# cdr_ai_final.createOrReplaceTempView('cdr_ai_final')

# COMMAND ----------

cdr_ai_final=spark.sql("""
with q1 as
(
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
      pdhProductFlag,
      Order_Quantity_Days
    from 
      cdr_ai_f
union
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
      pdhProductFlag,
      Order_Quantity_Days
    from 
      cdr_ai_closing_add
union
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
      pdhProductFlag,
      Order_Quantity_Days
    from 
      cdr_ai_for_cdr
)
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
  pdhProductFlag,
  nvl(oqd.ParameterValue, q1.Order_Quantity_Days) Order_Quantity_Days
from 
  q1
  left join itemsourceexceptions_Order_Quantity_Days oqd on q1.forecast_1_id = oqd.item_number and q1.forecast_2_id = oqd.destination
  join locationmaster lm on Forecast_2_ID = lm.globalLocationCode
""")
cdr_ai_final.createOrReplaceTempView('cdr_ai_final')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_ip_cdr_ai_l2', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
cdr_ai_final.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

cdr_ai_final = spark.read.format('delta').load(target_file)
cdr_ai_final.createOrReplaceTempView('cdr_ai_final')

# COMMAND ----------

loaded_items_L2=spark.sql("""
select distinct forecast_1_id || '-' ||  split(Forecast_2_ID, '_')[1]  key
  from 
 g_tembo.lom_aa_do_l1_hist_archive -- to be replaced with archive table
where 
  forecast_1_id not like '%#Error%'
  and forecast_2_id not like '%#Error%'
union 
select distinct forecast_1_id || '-' ||  split(Forecast_2_ID, '_')[1]  key
  from 
 g_tembo.lom_aa_do_l1_archive -- to be replaced with archive table
where 
  forecast_1_id not like '%#Error%'
  and forecast_2_id not like '%#Error%'
""")
loaded_items_L2.createOrReplaceTempView('loaded_items_L2')
# loaded_items_L2.display()

# COMMAND ----------

loaded_items=spark.sql("""
select distinct forecast_1_id || '-' || forecast_2_id key
  from 
 g_tembo.lom_aa_do_l1_hist_archive -- to be replaced with archive table
where 
forecast_1_id not like '%#Error%'
and forecast_2_id not like '%#Error%'
union 
select distinct forecast_1_id || '-' || forecast_2_id key
  from 
 g_tembo.lom_aa_do_l1_archive -- to be replaced with archive table
where 
forecast_1_id not like '%#Error%'
and forecast_2_id not like '%#Error%'
""")
loaded_items.createOrReplaceTempView('loaded_items')

# COMMAND ----------

qvunitprice = spark.sql("""
select 
  StyleCode, 
  avg(AVG_UNIT_SELLING_PRICE) AVG_UNIT_SELLING_PRICE,
  avg(AVG_UNIT_COST_PRICE) AVG_UNIT_COST_PRICE
from 
  sap.qvunitprice
where 
  not _deleted
group by
  StyleCode
""")
qvunitprice.createOrReplaceTempView('qvunitprice')

# COMMAND ----------

source_system_org_items=spark.sql("""
select distinct 
--   pr._source, 
  pr.e2eproductcode, 
  nvl(org.commonOrganizationCode, 'DC' || org.organizationCode) organizationCode,
  min(pr_org.productStatus) productStatus
from s_core.product_agg pr
  join s_core.product_org_agg pr_org on pr._id = pr_org.item_id
  join s_core.organization_agg org on pr_org.organization_id = org._id
where 
  pr._source in  (select sourceSystem from source_systems) 
  and not pr._deleted
  and not pr_org._deleted
  and not org._deleted
group by
--   pr._source, 
  pr.e2eproductcode, 
  nvl(org.commonOrganizationCode, 'DC' || org.organizationCode)
"""
)
source_system_org_items.createOrReplaceTempView('source_system_org_items')
# source_system_org_items.display()

# COMMAND ----------

source_system_items=spark.sql("""
select distinct 
  pr._source, 
  pr.e2eproductcode,
  pr.e2eItemDescription,
  pr.caseVolume,
  pr.caseNetWeight,
  pr.caseGrossWeight,
  pr.productStyle,
  pr.sizeDescription,
  pr.gbu,
  pr.productSbu,
  pr.productBrand,
  pr.productSubBrand,
  pr.marketingCode,
  pr.productStatus,
  pr.ansStdUom,  
  pr.baseProduct
from 
  s_core.product_agg pr
where 
  pr._source in  (select sourceSystem from source_systems) 
  and not pr._deleted
"""
)
source_system_items.createOrReplaceTempView('source_system_items')

# COMMAND ----------

LOM_AA_IP_L2 = spark.sql("""
with cdr_ax as (
  select
    distinct 
      'AI' sourcedfrom,
      forecast_1_id,
      forecast_2_id,
      case 
          when forecast_2_id in ('DCABL', 'DCHER') then ('TOT')
          when forecast_2_id in ('DC819') then ('COL')
          when forecast_2_id in ('DCASWH') then ('KGD')
          when forecast_2_id like 'DCAAL%' or  forecast_2_id like 'DCANV%' then ('SAP')
          when forecast_2_id in ('DSEMEA', 'DSAPAC') then 'SAP'
          else 'EBS'
     end source 
  from
    cdr_ai_final
  where 1=1
  union
  select
    distinct 
      'AS' sourcedfrom,
      forecast_1_id,
      forecast_2_id,
      case 
          when forecast_2_id in ('DCABL', 'DCHER') then ('TOT')
          when forecast_2_id in ('DC819') then ('COL')
          when forecast_2_id in ('DCASWH') then ('KGD')
          when forecast_2_id like 'DCAAL%' or  forecast_2_id like 'DCANV%' then ('SAP')
          when forecast_2_id in ('DSEMEA', 'DSAPAC') then 'SAP'
          else 'EBS'
     end source 
  from
    ip_cdr_as
  where 1=1
  union
  select distinct
  'PSL-P' sourcedfrom,
  Parent_Item_id forecast_1_id, 
  Parent_Location forecast_2_id,
  case 
      when Parent_Location in ('DCABL', 'DCHER') then ('TOT')
      when Parent_Location in ('DC819') then ('COL')
      when Parent_Location in ('DCASWH') then ('KGD')
      when Parent_Location like 'DCAAL%' or  Parent_Location like 'DCANV%' then ('SAP')
      when Parent_Location in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source 
from 
  df_ip_psl
union 
select 
  'PSL-C' sourcedfrom,
  Component_Item_ID forecast_1_id,
  Component_Location forecast_2_id,
  case 
      when Component_Location in ('DCABL', 'DCHER') then ('TOT')
      when Component_Location in ('DC819') then ('COL')
      when Component_Location in ('DCASWH') then ('KGD')
      when Component_Location like 'DCAAL%' or  Component_Location like 'DCANV%' then ('SAP')
      when Component_Location in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source 
from 
  df_ip_psl
union 
  select distinct
  'SourceExceptionsMS' sourcefrom,
  item_number forecast_1_id, 
  source forecast_2_id,
  case 
      when destination in ('DCABL', 'DCHER') then ('TOT')
      when destination in ('DC819') then ('COL')
      when destination in ('DCASWH') then ('KGD')
      when destination like 'DCAAL%' or  destination like 'DCANV%' then ('SAP')
      when destination in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source 
from itemsourceexceptions_multisource
union
    select distinct
  'ip_purchase_orders' sourcefrom,
  item_ID forecast_1_id, 
  Location_ID forecast_2_id,
  case 
      when Location_ID in ('DCABL', 'DCHER') then ('TOT')
      when Location_ID in ('DC819') then ('COL')
      when Location_ID in ('DCASWH') then ('KGD')
      when Location_ID like 'DCAAL%' or  Location_ID like 'DCANV%' then ('SAP')
      when Location_ID in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source 
from ip_purchase_orders
union
    select distinct
  'ip_intransit' sourcefrom,
  item_ID forecast_1_id, 
  Location_ID forecast_2_id,
  case 
      when Location_ID in ('DCABL', 'DCHER') then ('TOT')
      when Location_ID in ('DC819') then ('COL')
      when Location_ID in ('DCASWH') then ('KGD')
      when Location_ID like 'DCAAL%' or  Location_ID like 'DCANV%' then ('SAP')
      when Location_ID in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source 
from ip_intransit_orders
union
    select distinct
  'ip_production' sourcefrom,
  item_ID forecast_1_id, 
  Location_ID forecast_2_id,
  case 
      when Location_ID in ('DCABL', 'DCHER') then ('TOT')
      when Location_ID in ('DC819') then ('COL')
      when Location_ID in ('DCASWH') then ('KGD')
      when Location_ID like 'DCAAL%' or  Location_ID like 'DCANV%' then ('SAP')
      when Location_ID in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source 
from sps_production_order
  
) 
select
      sourcedFrom,
      main.source,
      '2' Pyramid_Level,
      main.Forecast_1_ID,
      main.Forecast_2_ID,
      '' Forecast_3_ID,
      'AA' Record_Type,
      left(nvl(pdhProduct.e2eItemDescription, pr.e2eItemDescription), 36) Description,
      'N' Forecast_calculate_indicator,
      round(nvl(qvunitprice.AVG_UNIT_SELLING_PRICE, 0), 5) Unit_price,
      round(nvl(qvunitprice.AVG_UNIT_COST_PRICE, 0), 5) Unit_cost,
      cast(coalesce(pdhProduct.caseVolume, pr.caseVolume,0) as decimal(9, 4)) Unit_cube,
      cast(coalesce(pdhProduct.caseGrossWeight, pr.caseGrossWeight,0) as decimal(9, 4)) Unit_weight,
      'N' Product_group_conversion_option,
      cast(100000 as decimal(11, 5)) Product_group_conversion_factor,
      'DEFAULT' Forecast_Planner,
      coalesce(pdhProduct.productStyle, pr.productStyle, 'No Style') || '_' || coalesce(pdhProduct.sizeDescription, pr.sizeDescription, 'No Size') User_Data_84,
      coalesce(pdhProduct.productStyle, pr.productStyle, 'No Style') User_Data_01,
      coalesce(pdhProduct.sizeDescription, pr.sizeDescription,'No Size') User_Data_02,
      coalesce(pdhProduct.productStyle, pr.productStyle, 'No Style') || '_' || coalesce(pdhProduct.sizeDescription, pr.sizeDescription, 'No Size') User_Data_03,
      nvl(pdhProduct.gbu, pr.gbu) User_Data_04,
      nvl(pdhProduct.productSbu, pr.productSbu) User_Data_05,
      nvl(pdhProduct.productBrand, pr.productBrand) User_Data_06,
      nvl(pdhProduct.productSubBrand,pr.productSubBrand) User_Data_07,
      coalesce(pdhProduct.baseProduct, pr.baseProduct, pdhProduct.productStyle, pr.productStyle) User_Data_08,
      nvl(pdhProduct.marketingCode, pr.marketingCode) User_Data_14,
      nvl(pdhProduct.productStatus, pr.productStatus) User_Data_15,
      pr_org.productStatus User_Data_16,
      nvl(pdhProduct.ansStdUom, pr.ansStdUom) Unit_of_measure,
      '' gtcReplacementFlag,
      case 
        when main.Forecast_2_ID like 'DS%' then trim(substring(main.Forecast_2_ID,3,5))
        when main.Forecast_2_ID like 'DC%' then nvl(org.region, '#Error49-' || main.Forecast_2_ID) 
        when main.Forecast_2_ID like 'PL%' then 'PLANTS'
        when main.Forecast_2_ID like 'EM%' then 'SUPPLIERS'
      end user_data_41
from
      cdr_ax main
      left join pdhProduct on main.forecast_1_id = pdhProduct.productCode 
      left join qvunitprice on  pdhProduct.productStyle  = qvunitprice.StyleCode
      left join source_system_org_items pr_org on main.forecast_1_id = pr_org.e2eproductcode
        and main.forecast_2_id = pr_org.organizationCode
      left join source_system_items pr on main.forecast_1_id = pr.e2eProductCode
         and main.source = pr._source
      left join s_core.organization_agg org on main.Forecast_2_ID = org.CommonOrganizationCode
      join locationmaster lm on Forecast_2_ID = lm.globalLocationCode
       
where 1=1
  and main.Forecast_1_ID || '-' || main.Forecast_2_ID not in (select Forecast_1_ID || '-' || Forecast_2_ID from all_do_items)
  and not nvl(org._deleted, false)
""")
# LOM_AA_IP_L2.createOrReplaceTempView('LOM_AA_IP_L2')
# LOM_AA_IP_L2.display()

target_file = get_file_path(temp_folder, 'LOM_AA_IP_L2')
LOM_AA_IP_L2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
LOM_AA_IP_L2 = spark.read.format('delta').load(target_file)
LOM_AA_IP_L2.createOrReplaceTempView('LOM_AA_IP_L2')

# COMMAND ----------

# # SETUP PARAMETERS
# file_name = get_file_name('tmp_ip_lom_au_l2', 'dlt') 
# target_file = get_file_path(temp_folder, file_name)
# print(file_name)
# print(target_file)

# # LOAD
# LOM_AU_IP_L2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,60.6 SPS_STORAGE_RESOURCE
sps_storage_resource_f = spark.sql("""
with sps_sr as 
(select distinct 
  forecast_2_id location_ID
from 
  cdr_ai_final
where 1=1
   and forecast_2_id is not null
--   and forecast_2_id not like 'DS%'
union 
select distinct 
  source_location_1 location_ID
from 
  cdr_ai_final
where 1=1
    and source_location_1 is not null
    and source_location_1 <> ''
union 
select distinct 
   GlobalLocationCode location_ID
from locationmaster lm
)
select
  Location_ID,
  CONCAT(Location_ID,'-S') Resource_Index,
  CONCAT(Location_ID,'-S') Resource_ID,
  '1' Resource_Type,
  'DEFAULT' Planner_ID,
  CONCAT(Location_ID,'-S') Description,
  left(CONCAT(Location_ID,'-S'),10) Abbreviation,
  'PIECE' Unit_Of_Measure,
  'DEFAULT' Calendar_ID,
  '0' Planning_Time_Fence,
  '0' Exception_Time_Fence,
  case when location_ID like 'DC%' 
    then '0'
    else '1' 
  end Constrained,
  CASE 
    WHEN location_ID like 'DC%' 
    THEN '0' 
    ELSE '1'
  END AS Constraint_Active,
  '1' Resource_Level_Load_Indicator
from 
  sps_sr
  join locationmaster lm on Location_ID = lm.globalLocationCode

""")  

sps_storage_resource_f.createOrReplaceTempView("SPS_STORAGE_RESOURCE")
# sps_storage_resource_f.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_SPS_STORAGE_RESOURCE', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_storage_resource_f.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,60.7 SPS_FLOW_RESOURCE
sps_flow_resource_f=spark.sql("""
with sps_fr as 
(select distinct 
  forecast_2_id location_ID
from 
  cdr_ai_final
where 1=1
   and forecast_2_id is not null
union 
select distinct 
  source_location_1 location_ID
from 
  cdr_ai_final
where 1=1
    and source_location_1 is not null
    and source_location_1 <> ''
union 
select distinct 
   GlobalLocationCode location_ID
from locationmaster lm
)
select
  Location_ID,
  CONCAT(Location_ID,'-F') Resource_Index,
  CONCAT(Location_ID,'-F') Resource_ID,
  '2' Resource_Type,
  'DEFAULT' Planner_ID,
  CONCAT(Location_ID,'-F') Description,
  left(CONCAT(Location_ID,'-F'),10) Abbreviation,
  'PIECE' Unit_Of_Measure,
  'DEFAULT' Calendar_ID,
  '0' Planning_Time_Fence,
  '0' Exception_Time_Fence,
  '0' Constrained,
  '0' Constraint_Active
from 
  sps_fr
  join locationmaster lm on Location_ID = lm.globalLocationCode

""")  
# sps_flow_resource_f.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_SPS_FLOW_RESOURCE', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_flow_resource_f.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,60.8 SPS_SECONDARY_RESOURCE
SPS_SECONDARY_RESOURCE = spark.sql("""
select distinct
  cm.Location Location_ID,
  cm.productionResource Resource_Index,
  cm.productionResource Resource_ID,
  3 Resource_Type,
  nvl(cm.PlannerID, 'No Planner') Planner_ID,
  cm.productionResource Description,
  UPPER(cm.UOM) AS Unit_of_Measure_ID,
  nvl(cm.Calendar, 'No Calendar') Calendar_ID,
  case
    when nvl(cm.PlanningTimeFence,0) = 0
      then lm.OrderLeadTime
    else
      --nvl(cm.PlanningTimeFence,0)
      case when nvl(cm.PlanningTimeFence,0) - 30 < 30 then 30 else nvl(cm.PlanningTimeFence,0) - 30 end
  end Planning_Time_Fence,
  0 Exception_Time_Fence,
  cm.ConstrainedResIndi AS Constrained,
  cm.ConstrActiveIndi AS Constraint_Active,
  1 Resource_Level_Load_Indicator
from
  current_capacity_master cm
  JOIN locationmaster lm on lm.GlobalLocationCode = cm.Location
where
  cm.resourceType like 'Sec%'
  and lm.MainOrigin = 'Y'
""")
# SPS_SECONDARY_RESOURCE.display()
# create_tmp_table(sql_SPS_SECONDARY_RESOURCE, "SPS_SECONDARY_RESOURCE")

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_SPS_SECONDARY_RESOURCE', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
SPS_SECONDARY_RESOURCE.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)


# COMMAND ----------

# DBTITLE 1,60.9 SECONDARY_RESOURCE_LINK
SECONDARY_RESOURCE_LINK = spark.sql("""
select distinct
  Location Location_ID,
  left(SecondaryResLink,40) Secondary_Resource,
  left(ResourceIndex,40) Production_Resource
from
  dbo_cm_resource_data
where ResourceType = 'Primary'
  and SecondaryResLink is not null
  
union 

select distinct
  Location Location_ID,
  left(SecSecondaryResLink,40) Secondary_Resource,
  left(ResourceIndex,40) Production_Resource
from
  dbo_cm_resource_data
  join locationmaster lm on Location = lm.globalLocationCode

where ResourceType = 'Primary'
and SecSecondaryResLink is not null
""")
SECONDARY_RESOURCE_LINK.createOrReplaceTempView('SECONDARY_RESOURCE_LINK')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_SECONDARY_RESOURCE_LINK', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
SECONDARY_RESOURCE_LINK.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)


# COMMAND ----------

# DBTITLE 1,60.13 SPS_LOCATION
sps_location =spark.sql("""
select
  GlobalLocationCode LOC_INDX,
  'DEFAULT' PLNR_USR_INDX,
  '1' SBJTV_BKORD_PNLTY_FCTR,
  'DEFAULT' CAL_ID,
  GlobalLocationName LOC_DESC,
  case 
    when GlobalLocationCode like 'DC%' then '2' 
    when GlobalLocationCode like 'DS%' then '3' 
    when GlobalLocationCode like 'PL%' then '5'
    when GlobalLocationCode like 'EM%' then '6'
  end LOC_TYPE,
  nvl(LatitudeLongitude,'') LATITUDE_LONGITUDE
from
  locationmaster
where
  1=1
  and mainOrigin = 'Y'
""")

# create_tmp_table(sql_sps_location_f, "sps_location")
sps_location.createOrReplaceTempView('sps_location')
# sps_location.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_SPS_LOCATION', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_location.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,60.14.1 LGTY_ITEM_LOCATION 
LGTY_ITEM_LOCATION = spark.sql("""
select distinct
  Forecast_1_id Item_ID,
  Forecast_2_id Location_ID,
  nvl(pdhProduct.ansStdUomConv,0) SP_Transfer_Out_Minimum_Quantity,
  nvl(pdhProduct.ansStdUomConv,0) SP_Transfer_Out_Mulitiple,
  1 SP_Transfer_Out_Max_Quantity,
  'S' Schedule_Type
from 
  cdr_ai_final 
  join pdhProduct on Forecast_1_ID = pdhProduct.e2eProductCode
union
select distinct
  Forecast_1_id Item_ID,
  Source_Location_1 Location_ID,
  nvl(pdhProduct.ansStdUomConv,0) SP_Transfer_Out_Minimum_Quantity,
  nvl(pdhProduct.ansStdUomConv,0) SP_Transfer_Out_Mulitiple,
  1 SP_Transfer_Out_Max_Quantity,
  'S' Schedule_Type
from 
  cdr_ai_final 
  join pdhProduct on Forecast_1_ID = pdhProduct.e2eProductCode  
""")
LGTY_ITEM_LOCATION.createOrReplaceTempView('LGTY_ITEM_LOCATION')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LGTY_ITEM_LOCATION', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LGTY_ITEM_LOCATION.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

main_source_location=spark.sql("""
select 
  forecast_1_id, 
  max(Source_Location_1) Source_Location_1
from 
  cdr_ai_final
where 
  network_level = 90
group by 
  forecast_1_id
""")
main_source_location.createOrReplaceTempView('main_source_location')
# main_source_location.display()

# COMMAND ----------

all_do_ip_items=spark.sql("""
  select distinct forecast_1_id--, forecast_2_id 
  from all_do_items
  union
  select distinct forecast_1_id--, forecast_2_id 
  from lom_aa_ip_l2
  union
  select distinct forecast_1_id--, forecast_2_id 
  from cdr_ai_final
  union
  select distinct forecast_1_id--, forecast_2_id 
  from ip_cdr_as
  union
  select distinct Parent_Item_ID--, Parent_Location  
  from df_ip_psl

""")
all_do_ip_items.createOrReplaceTempView('all_do_ip_items')
# all_do_ip_items.display()

# COMMAND ----------

# DBTITLE 1,6.16 SPS_ITEMLOC_RESOURCE
SPS_ITEMLOC_RESOURCE = spark.sql("""
select distinct
  'a' scenario,
  all_items.forecast_1_id ITEM_INDX,
  nvl(cm.Location, '#Error42 ' || pdhProduct.productstyle || '-' || pdhProduct.sizeDescription ) LOC_INDX,
  left(nvl(cm.productionResource, '#Error42 ' || pdhProduct.productstyle || '-' || pdhProduct.sizeDescription),40) RSRC_INDX,
  pdhProduct.ansStdUomConv PROD_MIN_QTY,
  pdhProduct.ansStdUomConv PROD_MULT_QTY
from
  all_do_ip_items all_items 
  left join pdhProduct on all_items.forecast_1_id = pdhProduct.e2eProductCode
  left join s_core.capacity_master_agg cm on pdhProduct.productstyle = cm.style 
        and pdhProduct.sizeDescription = cm.size 
  join locationmaster lm on cm.Location = lm.globalLocationCode
where
    not cm._deleted
    and pdhProduct.productStatus not in ('Inactive') 
 
union -- add additional REPACK entries
select 
  'b' scenario,
  Parent_item_ID ITEM_INDX, 
  Parent_Location LOC_INDX,
  left('REPACK_' || Parent_Location,40) RSRC_INDX,
  pdhProduct.ansStdUomConv PROD_MIN_QTY,
  pdhProduct.ansStdUomConv PROD_MULT_QTY
from 
  df_ip_psl
  join pdhProduct on df_ip_psl.Parent_item_ID = pdhProduct.e2eProductCode
""")
SPS_ITEMLOC_RESOURCE.createOrReplaceTempView('SPS_ITEMLOC_RESOURCE')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_itemloc_resource', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
SPS_ITEMLOC_RESOURCE.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

SPS_ITEMLOC_PROD_CONV=spark.sql("""
select distinct
  all_items.forecast_1_id as Item,
  nvl(cm.location, '#Error42') as Location,
  nvl(left(cm.productionResource,40), '#Error42' ) as Production_Resource,
  cm.beginDate as Begin_Date,
  cm.endDate as End_Date,
  0 as Material_Cost,
  case when cm.rateOrUnit = 'U'
      then cm.unitsPerHour 
    else
      nvl(1 / cm.unitsPerHour,0) 
  end Units_per_Hour,
  1 as Duration_in_Days,
  case when cm.rateOrUnit = 'U'
    then cm.unitsPerHour
    else
    cm.unitsPerHour --cm.regularCapacityQuantity 
  end as Hours_Per_Day,
  case 
    when nvl(pdhProduct.SHELFLIFE,0) = 0 or nvl(pdhProduct.SHELFLIFE,0) > 700
    then 700
    else pdhProduct.SHELFLIFE
  end Shelf_Life_Days
from 
--   main_5
  all_do_ip_items all_items
  left join pdhProduct on all_items.forecast_1_id = pdhProduct.e2eProductCode
  left join s_core.capacity_master_agg cm on pdhProduct.productstyle = cm.style
    and pdhProduct.sizeDescription = cm.size
  join locationmaster lm on cm.Location = lm.globalLocationCode
where 
  not cm._deleted
  --and pdhProduct.productStatus not in ('Inactive','Discontinue') 
  and pdhProduct.productStatus not in ('Inactive') 
  and cm.endDate >= current_date
  
union -- add additional REPACK entries

select 
  Parent_item_ID as Item, 
  Parent_Location as Location,
  'REPACK_' || Parent_Location Production_Resource,
  '2022-09-28' as Begin_Date,
  '2999-12-12' as End_Date,
  0 as Material_Cost,
  1 as Units_per_Hour,
  1 as Duration_in_Days,
  1 as Hours_Per_Day,
  case 
    when nvl(pdhProduct.SHELFLIFE,0) = 0 or nvl(pdhProduct.SHELFLIFE,0) > 700
    then 700
    else pdhProduct.SHELFLIFE
   end Shelf_Life_Days
from 
  df_ip_psl
  join pdhProduct on df_ip_psl.Parent_item_ID = pdhProduct.e2eProductCode
where pdhProduct.productStatus not in ('Inactive') 
""")

target_file = get_file_path(temp_folder, 'SPS_ITEMLOC_PROD_CONV')
SPS_ITEMLOC_PROD_CONV.createOrReplaceTempView('SPS_ITEMLOC_PROD_CONV')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_itemloc_prod_conv', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
SPS_ITEMLOC_PROD_CONV.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

more_ip_records=spark.sql("""
with more_ip_records as(
  select
    distinct 'prod_conv' sourcedFrom,
    item Forecast_1_ID,
    location Forecast_2_ID,
    case
      when location in ('DCABL', 'DCHER') then ('TOT')
      when location in ('DC819') then ('COL')
      when location in ('DCASWH') then ('KGD')
      when location like 'DCAAL%'
      or location like 'DCANV%' then ('SAP')
      when location in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source
  from
    SPS_ITEMLOC_PROD_CONV
  union
  select distinct
  'Source_no_multiSource' sourcedFrom,
  item_number Forecast_1_ID,
  source Forecast_2_ID,
    case
      when source in ('DCABL', 'DCHER') then ('TOT')
      when source in ('DC819') then ('COL')
      when source in ('DCASWH') then ('KGD')
      when source like 'DCAAL%'
      or source like 'DCANV%' then ('SAP')
      when source in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source
from 
  itemsourceexceptions_no_multisource
union
select distinct
  'Source_no_multiSource' sourcedFrom,
  item_number Forecast_1_ID,
  source Forecast_2_ID,
    case
      when source in ('DCABL', 'DCHER') then ('TOT')
      when source in ('DC819') then ('COL')
      when source in ('DCASWH') then ('KGD')
      when source like 'DCAAL%'
      or source like 'DCANV%' then ('SAP')
      when source in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
    end source
from 
  itemsourceexceptions_multisource

union 
select distinct 
  'TO' sourcedFrom,
  item_id forecast_1_id,
  destination_Location_ID forecast_2_ID,
  case
      when destination_Location_ID in ('DCABL', 'DCHER') then ('TOT')
      when destination_Location_ID in ('DC819') then ('COL')
      when destination_Location_ID in ('DCASWH') then ('KGD')
      when destination_Location_ID like 'DCAAL%'
      or destination_Location_ID like 'DCANV%' then ('SAP')
      when destination_Location_ID in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
  end source
from sap_transfer_orders_final

union 
select distinct 
  'TO' sourcedFrom,
  item_id,
  Source_Location_ID forecast_2_ID,
  case
      when destination_Location_ID in ('DCABL', 'DCHER') then ('TOT')
      when destination_Location_ID in ('DC819') then ('COL')
      when destination_Location_ID in ('DCASWH') then ('KGD')
      when destination_Location_ID like 'DCAAL%'
      or destination_Location_ID like 'DCANV%' then ('SAP')
      when destination_Location_ID in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
  end source
from sap_transfer_orders_final

union 
select 
distinct 
  'PO' sourcedFrom,
  item_id forecast_1_id,
  Location_ID forecast_2_ID,
  case
      when Location_ID in ('DCABL', 'DCHER') then ('TOT')
      when Location_ID in ('DC819') then ('COL')
      when Location_ID in ('DCASWH') then ('KGD')
      when Location_ID like 'DCAAL%'
      or Location_ID like 'DCANV%' then ('SAP')
      when Location_ID in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
  end source
from sap_purchase_orders_final

union 
select 
distinct 
  'TO' sourcedFrom,
  item_id forecast_1_id,
  Vendor_Location_ID forecast_2_ID,
  case
      when Location_ID in ('DCABL', 'DCHER') then ('TOT')
      when Location_ID in ('DC819') then ('COL')
      when Location_ID in ('DCASWH') then ('KGD')
      when Location_ID like 'DCAAL%'
      or Location_ID like 'DCANV%' then ('SAP')
      when Location_ID in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
  end source
from sap_purchase_orders_final

UNION
select distinct 
  'CO' sourcedFrom,
  item_id forecast_1_id,
  location_ID forecast_2_ID,
  case
      when Location_ID in ('DCABL', 'DCHER') then ('TOT')
      when Location_ID in ('DC819') then ('COL')
      when Location_ID in ('DCASWH') then ('KGD')
      when Location_ID like 'DCAAL%'
      or Location_ID like 'DCANV%' then ('SAP')
      when Location_ID in ('DSEMEA', 'DSAPAC') then 'SAP'
      else 'EBS'
  end source
from sps_customer_order

)
select distinct
  sourcedFrom,
  main.source,
  '2' Pyramid_Level,
  main.Forecast_1_ID,
  main.Forecast_2_ID,
  '' Forecast_3_ID,
  'AA' Record_Type,
  left(    nvl(      pdhProduct.e2eItemDescription,      pr.e2eItemDescription    ),    36  ) Description,
  'N' Forecast_calculate_indicator,
  round(nvl(qvunitprice.AVG_UNIT_SELLING_PRICE, 0), 5) Unit_price,
  round(nvl(qvunitprice.AVG_UNIT_COST_PRICE, 0.00001), 5) Unit_cost,
  cast(    coalesce(pdhProduct.caseVolume, pr.caseVolume, 0) as decimal(9, 4)  ) Unit_cube,
  cast(    coalesce(pdhProduct.caseGrossWeight, pr.caseGrossWeight, 0) as decimal(9, 4)  ) Unit_weight,
  'N' Product_group_conversion_option,
  cast(1 as decimal(11, 5)) Product_group_conversion_factor,
  'DEFAULT' Forecast_Planner,
  coalesce(    pdhProduct.productStyle,    pr.productStyle,    'No Style'  ) || '_' || coalesce(    pdhProduct.sizeDescription,    pr.sizeDescription,    'No Size'  ) User_Data_84,
  coalesce(    pdhProduct.productStyle,    pr.productStyle,    'No Style'  ) User_Data_01,
  coalesce(    pdhProduct.sizeDescription,    pr.sizeDescription,    'No Size'  ) User_Data_02,
  coalesce(pdhProduct.productStyle,    pr.productStyle,    'No Style'  ) || '_' || coalesce(    pdhProduct.sizeDescription,    pr.sizeDescription,    'No Size'  ) User_Data_03,
  nvl(pdhProduct.gbu, pr.gbu) User_Data_04,
  nvl(pdhProduct.productSbu, pr.productSbu) User_Data_05,
  nvl(pdhProduct.productBrand, pr.productBrand) User_Data_06,
  nvl(pdhProduct.productSubBrand, pr.productSubBrand) User_Data_07,
  coalesce(
    pdhProduct.baseProduct,
    pr.baseProduct,
    pdhProduct.productStyle,
    pr.productStyle
  ) User_Data_08,
  nvl(pdhProduct.marketingCode, pr.marketingCode) User_Data_14,
  nvl(pdhProduct.productStatus, pr.productStatus) User_Data_15,
  pr_org.productStatus User_Data_16,
  nvl(pdhProduct.ansStdUom, pr.ansStdUom) Unit_of_measure,
  '' gtcReplacementFlag,
  case
    when main.Forecast_2_ID like 'DS%' then trim(substring(main.Forecast_2_ID, 3, 5))
    when main.Forecast_2_ID like 'DC%' then nvl(org.region, '#Error49-' || main.Forecast_2_ID)
    when main.Forecast_2_ID like 'PL%' then 'PLANTS'
    when main.Forecast_2_ID like 'EM%' then 'SUPPLIERS'
  end user_data_41
from
  more_ip_records main
  left join pdhProduct on main.forecast_1_id = pdhProduct.productCode
  left join qvunitprice on pdhProduct.productStyle = qvunitprice.StyleCode
  left join source_system_org_items pr_org on main.forecast_1_id = pr_org.e2eproductcode
  and main.forecast_2_id = pr_org.organizationCode
  left join source_system_items pr on main.forecast_1_id = pr.e2eProductCode
  and main.source = pr._source
  left join s_core.organization_agg org on main.Forecast_2_ID = org.CommonOrganizationCode
where
  1 = 1
  and main.Forecast_1_ID || '-' || main.Forecast_2_ID not in (select Forecast_1_ID || '-' || Forecast_2_ID from all_do_items)
  and main.Forecast_1_ID || '-' || main.Forecast_2_ID not in (select Forecast_1_ID || '-' || Forecast_2_ID from LOM_AA_IP_L2)
  and not nvl(org._deleted, false)
""")

# COMMAND ----------

LOM_AA_IP_L2_FINAL=(LOM_AA_IP_L2
  .union(more_ip_records))
  
LOM_AA_IP_L2_FINAL.createOrReplaceTempView('LOM_AA_IP_L2_FINAL')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_ip_lom_aa_l2', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AA_IP_L2_FINAL.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
# LOM_AA_IP_L2.count()

# COMMAND ----------

LOM_AU_IP_L2 = spark.sql("""
select distinct 
  '2' Pyramid_Level,
  Forecast_1_ID,
  Forecast_2_ID,
  Forecast_3_ID,
  'AU' Record_Type,
  User_data_41,
  Forecast_2_ID User_Data_43
from
  LOM_AA_IP_L2_FINAL
  """)
LOM_AU_IP_L2.createOrReplaceTempView('LOM_AU_IP_L2')


# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_ip_lom_au_l2', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LOM_AU_IP_L2.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,60.5 SPS_PRODUCTION_RESOURCE
sps_production_resource = spark.sql("""
select
  cm.Location Location_ID,
  left(cm.productionResource,40) Resource_Index,
  left(cm.productionResource,40) Resource_ID,
  0 Resource_Type,
  nvl(cm.PlannerID, 'No Planner') PlannerID,
  left(cm.productionResource,50) Description,
  UPPER(cm.UOM) AS Unit_of_Measure_ID,
  nvl(cmrd.Calendar, 'No Calendar') Calendar_ID,
  case when nvl(cm.PlanningTimeFence - 30,0) < 30 
    then 30 
    else 
      cm.PlanningTimeFence - 30
  end Planning_Time_Fence,
  --case when nvl(cmrd.PlanningTimeFence,0) = 0 
  --  then  
  --    lm.OrderLeadTime
  --  else 
  --    nvl(cmrd.PlanningTimeFence,0) 
  --end Planning_Time_Fence,
  0 Exception_Time_Fence,
  cmrd.ConstrainedResIndi AS Constrained,
  cmrd.ConstrActiveIndi AS Constraint_Active,
  1 Resource_Level_Load_Indicator
from
  s_core.capacity_master_agg cm
  JOIN locationmaster lm on lm.GlobalLocationCode = cm.Location
  left join dbo_cm_resource_data cmrd
    on cm.productionResource = cmrd.resourceindex
where
  cm.resourceType= 'Primary'
union
select distinct
  Parent_Location Location_ID,
  left(Resource,40) Resource_Index,
  left(Resource,40) Resource_ID,
  0 Resource_Type,
  'mitesh.verma' Planner_ID,
  left(Resource,50) Description,
  'PIECE' AS Unit_of_Measure_ID,
  'DEFAULT' Calendar_ID,
  0 Planning_Time_Fence,
  0 Exception_Time_Fence,
  0 AS Constrained,
  0 AS Constraint_Active,
  0 Resource_Level_Load_Indicator
from 
  df_sp_psl
where 1=1
union
select distinct
   Parent_Location as Location_ID,
   'REPACK_' || Parent_Location Resource_Index,
   'REPACK_' || Parent_Location Resource_ID,
   0 as Resource_Type,
   'mitesh.verma' as PlannerID,
   'REPACK_' || Parent_Location Description,
   'PIECE' as Unit_of_Measure_ID,
   'DEFAULT' as Calendar_ID, 
   0 as Planning_Time_Fence,
   0 Exception_Time_Fence,
   0 AS Constrained,
   0 AS Constraint_Active,
   0 Resource_Level_Load_Indicator
 from 
   df_ip_psl
   join pdhProduct on df_ip_psl.Parent_item_ID = pdhProduct.e2eProductCode
""")

target_file = get_file_path(temp_folder, 'sps_production_resource')
sps_production_resource.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
sps_production_resource = spark.read.format('delta').load(target_file)
sps_production_resource.createOrReplaceTempView('sps_production_resource')

# COMMAND ----------

# DBTITLE 1,create only resources if we have items on them
# sps_production_resource_filtered=spark.sql("""
# select * from sps_production_resource where Resource_Index   in (select  Production_Resource from SPS_ITEMLOC_PROD_CONV)
# """)
# sps_production_resource_filtered.createOrReplaceTempView('sps_production_resource_filtered')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_production_resource', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_production_resource.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,6.18 SPS_PRODUCTION_CAPACITY
SPS_PRODUCTION_CAPACITY = spark.sql("""
select distinct
  left(cm.Location,40) as LOC_INDX,
  left(cm.productionResource,40) as RSRC_INDX,
  cm.beginDate as BGN_EFF_DATE,
  cmd.ConstrainedResIndi as RSRC_CNSTRNT_IND,
  cmd.ConstrActiveIndi as CNSTRNT_ACTV_IND,
  cm.regularCapacityQuantity as REGLR_CPCTY_QTY,
  case when nvl(cm.ResourcePriority,1) > 1 then nvl(cm.ResourcePriority,1) * 5 
     else
     nvl(cm.ResourcePriority,1)
  end as REGLR_COST_VALUE,
  nvl(cm.OvertimeCapacity,0) OVRTM1_CPCTY_QTY,
  nvl(cm.OvertimeCost,0) + 1 OVRTM1_COST_VALUE,
  nvl(cmd.MIN_USG_HRS_QTY,0) as MIN_USG_HRS_QTY
from
  s_core.capacity_master_agg cm
  JOIN locationmaster lm on lm.GlobalLocationCode = cm.Location
  join dbo_cm_resource_data cmd on cm.productionResource = cmd.ResourceIndex --and cm.size = cmd.size
union
select distinct
  Parent_Location LOC_INDX,
  Resource RSRC_INDX,
  '2021-01-01' AS BGN_EFF_DATE,
  0 AS RSRC_CNSTRNT_IND,
  0 AS CNSTRNT_ACTV_IND,
  0 AS REGLR_CPCTY_QTY,
  1 AS REGLR_COST_VALUE,
  0 AS OVRTM1_CPCTY_QTY,
  1 AS OVRTM1_COST_VALUE,
  0 MIN_USG_HRS_QTY
from 
  df_sp_psl
""")
SPS_PRODUCTION_CAPACITY.createOrReplaceTempView('SPS_PRODUCTION_CAPACITY')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_production_capacity', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
SPS_PRODUCTION_CAPACITY.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,6.23 SPS_TRANSPORT_SERVICE
sps_transport_service=spark.sql("""
select 
    1 TRNSPRT_SVC_NBR,
    'OCEAN' TRNSPRT_SVC_ID,
    'OCEAN' TRNSPRT_SVC_INDX,
    'OCEAN' TRNSPRT_SVC_DESC,
    'OCEAN' TRNSPRT_SVC_ABBR_TEXT
from 
  run_period
""")


# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_transport_service', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_transport_service.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

TEMBO_INTRANSIT_SUBINVENTORY_CODES=spark.sql("""
select
  distinct key_value subInventoryCode
from
  smartsheets.edm_control_table
where
  table_id = 'TEMBO_INTRANSIT_SUBINVENTORY_CODES'
  and active_flg = 'true'
  and not _deleted
  and current_date between valid_from and valid_to
""")
TEMBO_INTRANSIT_SUBINVENTORY_CODES.createOrReplaceTempView('TEMBO_INTRANSIT_SUBINVENTORY_CODES')
# TEMBO_INTRANSIT_SUBINVENTORY_CODES.display()

# COMMAND ----------

# DBTITLE 1,6.26 SPS_ITEMLOC_INITIAL_INV
SPS_ITEMLOC_INITIAL_INV = spark.sql("""
with inv as
(select
    prod.e2eProductCode ITEM_INDX,
    nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode)  LOC_INDX,
    nvl(to_date(inventory.lotExpirationDate, 'Day'), add_months(to_date(current_date, 'Day'), 18))  EXPIR_DATE,
    sum(inventory.ansStdQty) INV_QTY,
    add_months(to_date(current_date, 'Day'), -1)  FIRST_USE_DATE
from
  s_supplychain.inventory_agg inventory
  INNER JOIN s_core.organization_agg
    ON inventory.inventoryWarehouse_ID = organization_agg._id
  INNER JOIN s_core.organization_agg comp
    ON inventory.owningBusinessUnit_ID = comp._id
  INNER JOIN s_core.product_agg prod
    ON inventory.item_id = prod._id
  join s_core.product_org_agg pr_org on inventory.item_id = pr_org.item_id
       and inventory.inventoryWarehouse_ID = pr_org.organization_ID
where
  comp.organizationCode not in (select organization from exclude_organizations)
  and year(inventory.inventoryDate) * 100 + month(inventory.inventoryDate) = (select runPeriod from run_period)
  and not inventory._deleted
  and inventory.ansStdQty > 0
  and inventory._source in (select sourceSystem from source_systems)
  and nvl(upper(prod.itemType), 'FINISHED GOODS') in ('FINISHED GOODS', 'FINISHED GOOD', 'FERT', 'ZPRF', 'ACCESSORIES')
  and inventory.subInventoryCode not in (select subInventoryCode from TEMBO_HOLD_SUBINVENTORY_CODES)
  and inventory.subInventoryCode not in (select subInventoryCode from TEMBO_INTRANSIT_SUBINVENTORY_CODES)
  and not case when pr_org.lotControlCode = 1 then current_date else nvl(inventory.lotExpirationDate, current_date) end < current_date
group by
  prod.e2eProductCode,
  nvl(organization_agg.commonOrganizationCode, 'DC' || organization_agg.organizationCode),
  nvl(to_date(inventory.lotExpirationDate, 'Day'), add_months(to_date(current_date, 'Day'), 18)) )
select
  coalesce(main_mpx_l0.SuccessorCode, HistoricalProductSubstitution.final_Successor, inv.ITEM_INDX) ITEM_INDX,
  LOC_INDX,
  case
    when EXPIR_DATE > add_months(current_date, 18)
      then add_months(to_date(current_date, 'Day'), 18) + 186
    else
      EXPIR_DATE
  end EXPIR_DATE,
  sum(INV_QTY) INV_QTY,
  FIRST_USE_DATE
from
  inv
  left join HistoricalProductSubstitution on inv.ITEM_INDX = HistoricalProductSubstitution.PROD_CD and inv.LOC_INDX = HistoricalProductSubstitution.DC
  left join (select distinct predecessorCode, DC, SuccessorCode from main_mpx_l0) main_mpx_l0 on inv.ITEM_INDX  = main_mpx_l0.predecessorCode and inv.LOC_INDX = main_mpx_l0.DC
  join locationmaster lm on LOC_INDX = lm.globalLocationCode
where
  nvl(HistoricalProductSubstitution.final_Successor, 'Include')  not in ('dis')
group by 
  coalesce(main_mpx_l0.SuccessorCode, HistoricalProductSubstitution.final_Successor, inv.ITEM_INDX),
  LOC_INDX,
  case
    when EXPIR_DATE > add_months(current_date, 18)
      then add_months(to_date(current_date, 'Day'), 18) + 186
    else
      EXPIR_DATE
  end,
  FIRST_USE_DATE
""")
SPS_ITEMLOC_INITIAL_INV.createOrReplaceTempView('SPS_ITEMLOC_INITIAL_INV')
# SPS_ITEMLOC_INITIAL_INV.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_itemloc_initial_inv', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
SPS_ITEMLOC_INITIAL_INV.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,6.27 LGTY_ITEM_ALT_UOM
LGTY_ITEM_ALT_UOM = spark.sql("""
select 
   productCode ITEM_INDX,
  'CASE_CARTON' UOM_INDX,
  ansStduomConv UOM_CONV_FCTR
from 
  pdhProduct
where nvl(ansStduomConv,0) <> 0
""")

LGTY_ITEM_ALT_UOM.createOrReplaceTempView('LGTY_ITEM_ALT_UOM')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_lgty_item_alt_uom', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LGTY_ITEM_ALT_UOM.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

lom_aa=spark.sql("""
select distinct 
  forecast_1_id ,
  split(forecast_2_id, '_')[1] forecast_2_id  
from g_tembo.lom_aa_do_l1_hist_archive
where upper(nvl(user_data_16, 'Active')) in ('ACTIVE', 'NEW', 'DISCON', 'DISCONTINUED')
  and upper(forecast_1_id) not like 'DEL-%'
  and nvl(split(forecast_2_id, '_')[1],'X') not in ('325', '828')
union
select distinct 
  Lvl1Fcst Forecast_1_ID,
  split(Lvl2Fcst, '_')[1] Forecast_2_ID
from  logftp.logility_do_level_1_for_mpx 
where 1=1
  and upper(Lvl1Fcst) not like 'DEL-%'
  and nvl(split(Lvl2Fcst, '_')[1],'X') not in ('325', '828')
union 
select 
  item_number Forecast_1_ID,
  DESTINATION_LOCATIONValue Forecast_2_ID
from 
  spt.itemsourceexceptions
  join locationmasterNonPrimary lm on itemsourceexceptions.ORIGIN_CODEValue = lm.origin
where
   not itemsourceexceptions._deleted
   and DESTINATION_LOCATIONValue is not null
union 
select 
  item_number Forecast_1_ID,
  lm.globalLocationCode Forecast_2_ID
from 
  spt.itemsourceexceptions
  join locationmasterNonPrimary lm on itemsourceexceptions.ORIGIN_CODEValue = lm.origin
where
   not itemsourceexceptions._deleted
   and lm.globalLocationCode is not null
union
select distinct 
  forecast_1_ID, 
  split(forecast_2_id, '_')[1] forecast_2_id   
from LOM_AA_DO_L1_ALL_UNIQUE
union
select 
distinct 
  Forecast_1_ID, 
  forecast_2_ID
from
  LOM_AA_IP_L2_FINAL
""")

target_file = get_file_path(temp_folder, 'lom_aa')
lom_aa.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)
lom_aa = spark.read.format('delta').load(target_file)
lom_aa.createOrReplaceTempView('lom_aa')

# COMMAND ----------

# DBTITLE 1,Create standard data set for SP related interfaces
SP_LOAD_DATA=spark.sql("""
with q1 as (
select distinct 
  Forecast_1_id,
  Forecast_2_id,
  Source_Location_1
from
  cdr_ai_final
where source_location_1 <> ''  
union
select distinct
  item_number forecast_1_id, 
  destination forecast_2_id,
  source Source_Location_1
from 
  itemsourceexceptions_multisource
union 
select distinct
  item_number forecast_1_id, 
  destination forecast_2_id,
  source Source_Location_1
from 
  itemsourceexceptions_no_multisource
UNION
select 
distinct 
  item_id forecast_1_id,
  destination_Location_ID forecast_2_id,
  Source_Location_ID
 from sap_transfer_orders_final
UNION
select 
distinct 
  item_id forecast_1_id,
  Location_ID forecast_2_id,
  Vendor_Location_ID Source_Location_1
 from sap_purchase_orders_final
)
 select
   forecast_1_id,
   forecast_2_id,
   Source_Location_1,
   nvl(pdhProduct.ansStdUomConv,0) ansStdUomConv
 from 
   q1
   join pdhProduct on Forecast_1_ID = pdhProduct.e2eProductCode
""")
SP_LOAD_DATA.createOrReplaceTempView('SP_LOAD_DATA')
# SP_LOAD_DATA.display()

# COMMAND ----------

LGTY_ITEM_LOCATION =spark.sql("""
select distinct 
  Forecast_1_id Item_ID,
  Forecast_2_id Location_ID,
  ansStdUomConv SP_Transfer_Out_Minimum_Quantity,
  ansStdUomConv SP_Transfer_Out_Mulitiple,
  1 SP_Transfer_Out_Max_Quantity,
  'S' Schedule_Type
from
  lom_aa
  join locationmaster lm on forecast_2_id = lm.GlobalLocationCode
  join pdhProduct on forecast_1_id = pdhProduct.e2eProductCode
  left join dbo_cm_resource_data cm on
    pdhProduct.productStyle = cm.CapacityGroup
    and pdhProduct.sizeDescription = cm.size
    and forecast_2_id = cm.Location
  
""")
LGTY_ITEM_LOCATION.createOrReplaceTempView('LGTY_ITEM_LOCATION')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('TMP_LGTY_ITEM_LOCATION', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
LGTY_ITEM_LOCATION.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

default_origin_pdh=spark.sql("""
with q1 as
(select distinct
  pos.itemId,
  min(globallocationcode) globallocationcode
from
  s_core.product_origin_agg pos
  join locationmasterNonPrimary on pos.originId = locationmasterNonPrimary.origin
where 1=1
  and pos.primaryOrigin
  and not pos._deleted
  and pos.`_SOURCE` = 'PDH'
  and not locationmasterNonPrimary._deleted
group by 
  pos.itemId)

select 
  q1.itemId, 
  globallocationcode,
  nvl(cm.plannerID,'DEFAULT') plannerID
FROM
  q1
  join pdhProduct on q1.itemId = pdhProduct.productCode
   left join current_primary_capacity_master cm on 
     pdhProduct.productStyle = cm.Style
     and pdhProduct.sizeDescription = cm.size
     and q1.globallocationcode = cm.Location
""")
default_origin_pdh.createOrReplaceTempView('default_origin_pdh')

# COMMAND ----------

default_origins=spark.sql("""
select distinct
  forecast_1_id, 
  source_location_1
from cdr_ai_final
where 1=1
and (source_location_1 like 'EM%' or source_location_1 like 'PL%')
""")
default_origins.createOrReplaceTempView('default_origins')  

# COMMAND ----------

origin_count=spark.sql("""
select 
  forecast_1_id,
  count(*) counter
from default_origins
GROUP BY
  forecast_1_id
 """)
origin_count.createOrReplaceTempView('origin_count')

# COMMAND ----------

single_origins=spark.sql("""
select 
  default_origins.forecast_1_id, 
  source_location_1
from 
  default_origins
where exists(select 1 from origin_count where default_origins.forecast_1_id = origin_count.forecast_1_id and origin_count.counter = 1 )
""")
single_origins.createOrReplaceTempView('single_origins')

# COMMAND ----------

multiple_origin=spark.sql("""
select distinct 
  default_origins.forecast_1_id, 
  default_origin_pdh.globallocationcode
from 
  default_origins
  join default_origin_pdh on default_origins.forecast_1_id = default_origin_pdh.itemId
where not exists(select 1 from origin_count where default_origins.forecast_1_id = origin_count.forecast_1_id and origin_count.counter = 1 )
 """)
multiple_origin.createOrReplaceTempView('multiple_origin')

# COMMAND ----------

default_location_per_item=spark.sql("""
with q1 AS (
select 
  forecast_1_id, 
  source_location_1
from single_origins
UNION
select 
  forecast_1_id,
  globallocationcode source_location_1
from multiple_origin  
)
  select forecast_1_id,
  source_location_1,
  nvl(cm.plannerID, 'DEFAULT') plannerID
from 
  q1
  join pdhProduct on q1.forecast_1_id = pdhProduct.productCode
  left join current_primary_capacity_master cm on 
    pdhProduct.productStyle = cm.Style
    and pdhProduct.sizeDescription = cm.size
    and q1.source_location_1 = cm.Location
""")
default_location_per_item.createOrReplaceTempView('default_location_per_item')

# COMMAND ----------

default_planner=spark.sql("""
select distinct
  lom_aa.forecast_1_id,
  min(cm.PlannerID) PLNR_USR_INDX,
  min(nvl(case when cm.PlanningTimeFence - 30 < 30 then 30 else cm.PlanningTimeFence - 30 end,0)) PLAN_TIME_FNC_DAYS
from
  lom_aa
  join pdhProduct on lom_aa.forecast_1_id = pdhProduct.e2eProductCode
  left join default_location_per_item on lom_aa.forecast_1_id = default_location_per_item.forecast_1_id
  left join current_capacity_master cm on
    pdhProduct.productStyle = cm.Style
    and pdhProduct.sizeDescription = cm.size
    and forecast_2_id = default_location_per_item.source_location_1
 where 1=1
 and cm.PlannerID is not null
 and cm.ResourceType = 'Primary'
group by
  lom_aa.forecast_1_id
""")
default_planner.createOrReplaceTempView('default_planner')

# COMMAND ----------

SPS_ITEM_LOCATION=spark.sql("""
select distinct 
  lom_aa.forecast_1_id ITEM_INDX, 
  forecast_2_id LOC_INDX, 
  coalesce(cm.PlannerID, default_origin_pdh.plannerID, 'DEFAULT') PLNR_USR_INDX,
  lom_aa.forecast_2_id || '-S' STRG_RSRC_INDX,
  lom_aa.forecast_2_id || '-F' DFLT_FLOW_RSRC_INDX,
  '1' SS_PNLTY_VALUE,
  '1' OBJTV_BKORD_PNLTY_FCTR,
  '1' SBJTV_BKORD_PNLTY_FCTR,
  case 
    when left(lom_aa.forecast_2_id,2) in ('DS', 'DC')
      then 0
  else   
      coalesce(spt_ptf.ParameterValue, default_planner.PLAN_TIME_FNC_DAYS, cm.PlanningTimeFence, 0)
  end PLAN_TIME_FNC_DAYS,
  '0' BKORD_ALWD_CODE,
  '0' EXCMSG_TIME_FNC_DAYS,
  case 
    when forecast_2_id like 'DS%' then '0'
    when forecast_2_id like 'DC%' then '1'
    else '0'
  end ORD_SPLIT_IND,
  0 USR_STD_COST_VALUE,
  cast(182 as integer) REQD_SHLF_LIFE_DAYS
from
  lom_aa
  join pdhProduct on lom_aa.forecast_1_id = pdhProduct.e2eProductCode
   left join default_planner on lom_aa.forecast_1_id = default_planner.forecast_1_id
  left join current_primary_capacity_master cm on 
    pdhProduct.productStyle = cm.Style
    and pdhProduct.sizeDescription = cm.size
    and forecast_2_id = cm.Location
  left join itemsourceexceptions_Planning_Time_Fence spt_ptf on lom_aa.forecast_1_id = spt_ptf.item_number and lom_aa.forecast_2_id = spt_ptf.destination
  left join default_origin_pdh on lom_aa.forecast_1_id = default_origin_pdh.itemId
  
 where 1=1
   and left(lom_aa.forecast_2_id,2) not in ('DS', 'DC')
   
union

select distinct 
  lom_aa.forecast_1_id ITEM_INDX, 
  forecast_2_id LOC_INDX, 
  coalesce(default_location_per_item.plannerID, default_origin_pdh.plannerID, 'DEFAULT') PLNR_USR_INDX,
  lom_aa.forecast_2_id || '-S' STRG_RSRC_INDX,
  lom_aa.forecast_2_id || '-F' DFLT_FLOW_RSRC_INDX,
  '1' SS_PNLTY_VALUE,
  '1' OBJTV_BKORD_PNLTY_FCTR,
  '1' SBJTV_BKORD_PNLTY_FCTR,
  0 PLAN_TIME_FNC_DAYS,
  '0' BKORD_ALWD_CODE,
  '0' EXCMSG_TIME_FNC_DAYS,
  case 
    when forecast_2_id like 'DS%' then '0'
    when forecast_2_id like 'DC%' then '1'
    else '0'
  end ORD_SPLIT_IND,
  0 USR_STD_COST_VALUE,
  cast(182 as integer) REQD_SHLF_LIFE_DAYS
from
  lom_aa
  join pdhProduct on lom_aa.forecast_1_id = pdhProduct.e2eProductCode
  left join default_location_per_item on lom_aa.forecast_1_id = default_location_per_item.forecast_1_id
  left join itemsourceexceptions_Planning_Time_Fence spt_ptf on lom_aa.forecast_1_id = spt_ptf.item_number and lom_aa.forecast_2_id = spt_ptf.destination
  -- left join current_primary_capacity_master cm on 
  --   pdhProduct.productStyle = cm.Style
  --   and pdhProduct.sizeDescription = cm.size
  --   and forecast_2_id = default_location_per_item.source_location_1
  left join default_origin_pdh on lom_aa.forecast_1_id = default_origin_pdh.itemId
 where 1=1
   and left(lom_aa.forecast_2_id,2)  in ('DS', 'DC')

""")
SPS_ITEM_LOCATION.createOrReplaceTempView('SPS_ITEM_LOCATION')
SPS_ITEM_LOCATION.display()

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_item_location', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
SPS_ITEM_LOCATION.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,SPS_VEHICLE_TYPE (new)
sps_vehicle_type = spark.sql("""
select distinct
  'VHCL_' || Source_Location_1 || '_TO_' || forecast_2_id VHCL_TYP_INDX,
  'VHCL_' || Source_Location_1 || '_TO_' || forecast_2_id VHCL_TYP_DESC,
  left(Source_Location_1 || '_' || forecast_2_id, 10) VHCL_TYP_ABBR_TEXT,
  '1' VHCL_CNSTRNT_IND,
  '1' CNSTRNT_ACTV_IND
from 
  SP_LOAD_DATA
where 
  Source_Location_1 <> forecast_2_id
  
union
select distinct
   'VHCL_' || source || '_TO_' || destination VHCL_TYP_INDX,
   'VHCL_' || source || '_TO_' || destination VHCL_TYP_DESC,
   left(source || '_' || destination, 10) VHCL_TYP_ABBR_TEXT,
  '1' VHCL_CNSTRNT_IND,
  '1' CNSTRNT_ACTV_IND
from
  itemsourceexceptions_multisource

  
""")
sps_vehicle_type.createOrReplaceTempView('sps_vehicle_type')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_SPS_VEHICLE_TYPE', 'dlt') 
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_vehicle_type.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,SPS_LANE (new)
sps_lane=spark.sql("""
select distinct
  Source_location_1 SOURCE_LOCATION,
  Source_location_1 || '-F' SOURCE_FLOW_AREA,
  forecast_2_id DESTINATION_LOCATION,
  forecast_2_id || '-F' DESTINATION_FLOW_AREA,
  'VHCL_' || Source_Location_1 || '_TO_' || forecast_2_id VEHICLE_TYPE
from 
  SP_LOAD_DATA
where
  Source_location_1 <> forecast_2_id

union 

select 
  source SOURCE_LOCATION,
  source || '-F' SOURCE_FLOW_AREA,
  destination DESTINATION_LOCATION,
  destination || '-F' DESTINATION_FLOW_AREA,
  'VHCL_' || source || '_TO_' || destination VEHICLE_TYPE
from
  itemsourceexceptions_multisource

""")
sps_lane.createOrReplaceTempView('sps_lane')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_lane', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_lane.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,SPS_LANE_TRANSPORT (new)
sps_lane_transport=spark.sql("""
select distinct
  Source_location_1 SOURCE_LANE,
  forecast_2_id DEST_LANE,
  'VHCL_' || Source_Location_1 || '_TO_' || forecast_2_id VHCL_TYP_INDX,
  'OCEAN' TRNSPRT_SVC_INDX,
  'PIECE' UOM_INDEX,
  to_date('2020-01-01') BEGIN_EFFECTIVE_DATE,
  to_date('2040-01-01') END_EFFECTIVE_DATE,
  'DEFAULT' CALENDAR_ID,
  '0.1' UOM_COST_VALUE,
  case 
    when source.GTCPODPortOfDeparture is null then 1 
    when destination.GTCPOAPortOfArrival is null then 1 
    when intra_org_transit.TransitTime is null and FND_FLEX_VALUES_VL.transitTime is null then 1 
    else nvl(intra_org_transit.TransitTime,0) + nvl(FND_FLEX_VALUES_VL.transitTime,0)
  end LANE_LEAD_TIME_DAYS
from 
  SP_LOAD_DATA
  join locationmaster source on Source_location_1 = source.GlobalLocationCode
  join locationmaster destination on forecast_2_id = destination.GlobalLocationCode
  left join intra_org_transit on source.GTCPODPortOfDeparture = intra_org_transit.FROM_ORG
    and destination.GTCPOAPortOfArrival = intra_org_transit.TO_ORG
  left join FND_FLEX_VALUES_VL on source.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
    and destination.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival
  where
    Source_location_1 <> forecast_2_id
    
union

select distinct
  itemexceptions.Source SOURCE_LANE,
  itemexceptions.destination DEST_LANE,
  'VHCL_' || Source || '_TO_' || destination VHCL_TYP_INDX,
  'OCEAN' TRNSPRT_SVC_INDX,
  'PIECE' UOM_INDEX,
  to_date('2020-01-01') BEGIN_EFFECTIVE_DATE,
  to_date('2040-01-01') END_EFFECTIVE_DATE,
  'DEFAULT' CALENDAR_ID,
  '0.1' UOM_COST_VALUE,
  case 
    when sourceloc.GTCPODPortOfDeparture is null then 1 
    when destinationloc.GTCPOAPortOfArrival is null then 1 
    when intra_org_transit.TransitTime is null and FND_FLEX_VALUES_VL.transitTime is null then 1 
    else nvl(intra_org_transit.TransitTime,0) + nvl(FND_FLEX_VALUES_VL.transitTime,0)
  end LANE_LEAD_TIME_DAYS
  
  from itemsourceexceptions_multisource itemexceptions
  join locationmaster sourceloc on itemexceptions.Source = sourceloc.GlobalLocationCode
  join locationmaster destinationloc on itemexceptions.destination = destinationloc.GlobalLocationCode
  left join intra_org_transit on sourceloc.GTCPODPortOfDeparture = intra_org_transit.FROM_ORG
    and destinationloc.GTCPOAPortOfArrival = intra_org_transit.TO_ORG
  left join FND_FLEX_VALUES_VL on sourceloc.GTCPODPortOfDeparture = FND_FLEX_VALUES_VL.portOfDeparture
    and destinationloc.GTCPOAPortOfArrival = FND_FLEX_VALUES_VL.portOfArrival
    
    
""")
sps_lane_transport.createOrReplaceTempView('sps_lane_transport')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_lane_transport', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_lane_transport.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

# DBTITLE 1,SPS_ITEM_VEHICLE (new)
sps_item_vehicle = spark.sql("""
select distinct
  forecast_1_id ITEM_INDX,
  'VHCL_' || Source_Location_1 || '_TO_' || forecast_2_id VHCL_TYP_INDX,
  '0' CPCTY_CNSTRNT_IND,
  '0' VHCL_CPCTY_QTY
from 
  SP_LOAD_DATA
where 
  Source_Location_1 <> forecast_2_id
  
union 

select distinct
   item_number ITEM_INDEX,
'VHCL_' || source || '_TO_' ||  destination VHCL_TYP_INDX,
  '0' CPCTY_CNSTRNT_IND,
  '0' VHCL_CPCTY_QTY
from itemsourceexceptions_multisource
 
  
""")

sps_item_vehicle.createOrReplaceTempView('sps_item_vehicle')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name('tmp_sps_item_vehicle', 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# LOAD
sps_item_vehicle.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------


