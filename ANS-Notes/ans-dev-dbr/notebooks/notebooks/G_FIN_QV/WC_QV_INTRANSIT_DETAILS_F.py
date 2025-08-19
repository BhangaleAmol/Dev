# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_intransit_details_f

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'ITEM_NUMBER,ORG_CHANNEL,EXPORT_PERIOD')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_intransit_details_f')
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
source_table = 'ebs.xx_msc_orders_v'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  xx_msc_orders_v = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  xx_msc_orders_v = load_full_dataset(source_table)
  
xx_msc_orders_v.createOrReplaceTempView('xx_msc_orders_v')
xx_msc_orders_v.display()

# COMMAND ----------

TRANSIT_TIME=spark.sql("""
SELECT van.vendor_name ,
fv.attribute4,
supp.vendor_site_code,
int(NVL(max(fv.attribute5),'0')) Transit_Time
						   FROM ebs.ap_suppliers van,
						        ebs.ap_supplier_sites_all supp,
						        ebs.fnd_flex_values fv,
						        ebs.fnd_flex_value_sets fvs
						   WHERE van.vendor_id = supp.vendor_id
						    AND UPPER(TRIM(supp.attribute10)) = UPPER(fv.attribute2)
						    AND UPPER(supp.attribute_category) = 'GTC'
						    AND UPPER(TRIM(fv.attribute1)) = UPPER('Oracle Region')
						    AND fv.enabled_flag = 'Y'
						    AND current_date BETWEEN NVL(fv.start_date_active,current_date) AND NVL(fv.end_date_active,current_date)
						    AND UPPER(fv.value_category) = UPPER('XXASL_TRANSIT_TIME_ORA_REGN')
						    AND fv.flex_value_set_id = fvs.flex_value_set_id
						    AND UPPER(fvs.flex_value_set_name) = UPPER('XXASL_TRANSIT_TIME')
							and fv.attribute3 = 'FRT'
							--and van.vendor_name = SUPPLIER_NAME	and fv.attribute4= substr(MSC_ORDERS_V.ORGANIZATION_CODE, 5, 3)
							--and supp.vendor_site_code = MSC_ORDERS_V.SUPPLIER_SITE_CODE
							--and rownum=1
                            group by 
                            van.vendor_name ,
fv.attribute4,
supp.vendor_site_code
  """)
TRANSIT_TIME .createOrReplaceTempView('TRANSIT_TIME')

# COMMAND ----------

INTRANSIT_TIME=spark.sql("""
--INTRANSIT_TIME
SELECT
  TT.ORGANIZATION_CODE,
  FR.ORGANIZATION_CODE SOURCE_ORGANIZATION_CODE,
  int(NVL(MAX(sm.INTRANSIT_TIME), '0')) INTRANSIT_TIME
FROM
  ebs.msc_interorg_ship_methods sm,
  ebs.mtl_parameters FR,
  ebs.HR_LOCATIONS_all flc,
  ebs.mtl_parameters TT,
  ebs.HR_LOCATIONS_all tlc
WHERE
  sm.FROM_ORGANIZATION_ID = FR.ORGANIZATION_id
  and sm.TO_ORGANIZATION_ID = TT.ORGANIZATION_id
  and flc.inventory_ORGANIZATION_id = FR.ORGANIZATION_id
  and tlc.inventory_ORGANIZATION_id = TT.ORGANIZATION_id
  and sm.FROM_LOCATION_ID = flc.LOCATION_ID
  and sm.TO_LOCATION_ID = tlc.LOCATION_ID
  and FR.ORGANIZATION_CODE not in ('503', '504', '511', '512', '532', '801')
  and sm.DEFAULT_FLAG = 1 
  -- and FR.ORGANIZATION_CODE = substr(SOURCE_ORGANIZATION_CODE,5,3) and  TT.ORGANIZATION_CODE = substr(MSC_ORDERS_V.ORGANIZATION_CODE, 5, 3)
  -- and rownum=1
group by
  TT.ORGANIZATION_CODE,
  FR.ORGANIZATION_CODE
  """)
INTRANSIT_TIME .createOrReplaceTempView('INTRANSIT_TIME')

# COMMAND ----------

conv_from_item=spark.sql("""
--conv_from_item
select
  b.segment1,
  uom_code,
  sum(nvl(conversion_rate, 0)) conversion_rate
from
  ebs.MTL_UOM_CONVERSIONS a,
  ebs.mtl_system_items_b b
where
  a.inventory_item_id = b.inventory_item_id
  and b.organization_id = 124
group by
  uom_code,
  b.segment1
  """)
conv_from_item .createOrReplaceTempView('conv_from_item')

# COMMAND ----------

conv_from=spark.sql("""
--conv_from
select
  uom_code,
  sum(conversion_rate) conversion_rate
from
  ebs.MTL_UOM_CONVERSIONS
where
  inventory_item_id = 0
group by
  uom_code
  """)
conv_from .createOrReplaceTempView('conv_from')

# COMMAND ----------

conv_to_item=spark.sql("""
--conv_to_item
select
  b.segment1,
  uom_code,
  sum(conversion_rate) conversion_rate
from
  ebs.MTL_UOM_CONVERSIONS a,
  ebs.mtl_system_items_b b
where
  a.inventory_item_id = b.inventory_item_id
  and b.organization_id = 124
group by
  uom_code,
  b.segment1
  """)
conv_to_item .createOrReplaceTempView('conv_to_item')

# COMMAND ----------

conv_to=spark.sql("""
--conv_to
select
  uom_code,
  sum(conversion_rate) conversion_rate
from
  ebs.MTL_UOM_CONVERSIONS
where
  inventory_item_id = 0
group by
  uom_code
  """)
conv_to .createOrReplaceTempView('conv_to')

# COMMAND ----------

main_intransit_details_extract = spark.sql("""
select
  current_date() as EXPORT_PERIOD,
  MSC_ORDERS_V.ITEM_SEGMENTS ITEM_NUMBER,
  MSC_ORDERS_V.DESCRIPTION ITEM_DESCRIPTION,
  substr(MSC_ORDERS_V.ORGANIZATION_CODE, 5, 3) as ORG_CHANNEL,
  case
    when MSC_ORDERS_V.ORDER_TYPE_TEXT = 'Purchase order'
    and MSC_ORDERS_V.PROMISE_DATE is not null then MSC_ORDERS_V.PROMISE_DATE
    else MSC_ORDERS_V.NEW_DUE_DATE
  end as DUE_DATE,
  case
    when SUPPLIER_NAME is not null then (
      date_add(
        case
          when MSC_ORDERS_V.ORDER_TYPE_TEXT = 'Purchase order'
          and MSC_ORDERS_V.PROMISE_DATE is not null then MSC_ORDERS_V.PROMISE_DATE
          else MSC_ORDERS_V.NEW_DUE_DATE
        end,
        - (TRANSIT_TIME.TRANSIT_TIME)
      )
    )
    when SUPPLIER_SITE_CODE is null
    and ORDER_TYPE_TEXT <> 'Work order' then (
      date_add(
        case
          when MSC_ORDERS_V.ORDER_TYPE_TEXT = 'Purchase order'
          and MSC_ORDERS_V.PROMISE_DATE is not null then MSC_ORDERS_V.PROMISE_DATE
          else MSC_ORDERS_V.NEW_DUE_DATE
        end,
        - (INTRANSIT_TIME.INTRANSIT_TIME)
      )
    )
    when SUPPLIER_SITE_CODE is null
    and ORDER_TYPE_TEXT = 'Work order' then (
      date_add(
        case
          when MSC_ORDERS_V.ORDER_TYPE_TEXT = 'Purchase order'
          and MSC_ORDERS_V.PROMISE_DATE is not null then MSC_ORDERS_V.PROMISE_DATE
          else MSC_ORDERS_V.NEW_DUE_DATE
        end,
        -7
      )
    )
    else null
  end as REQ_SHIP_DATE,
  case
    when SUPPLIER_NAME is not null then SUPPLIER_NAME
    when SUPPLIER_NAME is null then org.NAME
  end as SUPPLIER_NAME,
  case
    when SUPPLIER_SITE_CODE is not null then SUPPLIER_SITE_CODE
    when SUPPLIER_SITE_CODE is null
    and ORDER_TYPE_TEXT <> 'Work order' then MSC_ORDERS_V.SOURCE_ORGANIZATION_CODE
    when SUPPLIER_SITE_CODE is null
    and ORDER_TYPE_TEXT = 'Work order' then MSC_ORDERS_V.ORGANIZATION_CODE
  end as SUPPLIER_SITE_CODE,
  MSC_ORDERS_V.ORDER_NUMBER,
  Case
    when ORDER_TYPE_TEXT = 'Purchase order' then 'ON ORDER'
    when ORDER_TYPE_TEXT = 'Purchase requisition' then 'ON ORDER'
    when ORDER_TYPE_TEXT = 'Work order' then 'ON ORDER'
    when ORDER_TYPE_TEXT = 'Intransit shipment' then 'IN TRANSIT'
    when ORDER_TYPE_TEXT = 'PO in receiving' then 'IN TRANSIT'
    else ORDER_TYPE_TEXT
  end as ORDER_TYPE,
  sum(
    case
      when ORDER_TYPE_TEXT in ('Intransit shipment', 'PO in receiving')
      and MSC_ORDERS_V.UOM_CODE <> MSI.ATTRIBUTE15 then round(
        QUANTITY * nvl(
          conv_from_item.conversion_rate,
          conv_from.conversion_rate
        ) / nvl(
          conv_to_item.conversion_rate,
          conv_to.conversion_rate
        ),
        3
      )
      when ORDER_TYPE_TEXT in ('Intransit shipment', 'PO in receiving')
      and MSC_ORDERS_V.UOM_CODE = MSI.ATTRIBUTE15 then QUANTITY
    end
  ) as QTY_STD_UOM,
  MSI.ATTRIBUTE15 ANSELL_STD_UOM,
  POH.CREATION_DATE PO_CREATION_DATE
from
  XX_MSC_ORDERS_V MSC_ORDERS_V
  JOIN EBS.MTL_SYSTEM_ITEMS_b msi ON MSC_ORDERS_V.ITEM_SEGMENTS = msi.segment1
  JOIN EBS.mtl_item_categories a ON msi.inventory_item_id = a.inventory_item_id
  JOIN EBS.mtl_category_sets_tl b ON a.category_set_id = b.category_set_id
  JOIN EBS.mtl_categories_b c ON a.category_id = c.category_id
  LEFT JOIN conv_from_item ON MSC_ORDERS_V.ITEM_SEGMENTS = conv_from_item.segment1
  AND MSC_ORDERS_V.UOM_CODE = conv_from_item.uom_code
  LEFT JOIN conv_from ON MSC_ORDERS_V.UOM_CODE = conv_from.uom_code
  LEFT JOIN conv_to ON msi.attribute15 = conv_to.uom_code
  LEFT JOIN conv_to_item ON msi.segment1 = conv_to_item.segment1
  and msi.attribute15 = CONV_TO_ITEM.uom_code
  LEFT JOIN EBS.HR_ALL_ORGANIZATION_UNITS org ON MSC_ORDERS_V.SOURCE_ORGANIZATION_ID = org.ORGANIZATION_ID
  LEFT JOIN TRANSIT_TIME ON TRANSIT_TIME.vendor_name = MSC_ORDERS_V.SUPPLIER_NAME
  and TRANSIT_TIME.attribute4 = substr(MSC_ORDERS_V.ORGANIZATION_CODE, 5, 3)
  and TRANSIT_TIME.vendor_site_code = MSC_ORDERS_V.SUPPLIER_SITE_CODE
  LEFT JOIN INTRANSIT_TIME ON INTRANSIT_TIME.SOURCE_ORGANIZATION_CODE = substr(MSC_ORDERS_V.SOURCE_ORGANIZATION_CODE, 5, 3)
  and INTRANSIT_TIME.ORGANIZATION_CODE = substr(
    MSC_ORDERS_V.ORGANIZATION_CODE,
    5,
    3
  )
  LEFT JOIN EBS.PO_HEADERS_ALL POH ON case
    when instr(MSC_ORDERS_V.ORDER_NUMBER, '_') > 0 then substr(
      MSC_ORDERS_V.ORDER_NUMBER,
      1,
      instr(MSC_ORDERS_V.ORDER_NUMBER, '_') -1
    )
    when instr(MSC_ORDERS_V.ORDER_NUMBER, '(') > 0 then substr(
      MSC_ORDERS_V.ORDER_NUMBER,
      1,
      instr(MSC_ORDERS_V.ORDER_NUMBER, '(') -1
    )
    when instr(MSC_ORDERS_V.ORDER_NUMBER, '.') > 0 then substr(
      MSC_ORDERS_V.ORDER_NUMBER,
      1,
      instr(MSC_ORDERS_V.ORDER_NUMBER, '.') -1
    )
    ELSE MSC_ORDERS_V.ORDER_NUMBER
  end = POH.SEGMENT1
where
  b.language = 'US'
  --and msi.segment1 = '845696'
  and msi.organization_id = 124
  and MSC_ORDERS_V.ORDER_TYPE_TEXT in (
    'Intransit shipment',
    'PO in receiving',
    'Purchase requisition',
    'Work order',
    'Purchase order'
  )
  and MSC_ORDERS_V.CATEGORY_SET_ID = '1002' --and MSC_ORDERS_V.CATEGORY_SET_ID            = '1'
  --and  CATEGORY_NAME like '%11435%'
  and MSC_ORDERS_V.ORGANIZATION_CODE in (
    'PRD:400',
    'PRD:601',
    'PRD:514',
    'PRD:518',
    'PRD:813',
    'PRD:817',
    'PRD:819',
    'PRD:600',
    'PRD:802',
    'PRD:803',
    'PRD:804',
    'PRD:807',
    'PRD:810',
    'PRD:403',
    'PRD:821',
    'PRD:822',
    'PRD:814',
    'PRD:724',
    'PRD:823',
    'PRD:326',
    'PRD:327',
    'PRD:826',
    'PRD:827',
    'PRD:809'
  )
  and a.organization_id = 124
  and b.category_set_name = 'Brand'
  and DISPOSITION_STATUS_TYPE is null
  and (
    case
      when ORDER_TYPE_TEXT in (
        'Purchase requisition',
        'Work order',
        'Purchase order',
        'Intransit shipment',
        'PO in receiving'
      ) then QUANTITY
    end
  ) > 0.0001
group by
  TRANSIT_TIME.TRANSIT_TIME,
  INTRANSIT_TIME.INTRANSIT_TIME,
  MSC_ORDERS_V.ITEM_SEGMENTS,
  MSC_ORDERS_V.DESCRIPTION,
  substr(MSC_ORDERS_V.ORGANIZATION_CODE, 5, 3),
  msi.PRIMARY_UNIT_OF_MEASURE,
  MSC_ORDERS_V.vendor_id,
  SUPPLIER_NAME,
  MSC_ORDERS_V.ORGANIZATION_CODE,
  SUPPLIER_SITE_CODE,
  case
    when SUPPLIER_SITE_CODE is not null then SUPPLIER_SITE_CODE
    when SUPPLIER_SITE_CODE is null
    and ORDER_TYPE_TEXT <> 'Work order' then MSC_ORDERS_V.SOURCE_ORGANIZATION_CODE
    when SUPPLIER_SITE_CODE is null
    and ORDER_TYPE_TEXT = 'Work order' then MSC_ORDERS_V.ORGANIZATION_CODE
  end,
  case
    when SUPPLIER_NAME is not null then SUPPLIER_NAME
    when SUPPLIER_NAME is null then org.NAME
  end,
  MSC_ORDERS_V.order_number,
  case
    when MSC_ORDERS_V.UOM_CODE = 'CA' then 'CASE'
    else MSC_ORDERS_V.UOM_CODE
  end,
  MSI.ATTRIBUTE15,
  MSC_ORDERS_V.new_order_date,
  MSC_ORDERS_V.ORDER_NUMBER,
  MSC_ORDERS_V.SOURCE_ORGANIZATION_CODE,
  ORDER_TYPE_TEXT,
  ACTION,
  case
    when MSC_ORDERS_V.ORDER_TYPE_TEXT = 'Purchase order'
    and MSC_ORDERS_V.PROMISE_DATE is not null then MSC_ORDERS_V.PROMISE_DATE
    else MSC_ORDERS_V.NEW_DUE_DATE
  end,
  POH.CREATION_DATE
order by
  MSC_ORDERS_V.order_number,
  MSC_ORDERS_V.ITEM_SEGMENTS
  
  """)
main_intransit_details_extract.createOrReplaceTempView("main_intransit_details_extract")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_intransit_details_extract
   .transform(attach_partition_column("EXPORT_PERIOD"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# VALIDATE DATA
#if incremental:
 # check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
