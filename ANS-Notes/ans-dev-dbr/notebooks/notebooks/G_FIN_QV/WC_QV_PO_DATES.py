# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_po_dates

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'PO_NUM,LINE_NUM')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_po_dates')
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
source_table = 'ebs.po_lines_all'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  po_lines_all = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  po_lines_all = load_full_dataset(source_table)
  
po_lines_all.createOrReplaceTempView('po_lines_all')
po_lines_all.display()

# COMMAND ----------

GTC_SUPPLIER_MULTI_CETD=spark.sql("""
select 
  ffvv.flex_value vendor_id
from 
  EBS.FND_FLEX_VALUES ffvv
  join EBS.fnd_flex_value_sets ffvs on ffvv.flex_value_set_id = ffvs.flex_value_set_id
where  
  ffvs.flex_value_set_name = 'XXANS_GTC_SUPPLIER_MULTI_CETD'
""")
GTC_SUPPLIER_MULTI_CETD.createOrReplaceTempView('GTC_SUPPLIER_MULTI_CETD')

# COMMAND ----------

main_wc_qv_po_dates = spark.sql("""
SELECT
  POH.SEGMENT1 AS PO_NUM,
  PLL.LINE_NUM,
  INT(POH.IN_TRANSIT_TIME) AS TRANSIT_TIME,
  PLA.NEED_BY_DATE,
  DATE_ADD(pla.need_by_date, - INT(POH.IN_TRANSIT_TIME)) RETD,
  PLA.PROMISED_DATE,
  (
    CASE 
      -- Existing order follow buffer calculation
      WHEN (date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20220520' AND '20221231' 
        OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20220520' AND  '20221231' )
        and nvl(check_val_emea_po.flag,'N') = 'N'
        and NVL(check_pto_dropship_so.flag,'N') = 'N' 
      THEN DATE_ADD(PLA.PROMISED_DATE, - INT(PLL.IN_TRANSIT_TIME) -21) -- (SELECT apps.fnd_profile.value@OBI_ANSPRD('XX_ANSELL_PO_CETD_UPDATE_DAYS') FROM DUAL)
      -- Previously buffer for 14 days created before 19-May-2022
      WHEN (date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20200101' AND '20220519'
        OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20200101'  AND '20220519')
        and NVL(check_val_emea_po.flag,'N') = 'N'
        and NVL(check_pto_dropship_so.flag,'N') = 'N' THEN DATE_ADD(PLA.PROMISED_DATE, - INT(PLL.IN_TRANSIT_TIME) -14)
      --SAP order no buffer for single CETD
      WHEN (NVL(check_val_emea_po.flag,'N') = 'Y')
        and GTC_SUPPLIER_MULTI_CETD.vendor_id is null THEN DATE_ADD(PLA.PROMISED_DATE, - INT(PLL.IN_TRANSIT_TIME))
      -- SAP order for MCETD 
      WHEN NVL(check_val_emea_po.flag,'N') = 'Y'
        and GTC_SUPPLIER_MULTI_CETD.vendor_id is not null  THEN DATE_ADD(rsh.EXPECTED_RECEIPT_DATE, - INT(PLL.IN_TRANSIT_TIME)
      )
      WHEN (
        date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20220520'
        AND date_format(current_date, 'yyyyMMdd')
        OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20220520'
        AND date_format(current_date, 'yyyyMMdd')
      )
      and (NVL(check_pto_dropship_so.flag,'N') = 'Y') THEN DATE_ADD(PLA.PROMISED_DATE, - INT(PLL.IN_TRANSIT_TIME) -21) -- (SELECT apps.fnd_profile.value@OBI_ANSPRD('XX_ANSELL_PTO_DS_CETD_UPDATE_DAYS') FROM DUAL)
      WHEN (
        date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20200101'
        AND '20220519'
        OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20200101'
        AND '20220519'
      )
      and (NVL(check_pto_dropship_so.flag,'N') = 'Y') THEN DATE_ADD(PLA.PROMISED_DATE, - INT(PLL.IN_TRANSIT_TIME))
      ELSE DATE_ADD(PLA.PROMISED_DATE, - INT(PLL.IN_TRANSIT_TIME))
    END
  ) CETD,
  pla.shipment_num SHIPMENT_NUM,
  SUPPLIER.SEGMENT1 AS VENDOR_NUM,
  INT(SUPPLIER.VENDOR_ID) AS VENDOR_ID
FROM
  ebs.PO_HEADERS_ALL POH
  join ebs.PO_LINES_ALL PLL on POH.PO_HEADER_ID = PLL.PO_HEADER_ID
  join ebs.po_line_locations_all pla on PLL.PO_LINE_ID = PLA.PO_LINE_ID
  left join GTC_SUPPLIER_MULTI_CETD on POH.vendor_id = GTC_SUPPLIER_MULTI_CETD.vendor_id
  left join (
    --check_pto_dropship_so
    SELECT
      distinct pha.segment1,
      'Y' flag
    FROM
      ebs.oe_order_headers_all ooh,
      ebs.oe_transaction_types_tl ott,
      ebs.oe_drop_ship_sources ods,
      ebs.po_headers_all pha
    WHERE
      1 = 1
      AND pha.org_id = 938 --AND pha.segment1 = '15191806'
      AND ott.transaction_type_id = ooh.order_type_id
      AND ooh.header_id = ods.header_id
      AND ods.po_header_id = pha.po_header_id
      AND UPPER (ott.name) IN (
        SELECT
          UPPER (meaning)
        FROM
          ebs.fnd_lookup_values
        WHERE
          enabled_flag = 'Y'
          AND current_date BETWEEN NVL (
            start_date_active,
            current_date
          )
          AND NVL (
            end_date_active,
            current_date
          )
          AND TRIM (UPPER (lookup_type)) = 'XXASL_ROA_DROPSHIP_ORDER_TYPES'
      )
  ) check_pto_dropship_so on poh.segment1 = check_pto_dropship_so.segment1
  left join (
    --- check_val_emea_po
    SELECT
      distinct pha.segment1,
      'Y' flag
    FROM
      ebs.oe_drop_ship_sources ods,
      ebs.oe_order_headers_all ooh,
      ebs.oe_order_lines_all ool,
      ebs.hz_cust_accounts hca,
      ebs.hz_parties hp,
      ebs.fnd_lookup_values flv,
      ebs.po_headers_all pha
    WHERE
      pha.org_id = 938
      AND ods.po_header_id = pha.po_header_id
      AND ooh.header_id = ods.header_id
      AND ooh.header_id = ool.header_id
      AND ooh.sold_to_org_id = hca.cust_account_id
      AND hp.party_id = hca.party_id
      AND hca.account_number = flv.lookup_code
      AND flv.lookup_type = 'XXANS_EMEA_CUSTOMER'
      AND flv.language = 'US'
      AND flv.enabled_flag = 'Y' --AND pha.segment1 = p_po_num
  ) check_val_emea_po on poh.segment1 = check_val_emea_po.segment1
  left join (
    select
      poh.segment1,
      max(rsh.EXPECTED_RECEIPT_DATE) EXPECTED_RECEIPT_DATE
    from
      ebs.rcv_shipment_headers rsh,
      ebs.rcv_shipment_lines rsl,
      ebs.po_headers_all poh,
      ebs.po_lines_all pla
    where
      rsh.shipment_header_id = rsl.shipment_header_id
      and rsl.po_header_id = poh.po_header_id
      and rsl.PO_LINE_ID = pla.PO_LINE_ID
      and shipment_line_status_code = 'EXPECTED'
    group by
      poh.segment1
  ) rsh on poh.segment1 = rsh.segment1
  left join ebs.ap_suppliers supplier on poh.vendor_id = supplier.vendor_id
where
  NVL(POH.CLOSED_CODE, 'OPEN') <> 'CLOSED'
  AND NVL(PLA.CLOSED_CODE, 'OPEN') <> 'CLOSED'
  AND NVL(pll.CLOSED_CODE, 'OPEN') <> 'CLOSED'
  AND NVL (poh.cancel_flag, 'N') <> 'Y'
  AND NVL (pla.cancel_flag, 'N') <> 'Y'
  AND NVL (pll.cancel_flag, 'N') <> 'Y'
  AND poh.type_lookup_code = 'STANDARD'
  and date_format(poh.creation_date, 'yyyyMMdd') >= '20210701'
  and PLL.item_id is not null
  AND poh.org_id IN (select distinct key_value from smartsheets.edm_control_table where table_id = 'WC_QV_PO_DATES')
 -- and POH.SEGMENT1 = '15191806'
  
  """)
main_wc_qv_po_dates.createOrReplaceTempView("main_wc_qv_po_dates")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_wc_qv_po_dates
   .transform(attach_partition_column("NEED_BY_DATE"))
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
